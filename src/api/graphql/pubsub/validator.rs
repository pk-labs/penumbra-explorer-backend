use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug)]
pub struct ValidatorBlockEvent {
    pub validator_id: String,
    pub block_height: i64,
    pub signed: bool,
}

#[derive(Deserialize, Serialize, Debug)]
struct ValidatorBlockNotification {
    validator_id: String,
    block_height: i64,
    signed: bool,
}

pub async fn listen_validator_blocks(
    pubsub: super::PubSub,
    pool: Pool<Postgres>,
    validator_id: String,
) {
    info!("Starting validator block listener for {}", validator_id);
    
    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create PostgreSQL listener: {}", e);
            let interval = interval(Duration::from_secs(1));
            poll_validator_blocks(pubsub, pool, validator_id, interval).await;
            return;
        }
    };
    
    if let Err(e) = listener.listen("explorer_validator_block_update").await {
        error!("Failed to listen to validator block updates: {}", e);
        let interval = interval(Duration::from_secs(1));
        poll_validator_blocks(pubsub, pool, validator_id, interval).await;
        return;
    }
    
    info!("Successfully connected to PostgreSQL notifications for validator blocks");
    
    let pubsub_clone = pubsub.clone();
    let pool_clone = pool.clone();
    let validator_id_clone = validator_id.clone();
    tokio::spawn(async move {
        let interval = interval(Duration::from_secs(10));
        poll_validator_blocks(pubsub_clone, pool_clone, validator_id_clone, interval).await;
    });
    
    loop {
        match listener.recv().await {
            Ok(notification) => {
                match serde_json::from_str::<ValidatorBlockNotification>(&notification.payload()) {
                    Ok(data) => {
                        if data.validator_id == validator_id {
                            debug!(
                                "Notification: New validator block for {} at height {} (signed: {})",
                                data.validator_id, data.block_height, data.signed
                            );
                            
                            let event = ValidatorBlockEvent {
                                validator_id: data.validator_id,
                                block_height: data.block_height,
                                signed: data.signed,
                            };
                            
                            let pubsub_clone = pubsub.clone();
                            tokio::spawn(async move {
                                pubsub_clone.publish_validator_block(event).await;
                            });
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse validator block notification: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving notification: {}", e);
                if let Err(e) = listener.listen("explorer_validator_block_update").await {
                    error!("Failed to re-listen after error: {}", e);
                    break;
                }
            }
        }
    }
    
    warn!("Notification listener exited, falling back to polling");
    let interval = interval(Duration::from_secs(1));
    poll_validator_blocks(pubsub, pool, validator_id, interval).await;
}

async fn poll_validator_blocks(
    pubsub: super::PubSub,
    pool: Pool<Postgres>,
    validator_id: String,
    mut interval: tokio::time::Interval,
) {
    let mut last_block_height: Option<i64> = None;

    loop {
        interval.tick().await;

        match get_latest_validator_block(&pool, &validator_id).await {
            Ok(Some((block_height, signed))) => {
                if last_block_height.is_none() || last_block_height.unwrap() < block_height {
                    debug!(
                        "Polling: New validator block detected for {} at height {} (signed: {})",
                        validator_id, block_height, signed
                    );
                    let event = ValidatorBlockEvent {
                        validator_id: validator_id.clone(),
                        block_height,
                        signed,
                    };
                    let pubsub_clone = pubsub.clone();
                    tokio::spawn(async move {
                        pubsub_clone.publish_validator_block(event).await;
                    });
                    last_block_height = Some(block_height);
                }
            }
            Ok(None) => {}
            Err(e) => error!("Error fetching latest validator block: {}", e),
        }
    }
}

async fn get_latest_validator_block(
    pool: &Pool<Postgres>,
    validator_id: &str,
) -> Result<Option<(i64, bool)>, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64, bool)>(
        r"
        SELECT 
            vb.block_height,
            vb.signed
        FROM 
            validator_blocks vb
        JOIN validators v ON v.identity_key = vb.identity_key
        WHERE 
            v.decoded_address = $1
        ORDER BY 
            vb.block_height DESC
        LIMIT 1
        ",
    )
    .bind(validator_id)
    .fetch_optional(pool)
    .await?;

    Ok(result)
}