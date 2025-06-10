use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
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
            error!(
                "Failed to create PostgreSQL listener for validator {}: {}",
                validator_id, e
            );
            return;
        }
    };

    if let Err(e) = listener.listen("explorer_validator_block_update").await {
        error!(
            "Failed to listen to validator block updates for {}: {}",
            validator_id, e
        );
        return;
    }

    info!(
        "Successfully connected to PostgreSQL notifications for validator blocks: {}",
        validator_id
    );

    loop {
        match listener.recv().await {
            Ok(notification) => {
                match serde_json::from_str::<ValidatorBlockNotification>(notification.payload()) {
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
                error!(
                    "Error receiving notification for validator {}: {}",
                    validator_id, e
                );
                if let Err(e) = listener.listen("explorer_validator_block_update").await {
                    error!(
                        "Failed to re-listen after error for validator {}: {}",
                        validator_id, e
                    );
                    break;
                }
            }
        }
    }

    warn!(
        "Notification listener exited for validator {}",
        validator_id
    );
}

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct ChainParametersEvent {
    pub chain_id: String,
    pub current_block_height: i64,
    pub current_block_time: DateTime<Utc>,
    pub current_epoch: i64,
    pub epoch_duration: i64,
    pub next_epoch_in: i64,
    pub last_updated: DateTime<Utc>,
}

#[derive(Deserialize, Serialize, Debug)]
struct ChainParametersNotification {
    chain_id: String,
    current_block_height: i64,
    current_block_time: DateTime<Utc>,
    current_epoch: i64,
    epoch_duration: i64,
    next_epoch_in: i64,
    last_updated: DateTime<Utc>,
}

pub async fn listen_chain_parameters(pubsub: super::PubSub, pool: Pool<Postgres>) {
    info!("Starting chain parameters listener");

    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create PostgreSQL listener: {}", e);
            let interval = interval(Duration::from_secs(5));
            poll_chain_parameters(pubsub, pool, interval).await;
            return;
        }
    };

    if let Err(e) = listener.listen("explorer_chain_parameters_update").await {
        error!("Failed to listen to chain parameters updates: {}", e);
        let interval = interval(Duration::from_secs(5));
        poll_chain_parameters(pubsub, pool, interval).await;
        return;
    }

    info!("Successfully connected to PostgreSQL notifications for chain parameters");

    loop {
        match listener.recv().await {
            Ok(notification) => {
                match serde_json::from_str::<ChainParametersNotification>(notification.payload()) {
                    Ok(data) => {
                        debug!(
                            "Notification: Chain parameters updated - block height: {}, epoch: {}",
                            data.current_block_height, data.current_epoch
                        );

                        let event = ChainParametersEvent {
                            chain_id: data.chain_id,
                            current_block_height: data.current_block_height,
                            current_block_time: data.current_block_time,
                            current_epoch: data.current_epoch,
                            epoch_duration: data.epoch_duration,
                            next_epoch_in: data.next_epoch_in,
                            last_updated: data.last_updated,
                        };

                        let pubsub_clone = pubsub.clone();
                        tokio::spawn(async move {
                            pubsub_clone.publish_chain_parameters(&event);
                        });
                    }
                    Err(e) => {
                        warn!("Failed to parse chain parameters notification: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving notification: {}", e);
                if let Err(e) = listener.listen("explorer_chain_parameters_update").await {
                    error!("Failed to re-listen after error: {}", e);
                    break;
                }
            }
        }
    }

    warn!("Notification listener exited, falling back to polling");
    let interval = interval(Duration::from_secs(5));
    poll_chain_parameters(pubsub, pool, interval).await;
}

async fn poll_chain_parameters(
    pubsub: super::PubSub,
    pool: Pool<Postgres>,
    mut interval: tokio::time::Interval,
) {
    let mut last_update: Option<DateTime<Utc>> = None;

    loop {
        interval.tick().await;

        match get_chain_parameters(&pool).await {
            Ok(Some(params)) => {
                if last_update.is_none() || last_update.unwrap() < params.last_updated {
                    debug!(
                        "Polling: Chain parameters updated - block height: {}, epoch: {}",
                        params.current_block_height, params.current_epoch
                    );
                    let last_updated = params.last_updated;
                    let pubsub_clone = pubsub.clone();
                    tokio::spawn(async move {
                        pubsub_clone.publish_chain_parameters(&params);
                    });
                    last_update = Some(last_updated);
                }
            }
            Ok(None) => {}
            Err(e) => error!("Error fetching chain parameters: {}", e),
        }
    }
}

async fn get_chain_parameters(
    pool: &Pool<Postgres>,
) -> Result<Option<ChainParametersEvent>, sqlx::Error> {
    let result = sqlx::query_as::<_, (String, i64, DateTime<Utc>, i64, i64, i64, DateTime<Utc>)>(
        r"
        SELECT 
            chain_id,
            current_block_height,
            current_block_time,
            current_epoch,
            epoch_duration,
            next_epoch_in,
            last_updated
        FROM 
            validator_chain_parameters
        LIMIT 1
        ",
    )
    .fetch_optional(pool)
    .await?;

    Ok(result.map(
        |(
            chain_id,
            current_block_height,
            current_block_time,
            current_epoch,
            epoch_duration,
            next_epoch_in,
            last_updated,
        )| {
            ChainParametersEvent {
                chain_id,
                current_block_height,
                current_block_time,
                current_epoch,
                epoch_duration,
                next_epoch_in,
                last_updated,
            }
        },
    ))
}
