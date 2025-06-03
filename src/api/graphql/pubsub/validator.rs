use sqlx::{Pool, Postgres};
use tokio::time::Interval;
use tracing::{debug, error};

#[derive(Clone, Debug)]
pub struct ValidatorBlockEvent {
    pub validator_id: String,
    pub block_height: i64,
    pub signed: bool,
}

pub async fn poll_validator_blocks(
    pubsub: super::PubSub,
    pool: Pool<Postgres>,
    validator_id: String,
    mut interval: Interval,
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