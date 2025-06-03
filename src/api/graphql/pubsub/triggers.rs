use sqlx::{Pool, Postgres};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

use super::ibc;
use super::PubSub;

pub async fn start(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting real-time subscription triggers");

    if let Err(e) = setup_notification_triggers(&pool).await {
        error!("Failed to set up database notification triggers: {}", e);
        info!("Falling back to polling mechanism only");
    } else {
        info!("Database notification triggers set up successfully");
    }

    fallback_polling(pubsub, pool).await;
}

#[allow(clippy::too_many_lines)]
async fn setup_notification_triggers(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION notify_block_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('explorer_block_update', NEW.height::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(r"
        CREATE OR REPLACE FUNCTION notify_transaction_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('explorer_tx_update', NEW.block_height::text);
            PERFORM pg_notify('explorer_tx_count_update', (SELECT COUNT(*)::text FROM explorer_transactions));
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ").execute(pool).await?;

    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION notify_ibc_transaction_update()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Only notify if this is an IBC transaction
            IF NEW.ibc_client_id IS NOT NULL THEN
                PERFORM pg_notify('explorer_ibc_tx_update', encode(NEW.tx_hash, 'hex'));
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION notify_total_shielded_volume_update()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Notify when an IBC transaction is completed or status changes
            IF (TG_OP = 'INSERT' AND NEW.ibc_status IN ('completed', 'pending')) OR
               (TG_OP = 'UPDATE' AND NEW.ibc_status != OLD.ibc_status) THEN
                PERFORM pg_notify('explorer_total_shielded_volume_update', 'updated');
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE OR REPLACE FUNCTION notify_validator_block_update()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('explorer_validator_block_update', 
                json_build_object(
                    'validator_id', (SELECT decoded_address FROM validators WHERE identity_key = NEW.identity_key),
                    'block_height', NEW.block_height,
                    'signed', NEW.signed
                )::text
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    ",
    )
    .execute(pool)
    .await?;

    let _ = sqlx::query("DROP TRIGGER IF EXISTS block_update_trigger ON explorer_block_details")
        .execute(pool)
        .await;
    let _ =
        sqlx::query("DROP TRIGGER IF EXISTS transaction_update_trigger ON explorer_transactions")
            .execute(pool)
            .await;
    let _ = sqlx::query(
        "DROP TRIGGER IF EXISTS ibc_transaction_update_trigger ON explorer_transactions",
    )
    .execute(pool)
    .await;
    let _ = sqlx::query(
        "DROP TRIGGER IF EXISTS total_shielded_volume_update_trigger ON explorer_transactions",
    )
    .execute(pool)
    .await;
    let _ =
        sqlx::query("DROP TRIGGER IF EXISTS validator_block_update_trigger ON validator_blocks")
            .execute(pool)
            .await;

    sqlx::query(
        r"
        CREATE TRIGGER block_update_trigger
        AFTER INSERT OR UPDATE ON explorer_block_details
        FOR EACH ROW EXECUTE FUNCTION notify_block_update();
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE TRIGGER transaction_update_trigger
        AFTER INSERT OR UPDATE ON explorer_transactions
        FOR EACH ROW EXECUTE FUNCTION notify_transaction_update();
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE TRIGGER ibc_transaction_update_trigger
        AFTER INSERT OR UPDATE OF ibc_status ON explorer_transactions
        FOR EACH ROW EXECUTE FUNCTION notify_ibc_transaction_update();
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE TRIGGER total_shielded_volume_update_trigger
        AFTER INSERT OR UPDATE OF ibc_status ON explorer_transactions
        FOR EACH ROW EXECUTE FUNCTION notify_total_shielded_volume_update();
    ",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r"
        CREATE TRIGGER validator_block_update_trigger
        AFTER INSERT OR UPDATE ON validator_blocks
        FOR EACH ROW EXECUTE FUNCTION notify_validator_block_update();
    ",
    )
    .execute(pool)
    .await?;

    info!("Successfully set up database notification triggers");
    Ok(())
}

async fn fallback_polling(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting polling mechanism");

    let blocks_interval = interval(Duration::from_secs(1));
    let txs_interval = interval(Duration::from_secs(1));
    let count_interval = interval(Duration::from_secs(1));
    let ibc_txs_interval = interval(Duration::from_secs(2));

    let total_shielded_volume_interval = interval(Duration::from_secs(5));

    tokio::join!(
        poll_blocks(pubsub.clone(), pool.clone(), blocks_interval),
        poll_transactions(pubsub.clone(), pool.clone(), txs_interval),
        poll_transaction_count(pubsub.clone(), pool.clone(), count_interval),
        ibc::poll_ibc_transactions(pubsub.clone(), pool.clone(), ibc_txs_interval),
        poll_total_shielded_volume(pubsub, pool, total_shielded_volume_interval)
    );
}

async fn poll_blocks(pubsub: PubSub, pool: Pool<Postgres>, mut interval: tokio::time::Interval) {
    let mut last_height: Option<i64> = None;

    loop {
        interval.tick().await;

        match get_latest_block_height(&pool).await {
            Ok(Some(height)) => {
                if last_height.is_none() || last_height.unwrap() < height {
                    debug!("Polling: New block detected at height {}", height);
                    pubsub.publish_block(height);
                    last_height = Some(height);
                }
            }
            Ok(None) => {}
            Err(e) => error!("Error fetching latest block: {}", e),
        }
    }
}

async fn poll_transactions(
    pubsub: PubSub,
    pool: Pool<Postgres>,
    mut interval: tokio::time::Interval,
) {
    let mut last_tx_height: Option<i64> = None;

    loop {
        interval.tick().await;

        match get_latest_transaction_height(&pool).await {
            Ok(Some(height)) => {
                if last_tx_height.is_none() || last_tx_height.unwrap() < height {
                    debug!(
                        "Polling: New transaction detected at block height {}",
                        height
                    );
                    pubsub.publish_transaction(height);
                    last_tx_height = Some(height);
                }
            }
            Ok(None) => {}
            Err(e) => error!("Error fetching latest transaction: {}", e),
        }
    }
}

async fn poll_transaction_count(
    pubsub: PubSub,
    pool: Pool<Postgres>,
    mut interval: tokio::time::Interval,
) {
    let mut last_count: Option<i64> = None;

    loop {
        interval.tick().await;

        match get_transaction_count(&pool).await {
            Ok(count) => {
                if last_count.is_none() || last_count.unwrap() != count {
                    debug!("Polling: Transaction count changed to {}", count);
                    pubsub.publish_transaction_count(count);
                    last_count = Some(count);
                }
            }
            Err(e) => error!("Error fetching transaction count: {}", e),
        }
    }
}

async fn poll_total_shielded_volume(
    pubsub: PubSub,
    pool: Pool<Postgres>,
    mut interval: tokio::time::Interval,
) {
    let mut last_value: Option<String> = None;

    loop {
        interval.tick().await;

        match get_total_shielded_volume(&pool).await {
            Ok(value) => {
                if last_value.is_none() || last_value.as_ref().unwrap() != &value {
                    debug!("Polling: Total shielded volume changed to {}", value);
                    pubsub.publish_total_shielded_volume(value.clone());
                    last_value = Some(value);
                }
            }
            Err(e) => error!("Error fetching total shielded volume: {}", e),
        }
    }
}

async fn get_latest_block_height(pool: &Pool<Postgres>) -> Result<Option<i64>, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64,)>(
        "SELECT height FROM explorer_block_details ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(result.map(|r| r.0))
}

async fn get_latest_transaction_height(pool: &Pool<Postgres>) -> Result<Option<i64>, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64,)>(
        "SELECT block_height FROM explorer_transactions ORDER BY timestamp DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(result.map(|r| r.0))
}

async fn get_transaction_count(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM explorer_transactions")
        .fetch_one(pool)
        .await?;

    Ok(result.0)
}

async fn get_total_shielded_volume(pool: &Pool<Postgres>) -> Result<String, sqlx::Error> {
    let result = sqlx::query_scalar::<_, String>(
        r"
        SELECT
            COALESCE(SUM(shielded_volume), '0')::TEXT
        FROM
            ibc_client_summary
        ",
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}
