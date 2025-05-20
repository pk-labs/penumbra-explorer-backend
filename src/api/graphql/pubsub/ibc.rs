use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use tokio::time::Interval;
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct IbcTransactionEvent {
    pub tx_hash: Vec<u8>,
    pub client_id: String,
    pub status: String,
    pub is_status_update: bool,
}

struct IbcTransactionTracker {
    seen_transactions: HashMap<Vec<u8>, String>,
    max_entries: usize,
}

impl IbcTransactionTracker {
    fn new(max_entries: usize) -> Self {
        Self {
            seen_transactions: HashMap::with_capacity(max_entries),
            max_entries,
        }
    }

    // Returns true if this is a status update for an existing transaction
    fn check_and_update(&mut self, tx_hash: &Vec<u8>, status: &str) -> bool {
        // Check if we need to prune the cache
        if self.seen_transactions.len() >= self.max_entries {
            // Simple pruning strategy: clear half the cache when full
            let keys_to_remove: Vec<Vec<u8>> = self
                .seen_transactions
                .keys()
                .take(self.max_entries / 2)
                .cloned()
                .collect();

            for key in keys_to_remove {
                self.seen_transactions.remove(&key);
            }
            debug!(
                "Pruned IBC transaction tracker cache (removed {} entries)",
                self.max_entries / 2
            );
        }

        // Check if this is a status update or a new transaction
        let is_status_update = match self.seen_transactions.get(tx_hash) {
            Some(old_status) => old_status != status,
            None => false,
        };

        self.seen_transactions
            .insert(tx_hash.clone(), status.to_string());

        is_status_update
    }
}

pub async fn poll_ibc_transactions(
    pubsub: super::PubSub,
    pool: Pool<Postgres>,
    mut interval: Interval,
) {
    info!("Starting IBC transaction polling");

    let mut tracker = IbcTransactionTracker::new(10_000);

    loop {
        interval.tick().await;

        match get_recent_ibc_transactions(&pool).await {
            Ok(transactions) => {
                for (tx_hash, client_id, status, _, _) in transactions {
                    let is_status_update = tracker.check_and_update(&tx_hash, &status);

                    if is_status_update {
                        debug!(
                            "Detected status change for IBC transaction: new status = {}",
                            status
                        );
                    }

                    let event = IbcTransactionEvent {
                        tx_hash,
                        client_id,
                        status,
                        is_status_update,
                    };

                    pubsub.publish_ibc_transaction(event);
                }
            }
            Err(e) => error!("Error fetching IBC transactions: {}", e),
        }
    }
}

async fn get_recent_ibc_transactions(
    pool: &Pool<Postgres>,
) -> Result<Vec<(Vec<u8>, String, String, i64, chrono::DateTime<chrono::Utc>)>, sqlx::Error> {
    sqlx::query_as::<_, (Vec<u8>, String, String, i64, chrono::DateTime<chrono::Utc>)>(
        r"
        SELECT
            tx_hash,
            ibc_client_id,
            ibc_status,
            block_height,
            timestamp
        FROM
            explorer_transactions
        WHERE
            ibc_client_id IS NOT NULL
        ORDER BY
            timestamp DESC
        LIMIT 100
        ",
    )
    .fetch_all(pool)
    .await
}

/// Returns IBC transactions for a specific client ID with pagination
///
/// # Errors
/// Returns a `sqlx::Error` if the database query fails
pub async fn get_ibc_transactions_by_client(
    pool: &Pool<Postgres>,
    client_id: &str,
    limit: i32,
    offset: i32,
) -> Result<
    Vec<(
        Vec<u8>,
        String,
        String,
        i64,
        chrono::DateTime<chrono::Utc>,
        String,
    )>,
    sqlx::Error,
> {
    sqlx::query_as::<
        _,
        (
            Vec<u8>,
            String,
            String,
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
        ),
    >(
        r"
        SELECT
            tx_hash,
            ibc_client_id,
            ibc_status,
            block_height,
            timestamp,
            raw_data
        FROM
            explorer_transactions
        WHERE
            ibc_client_id = $1
        ORDER BY
            timestamp DESC
        LIMIT $2 OFFSET $3
        ",
    )
    .bind(client_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

/// Returns all IBC transactions with pagination
///
/// # Errors
/// Returns a `sqlx::Error` if the database query fails
pub async fn get_all_ibc_transactions(
    pool: &Pool<Postgres>,
    limit: i32,
    offset: i32,
) -> Result<
    Vec<(
        Vec<u8>,
        String,
        String,
        i64,
        chrono::DateTime<chrono::Utc>,
        String,
    )>,
    sqlx::Error,
> {
    sqlx::query_as::<
        _,
        (
            Vec<u8>,
            String,
            String,
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
        ),
    >(
        r"
        SELECT
            tx_hash,
            ibc_client_id,
            ibc_status,
            block_height,
            timestamp,
            raw_data
        FROM
            explorer_transactions
        WHERE
            ibc_client_id IS NOT NULL
        ORDER BY
            timestamp DESC
        LIMIT $1 OFFSET $2
        ",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

/// Returns details for a specific transaction by hash
///
/// # Errors
/// Returns a `sqlx::Error` if the database query fails
pub async fn get_transaction_details(
    pool: &Pool<Postgres>,
    tx_hash: &[u8],
) -> Result<(i64, chrono::DateTime<chrono::Utc>, String, String), sqlx::Error> {
    let row = sqlx::query_as::<_, (i64, chrono::DateTime<chrono::Utc>, String, String)>(
        r"
        SELECT
            block_height,
            timestamp,
            ibc_status,
            raw_data
        FROM
            explorer_transactions
        WHERE
            tx_hash = $1
        ",
    )
    .bind(tx_hash)
    .fetch_one(pool)
    .await?;

    Ok((row.0, row.1, row.2, row.3))
}
