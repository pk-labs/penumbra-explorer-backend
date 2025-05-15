use async_graphql::Context;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub mod ibc;
mod triggers;
use ibc::IbcTransactionEvent;
pub use triggers::start;

#[derive(Clone)]
pub struct PubSub {
    blocks_tx: broadcast::Sender<i64>,
    transactions_tx: broadcast::Sender<i64>,
    transaction_count_tx: broadcast::Sender<i64>,
    ibc_transactions_tx: broadcast::Sender<IbcTransactionEvent>,

    total_shielded_volume_tx: broadcast::Sender<String>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    #[must_use]
    pub fn new() -> Self {
        let (blocks_tx, _) = broadcast::channel(1000);
        let (transactions_tx, _) = broadcast::channel(1000);
        let (transaction_count_tx, _) = broadcast::channel(1000);
        let (ibc_transactions_tx, _) = broadcast::channel(1000);

        let (total_shielded_volume_tx, _) = broadcast::channel(1000);
        Self {
            blocks_tx,
            transactions_tx,
            transaction_count_tx,
            ibc_transactions_tx,
            total_shielded_volume_tx,
        }
    }

    #[must_use]
    pub fn blocks_subscribe(&self) -> broadcast::Receiver<i64> {
        self.blocks_tx.subscribe()
    }

    #[must_use]
    pub fn transactions_subscribe(&self) -> broadcast::Receiver<i64> {
        self.transactions_tx.subscribe()
    }

    #[must_use]
    pub fn transaction_count_subscribe(&self) -> broadcast::Receiver<i64> {
        self.transaction_count_tx.subscribe()
    }

    #[must_use]
    pub fn ibc_transactions_subscribe(&self) -> broadcast::Receiver<IbcTransactionEvent> {
        self.ibc_transactions_tx.subscribe()
    }

    #[must_use]
    pub fn total_shielded_volume_subscribe(&self) -> broadcast::Receiver<String> {
        self.total_shielded_volume_tx.subscribe()
    }

    pub fn publish_block(&self, height: i64) {
        match self.blocks_tx.send(height) {
            Ok(_) => debug!("Published block update: {}", height),
            Err(e) => {
                let receiver_count = self.blocks_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for block update");
                } else {
                    warn!("Failed to publish block update: {}", e);
                }
            }
        }
    }

    pub fn publish_transaction(&self, id: i64) {
        match self.transactions_tx.send(id) {
            Ok(_) => debug!("Published transaction update: {}", id),
            Err(e) => {
                let receiver_count = self.transactions_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for transaction update");
                } else {
                    warn!("Failed to publish transaction update: {}", e);
                }
            }
        }
    }

    pub fn publish_transaction_count(&self, count: i64) {
        match self.transaction_count_tx.send(count) {
            Ok(_) => debug!("Published transaction count update: {}", count),
            Err(e) => {
                let receiver_count = self.transaction_count_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for transaction count update");
                } else {
                    warn!("Failed to publish transaction count update: {}", e);
                }
            }
        }
    }

    pub fn publish_ibc_transaction(&self, event: IbcTransactionEvent) {
        match self.ibc_transactions_tx.send(event) {
            Ok(_) => debug!("Published IBC transaction update"),
            Err(e) => {
                let receiver_count = self.ibc_transactions_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for IBC transaction update");
                } else {
                    warn!("Failed to publish IBC transaction update: {}", e);
                }
            }
        }
    }

    pub fn publish_total_shielded_volume(&self, value: String) {
        let value_clone = value.clone();
        match self.total_shielded_volume_tx.send(value) {
            Ok(_) => debug!("Published total shielded volume update: {}", value_clone),
            Err(e) => {
                let receiver_count = self.total_shielded_volume_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for total shielded volume update");
                } else {
                    warn!("Failed to publish total shielded volume update: {}", e);
                }
            }
        }
    }

    #[must_use]
    pub fn from_context<'a>(ctx: &'a Context<'_>) -> Option<&'a Self> {
        ctx.data_opt::<Self>()
    }

    pub fn start_subscriptions(&self, pool: &Pool<Postgres>) {
        info!("Starting subscription triggers");

        let pubsub_clone = self.clone();
        let pool_clone = pool.clone();

        tokio::spawn(async move {
            start(pubsub_clone, pool_clone).await;
        });
    }
}
