use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use super::PubSub;

pub async fn blocks(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting PostgreSQL LISTEN for block notifications");
    
    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create PostgreSQL listener for blocks: {}", e);
            warn!("Falling back to polling mode for blocks");
            let interval = interval(Duration::from_secs(1));
            super::triggers::poll_blocks(pubsub, pool, interval).await;
            return;
        }
    };

    if let Err(e) = listener.listen("explorer_block_update").await {
        error!("Failed to LISTEN to explorer_block_update channel: {}", e);
        warn!("Falling back to polling mode for blocks");
        let interval = interval(Duration::from_secs(1));
        super::triggers::poll_blocks(pubsub, pool, interval).await;
        return;
    }

    info!("Successfully listening to PostgreSQL notifications for blocks");

    loop {
        match listener.recv().await {
            Ok(notification) => {
                match notification.payload().parse::<i64>() {
                    Ok(height) => {
                        debug!("Block notification received: height {}", height);
                        pubsub.publish_block(height);
                    }
                    Err(e) => {
                        warn!("Failed to parse block notification payload '{}': {}", notification.payload(), e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving block notification: {}", e);
                
                warn!("Attempting to reconnect block listener...");
                if let Err(e) = listener.listen("explorer_block_update").await {
                    error!("Failed to re-establish LISTEN connection: {}", e);
                    break;
                }
                info!("Successfully reconnected block listener");
            }
        }
    }

    warn!("Block notification listener exited, falling back to polling");
    let interval = interval(Duration::from_secs(1));
    super::triggers::poll_blocks(pubsub, pool, interval).await;
}

pub async fn transactions(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting PostgreSQL LISTEN for transaction notifications");
    
    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create PostgreSQL listener for transactions: {}", e);
            warn!("Falling back to polling mode for transactions");
            let interval = interval(Duration::from_secs(1));
            super::triggers::poll_transactions(pubsub, pool, interval).await;
            return;
        }
    };

    if let Err(e) = listener.listen("explorer_tx_update").await {
        error!("Failed to LISTEN to explorer_tx_update channel: {}", e);
        warn!("Falling back to polling mode for transactions");
        let interval = interval(Duration::from_secs(1));
        super::triggers::poll_transactions(pubsub, pool, interval).await;
        return;
    }

    if let Err(e) = listener.listen("explorer_tx_count_update").await {
        warn!("Failed to LISTEN to explorer_tx_count_update channel: {}", e);
    }

    info!("Successfully listening to PostgreSQL notifications for transactions");

    loop {
        match listener.recv().await {
            Ok(notification) => {
                match notification.channel() {
                    "explorer_tx_update" => {
                        match notification.payload().parse::<i64>() {
                            Ok(height) => {
                                debug!("Transaction notification received: block height {}", height);
                                pubsub.publish_transaction(height);
                            }
                            Err(e) => {
                                warn!("Failed to parse transaction notification payload '{}': {}", 
                                      notification.payload(), e);
                            }
                        }
                    }
                    "explorer_tx_count_update" => {
                        match notification.payload().parse::<i64>() {
                            Ok(count) => {
                                debug!("Transaction count notification received: {}", count);
                                pubsub.publish_transaction_count(count);
                            }
                            Err(e) => {
                                warn!("Failed to parse transaction count notification payload '{}': {}", 
                                      notification.payload(), e);
                            }
                        }
                    }
                    channel => {
                        warn!("Received notification on unexpected channel: {}", channel);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving transaction notification: {}", e);
                
                warn!("Attempting to reconnect transaction listener...");
                let mut reconnect_success = true;
                
                if let Err(e) = listener.listen("explorer_tx_update").await {
                    error!("Failed to re-establish LISTEN on explorer_tx_update: {}", e);
                    reconnect_success = false;
                }
                
                if let Err(e) = listener.listen("explorer_tx_count_update").await {
                    warn!("Failed to re-establish LISTEN on explorer_tx_count_update: {}", e);
                }
                
                if !reconnect_success {
                    error!("Failed to reconnect transaction listener");
                    break;
                }
                
                info!("Successfully reconnected transaction listener");
            }
        }
    }

    warn!("Transaction notification listener exited, falling back to polling");
    let interval = interval(Duration::from_secs(1));
    super::triggers::poll_transactions(pubsub, pool, interval).await;
}

pub async fn transaction_count(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting PostgreSQL LISTEN for transaction count notifications");
    
    let mut listener = match PgListener::connect_with(&pool).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to create PostgreSQL listener for transaction count: {}", e);
            warn!("Falling back to polling mode for transaction count");
            let interval = interval(Duration::from_secs(1));
            super::triggers::poll_transaction_count(pubsub, pool, interval).await;
            return;
        }
    };

    if let Err(e) = listener.listen("explorer_tx_count_update").await {
        error!("Failed to LISTEN to explorer_tx_count_update channel: {}", e);
        warn!("Falling back to polling mode for transaction count");
        let interval = interval(Duration::from_secs(1));
        super::triggers::poll_transaction_count(pubsub, pool, interval).await;
        return;
    }

    info!("Successfully listening to PostgreSQL notifications for transaction count");

    loop {
        match listener.recv().await {
            Ok(notification) => {
                match notification.payload().parse::<i64>() {
                    Ok(count) => {
                        debug!("Transaction count notification received: {}", count);
                        pubsub.publish_transaction_count(count);
                    }
                    Err(e) => {
                        warn!("Failed to parse transaction count notification payload '{}': {}", 
                              notification.payload(), e);
                    }
                }
            }
            Err(e) => {
                error!("Error receiving transaction count notification: {}", e);
                
                warn!("Attempting to reconnect transaction count listener...");
                if let Err(e) = listener.listen("explorer_tx_count_update").await {
                    error!("Failed to re-establish LISTEN connection: {}", e);
                    break;
                }
                info!("Successfully reconnected transaction count listener");
            }
        }
    }

    warn!("Transaction count notification listener exited, falling back to polling");
    let interval = interval(Duration::from_secs(1));
    super::triggers::poll_transaction_count(pubsub, pool, interval).await;
}