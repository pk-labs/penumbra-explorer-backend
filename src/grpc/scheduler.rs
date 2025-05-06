use super::client::{
    query_all_client_channels, query_all_clients, GrpcClient, CLIENT_STATUS_ACTIVE,
    CLIENT_STATUS_EXPIRED, CLIENT_STATUS_FROZEN, CLIENT_STATUS_UNKNOWN,
};
use sqlx;
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

async fn check_client_statuses(
    client: &GrpcClient,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    match query_all_clients(client).await {
        Ok(statuses) => {
            info!("Retrieved {} IBC client statuses", statuses.len());
            let active_count = statuses
                .iter()
                .filter(|(_, status)| status == CLIENT_STATUS_ACTIVE)
                .count();
            let expired_count = statuses
                .iter()
                .filter(|(_, status)| status == CLIENT_STATUS_EXPIRED)
                .count();
            let frozen_count = statuses
                .iter()
                .filter(|(_, status)| status == CLIENT_STATUS_FROZEN)
                .count();
            let unknown_count = statuses
                .iter()
                .filter(|(_, status)| status == CLIENT_STATUS_UNKNOWN)
                .count();

            info!(
                "IBC clients: Total: {}, Active: {}, Expired: {}, Frozen: {}, Unknown: {}",
                statuses.len(),
                active_count,
                expired_count,
                frozen_count,
                unknown_count
            );

            info!("=== IBC Client Status Summary ===");
            for (client_id, status) in &statuses {
                info!("Client: {}, Status: {}", client_id, status);
            }
            info!("================================");

            Ok(statuses)
        }
        Err(e) => {
            error!("Failed to query IBC clients: {}", e);
            Err(anyhow::anyhow!("Failed to query IBC clients: {}", e))
        }
    }
}

async fn check_client_channels(
    client: &GrpcClient,
) -> Result<Vec<(String, Option<String>, Option<String>)>, anyhow::Error> {
    match query_all_client_channels(client).await {
        Ok(client_channels) => {
            info!("=== IBC Client Open Channels Summary ===");

            let mut results = Vec::with_capacity(client_channels.len());

            for (client_id, channels) in &client_channels {
                if channels.is_empty() {
                    info!("Client: {} - No open channels found", client_id);
                    // Still include the client in results with None for channels
                    results.push((client_id.clone(), None, None));
                } else {
                    info!(
                        "Client: {} - Found {} open channels:",
                        client_id,
                        channels.len()
                    );

                    // For now, we'll just use the first channel for each client
                    // We can later enhance this to handle multiple channels if needed
                    if let Some((_, channel_id, counterparty_channel_id)) = channels.first() {
                        info!(
                            "    Using primary channel: {}, Counterparty Channel: {}",
                            channel_id, counterparty_channel_id
                        );
                        results.push((
                            client_id.clone(),
                            Some(channel_id.clone()),
                            Some(counterparty_channel_id.clone()),
                        ));
                    } else {
                        results.push((client_id.clone(), None, None));
                    }

                    // Log all channels
                    for (port_id, channel_id, counterparty_channel_id) in channels {
                        info!(
                            "    Port: {}, Channel: {}, Counterparty Channel: {}",
                            port_id, channel_id, counterparty_channel_id
                        );
                    }
                }
            }
            info!("=====================================");

            Ok(results)
        }
        Err(e) => {
            error!("Failed to query IBC client channels: {}", e);
            Err(anyhow::anyhow!(
                "Failed to query IBC client channels: {}",
                e
            ))
        }
    }
}

async fn check_ibc_clients(pool: &sqlx::PgPool) {
    info!("Running scheduled IBC client status check");
    let client = GrpcClient::new("grpc.penumbra.silentvalidator.com", 443);

    // Check client statuses
    let _client_statuses = match check_client_statuses(&client).await {
        Ok(statuses) => {
            // Update client statuses in the database
            for (client_id, status) in &statuses {
                // We only update if the client ID already exists in the database
                let result = sqlx::query(
                    r"
                    UPDATE ibc_clients 
                    SET status = $1 
                    WHERE client_id = $2
                    ",
                )
                .bind(status)
                .bind(client_id)
                .execute(pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to update client status in database: {}", e);
                }
            }

            statuses
        }
        Err(e) => {
            error!("Failed to check client statuses: {}", e);
            Vec::new()
        }
    };

    // Check client channels
    match check_client_channels(&client).await {
        Ok(client_channels) => {
            // Update channel information in the database
            for (client_id, channel_id, counterparty_channel_id) in client_channels {
                // We only update if the client ID already exists in the database
                let result = sqlx::query(
                    r"
                    UPDATE ibc_clients 
                    SET 
                        channel_id = $1,
                        counterparty_channel_id = $2
                    WHERE client_id = $3
                    ",
                )
                .bind(channel_id)
                .bind(counterparty_channel_id)
                .bind(client_id)
                .execute(pool)
                .await;

                if let Err(e) = result {
                    error!("Failed to update client channel info in database: {}", e);
                }
            }
        }
        Err(e) => {
            error!("Failed to check client channels: {}", e);
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub fn start_ibc_status_scheduler(pool: sqlx::PgPool) {
    tokio::spawn(async move {
        check_ibc_clients(&pool).await;
        let mut interval = time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            check_ibc_clients(&pool).await;
        }
    });
    info!("Started IBC client status scheduler (running hourly)");
}
