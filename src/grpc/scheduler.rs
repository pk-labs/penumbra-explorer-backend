use super::client::{
    query_all_client_channels, query_all_clients, GrpcClient, CLIENT_STATUS_ACTIVE,
    CLIENT_STATUS_EXPIRED, CLIENT_STATUS_FROZEN, CLIENT_STATUS_UNKNOWN,
};
use std::time::Duration;
use tokio::time;
use tracing::{error, info};

async fn check_client_statuses(client: &GrpcClient) {
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
        }
        Err(e) => {
            error!("Failed to query IBC clients: {}", e);
        }
    }
}

async fn check_client_channels(client: &GrpcClient) {
    match query_all_client_channels(client).await {
        Ok(client_channels) => {
            info!("=== IBC Client Open Channels Summary ===");
            for (client_id, channels) in &client_channels {
                if channels.is_empty() {
                    info!("Client: {} - No open channels found", client_id);
                } else {
                    info!(
                        "Client: {} - Found {} open channels:",
                        client_id,
                        channels.len()
                    );
                    for (port_id, channel_id, counterparty_channel_id) in channels {
                        info!(
                            "    Port: {}, Channel: {}, Counterparty Channel: {}",
                            port_id, channel_id, counterparty_channel_id
                        );
                    }
                }
            }
            info!("=====================================");
        }
        Err(e) => {
            error!("Failed to query IBC client channels: {}", e);
        }
    }
}

async fn check_ibc_clients() {
    info!("Running scheduled IBC client status check");
    let client = GrpcClient::new("grpc.penumbra.silentvalidator.com", 443);

    // Check client statuses
    check_client_statuses(&client).await;

    // Check client channels
    check_client_channels(&client).await;
}

#[allow(clippy::module_name_repetitions)]
pub fn start_ibc_status_scheduler() {
    tokio::spawn(async {
        check_ibc_clients().await;
        let mut interval = time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            check_ibc_clients().await;
        }
    });
    info!("Started IBC client status scheduler (running hourly)");
}
