use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use regex::Regex;
use serde_json::Value;
use sqlx::{
    types::chrono::{DateTime, Utc},
    Row,
};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Direction of an IBC transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Inbound => write!(f, "inbound"),
            Direction::Outbound => write!(f, "outbound"),
        }
    }
}

/// Status of an IBC transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Pending,
    Completed,
    Expired,
    Error,
}

impl std::fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionStatus::Pending => write!(f, "pending"),
            TransactionStatus::Completed => write!(f, "completed"),
            TransactionStatus::Expired => write!(f, "expired"),
            TransactionStatus::Error => write!(f, "error"),
        }
    }
}

// Add this constant for the USDC asset ID
const USDC_ASSET_ID: &[u8] = &[0x75, 0x73, 0x64, 0x63]; // "usdc" in hex

// Helper function - move to top to ensure it's in scope for all functions that use it
fn find_attribute_value<'a>(event: &'a ContextualizedEvent<'_>, key: &str) -> Option<&'a str> {
    for attr in &event.event.attributes {
        if let Ok(attr_key) = attr.key_str() {
            if attr_key == key {
                if let Ok(attr_value) = attr.value_str() {
                    return Some(attr_value);
                }
            }
        }
    }
    None
}

/// Extract any error information from the acknowledgment packet data
/// Returns true if an error is detected, false otherwise
fn extract_error_from_ack(ack_data: &str) -> bool {
    let error_patterns = [
        "\"error\":",
        "\"Error\":",
        "\"ERROR\":",
        "failed",
        "Failed",
        "FAILED",
        "reject",
        "Reject",
        "REJECT",
        "insufficient",
        "Insufficient",
        "invalid",
        "Invalid",
        "INVALID",
        "REASON_ERROR",
        "reason error",
        "Reason Error",
        "timeout",
        "Timeout",
        "TIMEOUT",
    ];

    for pattern in error_patterns {
        if ack_data.contains(pattern) {
            return true;
        }
    }

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(ack_data) {
        if json.get("error").is_some() || json.get("Error").is_some() || json.get("ERROR").is_some()
        {
            return true;
        }

        if let Some(result) = json.get("result") {
            if result.get("error").is_some()
                || result.is_string() && result.as_str().unwrap_or("").contains("error")
            {
                return true;
            }
        }

        if let Some(code) = json.get("code") {
            if let Some(code_num) = code.as_u64() {
                if code_num != 0 {
                    return true;
                }
            }
        }
    }

    false
}

/// Update asset price from candlestick data
///
/// # Errors
/// Returns an error if database operations fail
async fn update_asset_price(
    dbtx: &mut PgTransaction<'_>,
    asset_id: &[u8],
    price_usd: f64,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    // Update current price
    sqlx::query(
        r"
        INSERT INTO asset_prices (asset_id, price_usd, last_updated)
        VALUES ($1, $2, $3)
        ON CONFLICT (asset_id)
        DO UPDATE SET
            price_usd = $2,
            last_updated = $3
        ",
    )
        .bind(asset_id)
        .bind(price_usd)
        .bind(timestamp)
        .execute(dbtx.as_mut())
        .await?;

    debug!(
        "Updated price for asset {}: ${}",
        hex::encode(asset_id),
        price_usd
    );

    Ok(())
}

/// Get the latest price for an asset
///
/// # Errors
/// Returns an error if database operations fail
async fn get_asset_price(
    dbtx: &mut PgTransaction<'_>,
    asset_id: &[u8],
) -> Result<f64, anyhow::Error> {
    // If asset is USDC, return 1.0 directly
    if asset_id == USDC_ASSET_ID {
        return Ok(1.0);
    }

    // Query the latest price
    let price: Option<f64> = sqlx::query_scalar(
        "SELECT price_usd FROM asset_prices WHERE asset_id = $1"
    )
        .bind(asset_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

    Ok(price.unwrap_or(0.0))
}

/// Extract asset ID from event metadata and value
fn extract_asset_id(meta: &Value, value: &Value) -> Option<Vec<u8>> {
    // Try to get from value.asset_id
    if let Some(asset_id) = value.get("asset_id") {
        if let Some(asset_id_str) = asset_id.as_str() {
            return Some(hex::decode(asset_id_str).unwrap_or_default());
        }
    }

    // Try to get from value.value.asset_id
    if let Some(value_inner) = value.get("value") {
        if let Some(asset_id) = value_inner.get("asset_id") {
            if let Some(asset_id_str) = asset_id.as_str() {
                return Some(hex::decode(asset_id_str).unwrap_or_default());
            }
        }
    }

    // Try to get from meta
    if let Some(asset_id) = meta.get("asset_id") {
        if let Some(asset_id_str) = asset_id.as_str() {
            return Some(hex::decode(asset_id_str).unwrap_or_default());
        }
    }

    // Try to get from meta.denom
    if let Some(denom) = meta.get("denom") {
        if let Some(denom_str) = denom.as_str() {
            // This is a simplified approach - in practice you might need
            // more sophisticated parsing depending on your denom format
            return Some(denom_str.as_bytes().to_vec());
        }
    }

    None
}

/// Process candlestick data to update asset prices
async fn process_candlestick_data(
    dbtx: &mut PgTransaction<'_>,
    event: &ContextualizedEvent<'_>,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    if event.event.kind.as_str() != "penumbra.core.component.dex.v1.EventCandlestickData" {
        return Ok(());
    }

    debug!("Processing candlestick data from event at height {}", event.block_height);

    // Extract candlestick data from event attributes
    let mut base_asset_id: Option<Vec<u8>> = None;
    let mut quote_asset_id: Option<Vec<u8>> = None;
    let mut close_price: Option<f64> = None;

    for attr in &event.event.attributes {
        if let (Ok(key), Ok(value)) = (attr.key_str(), attr.value_str()) {
            match key {
                "pair.base" => {
                    base_asset_id = Some(hex::decode(value).unwrap_or_default());
                    debug!("Found base asset ID: {}", value);
                },
                "pair.quote" => {
                    quote_asset_id = Some(hex::decode(value).unwrap_or_default());
                    debug!("Found quote asset ID: {}", value);
                },
                "stick.close" => {
                    close_price = value.parse::<f64>().ok();
                    debug!("Found close price: {}", value);
                },
                _ => {}
            }
        }
    }

    // Process the candlestick data if we have all components
    // Clone the values to avoid moving them
    let base_asset_id_clone = base_asset_id.clone();
    let quote_asset_id_clone = quote_asset_id.clone();
    let close_price_clone = close_price;

    if let (Some(base), Some(quote), Some(price)) = (base_asset_id_clone, quote_asset_id_clone, close_price_clone) {
        debug!(
            "Processing candlestick with base={}, quote={}, price={}",
            hex::encode(&base),
            hex::encode(&quote),
            price
        );

        if quote == USDC_ASSET_ID {
            // This is a direct USDC pair, so we have USD price directly
            update_asset_price(dbtx, &base, price, timestamp).await?;
            debug!("Updated price for asset {} against USDC: ${}", hex::encode(&base), price);
        } else if base == USDC_ASSET_ID {
            // This is an inverse USDC pair, calculate USD price as 1/price
            if price > 0.0 {
                let inverse_price = 1.0 / price;
                update_asset_price(dbtx, &quote, inverse_price, timestamp).await?;
                debug!("Updated price for asset {} (inverse USDC pair): ${}",
                       hex::encode(&quote), inverse_price);
            }
        } else {
            debug!(
                "Skipping non-USDC candlestick: {} / {}",
                hex::encode(&base),
                hex::encode(&quote)
            );
        }
    } else {
        debug!("Incomplete candlestick data: base={:?}, quote={:?}, price={:?}",
               base_asset_id.as_ref().map(|v| hex::encode(v)),
               quote_asset_id.as_ref().map(|v| hex::encode(v)),
               close_price);
    }

    Ok(())
}

/// Extract numeric portion from channel ID (e.g., "channel-42" -> 42)
fn extract_number_from_channel(channel_id: &str) -> Option<u64> {
    let parts: Vec<&str> = channel_id.split('-').collect();
    if parts.len() >= 2 {
        if let Ok(num) = parts[1].parse::<u64>() {
            return Some(num);
        }
    }
    None
}

/// Checks if a specific sequence has a refund event in the event list
///
/// This function looks for:
/// 1. `EventOutboundFungibleTokenRefund` events for the specific sequence
/// 2. Error indicators in the event's reason attribute
fn has_refund_event(events: &[ContextualizedEvent<'_>], sequence: &str) -> bool {
    for event in events {
        if event.event.kind.as_str()
            == "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund"
        {
            if let Some(meta) = find_attribute_value(event, "meta") {
                if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                    if let Some(event_seq) = meta_json.get("sequence").and_then(|s| s.as_str()) {
                        if event_seq == sequence {
                            if let Some(reason) = find_attribute_value(event, "reason") {
                                if reason.contains("ERROR") || reason.contains("REASON_ERROR") {
                                    debug!(
                                        "Found refund event with error reason for sequence {}: {}",
                                        sequence, reason
                                    );
                                    return true;
                                }
                            } else {
                                debug!(
                                    "Found refund event for sequence {} without specific reason",
                                    sequence
                                );
                                return true;
                            }
                        }
                    }
                } else if meta.contains(&format!("\"sequence\":\"{sequence}\","))
                    || meta.contains(&format!("\"sequence\":\"{sequence}\"}}"))
                {
                    debug!(
                        "Found refund event for sequence {} via string matching",
                        sequence
                    );
                    return true;
                }
            }

            if let Some(event_seq) = find_attribute_value(event, "sequence") {
                if event_seq == sequence {
                    debug!(
                        "Found refund event for sequence {} via direct attribute",
                        sequence
                    );
                    return true;
                }
            }
        }
    }

    false
}

/// Helper function to determine if an `acknowledge_packet` contains an error
/// by analyzing its `packet_ack` data
#[allow(dead_code)]
fn is_error_acknowledgment(event: &ContextualizedEvent<'_>) -> bool {
    if event.event.kind.as_str() != "acknowledge_packet" {
        return false;
    }

    if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
        return extract_error_from_ack(ack_data);
    }

    false
}

/// Record an individual IBC transfer in the time series table
///
/// # Errors
/// Returns an error if database operations fail
#[allow(clippy::too_many_arguments)]
pub async fn record_transfer(
    dbtx: &mut PgTransaction<'_>,
    client_id: &str,
    channel_id: &str,
    direction: Direction,
    amount: &str,
    timestamp: DateTime<Utc>,
    tx_hash: Option<Vec<u8>>,
    status: TransactionStatus,
) -> Result<(), anyhow::Error> {
    // Try to parse the amount as numeric - DIRECTLY bind as numeric instead of string
    let amount_value = amount.parse::<i64>().unwrap_or_default();

    let tx_status = status.to_string();

    match sqlx::query(
        r"
        INSERT INTO ibc_transfers (
            client_id,
            channel_id,
            direction,
            amount,
            timestamp,
            tx_hash,
            status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ",
    )
        .bind(client_id)
        .bind(channel_id)
        .bind(direction.to_string())
        .bind(amount_value) // Bind as numeric i64 value directly
        .bind(timestamp)
        .bind(tx_hash)
        .bind(&tx_status)
        .execute(dbtx.as_mut())
        .await
    {
        Ok(_) => {
            debug!(
                "Recorded {} IBC transfer: client={}, channel={}, amount={}, status={}",
                direction, client_id, channel_id, amount_value, tx_status
            );
            Ok(())
        }
        Err(e) => {
            error!("Failed to record {} IBC transfer: {}", direction, e);
            Err(e.into())
        }
    }
}

/// Update the status of an IBC transfer
///
/// # Errors
/// Returns an error if database operations fail
pub async fn update_transfer_status(
    dbtx: &mut PgTransaction<'_>,
    tx_hash: &[u8],
    status: TransactionStatus,
) -> Result<(), anyhow::Error> {
    match sqlx::query(
        r"
        UPDATE ibc_transfers
        SET status = $2
        WHERE tx_hash = $1
        ",
    )
        .bind(tx_hash)
        .bind(status.to_string())
        .execute(dbtx.as_mut())
        .await
    {
        Ok(result) => {
            if result.rows_affected() > 0 {
                debug!(
                    "Updated transfer status for tx hash: {}, new status: {}",
                    hex::encode(tx_hash),
                    status
                );
            } else {
                debug!("No transfer found with tx hash: {}", hex::encode(tx_hash));
            }
            Ok(())
        }
        Err(e) => {
            error!("Failed to update transfer status: {}", e);
            Err(e.into())
        }
    }
}

/// Update client statistics with USD-denominated volume
async fn update_client_stats_with_usd(
    dbtx: &mut PgTransaction<'_>,
    client_id: &str,
    direction: Direction,
    amount: &str,
    asset_id: &[u8],
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    // Parse the amount
    let amount_value = amount.parse::<i64>().unwrap_or_default();

    // Get USD price and calculate USD amount
    let price = get_asset_price(dbtx, asset_id).await?;
    let usd_amount = if price > 0.0 {
        amount_value as f64 * price
    } else {
        0.0
    };

    // Use separate queries based on direction instead of dynamic SQL
    match direction {
        Direction::Inbound => {
            match sqlx::query(
                r"
                UPDATE ibc_stats
                SET
                    shielded_volume = shielded_volume + $2,
                    shielded_tx_count = shielded_tx_count + 1,
                    last_updated = $3
                WHERE client_id = $1
                ",
            )
                .bind(client_id)
                .bind(usd_amount)
                .bind(timestamp)
                .execute(dbtx.as_mut())
                .await
            {
                Ok(_) => {
                    debug!(
                        "Updated USD stats for inbound transfer: client={}, amount=${:.2} (raw amount={}, price=${:.4})",
                        client_id, usd_amount, amount_value, price
                    );
                    Ok(())
                }
                Err(e) => {
                    error!(
                        "Error updating USD stats for inbound transfer: {}",
                        e
                    );

                    // Fallback to just incrementing tx count if USD update fails
                    if let Err(e) = sqlx::query(
                        r"
                        UPDATE ibc_stats
                        SET
                            shielded_tx_count = shielded_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        ",
                    )
                        .bind(client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await
                    {
                        error!("Failed to update fallback stats for inbound transfer: {}", e);
                    }

                    Err(e.into())
                }
            }
        }
        Direction::Outbound => {
            match sqlx::query(
                r"
                UPDATE ibc_stats
                SET
                    unshielded_volume = unshielded_volume + $2,
                    unshielded_tx_count = unshielded_tx_count + 1,
                    last_updated = $3
                WHERE client_id = $1
                ",
            )
                .bind(client_id)
                .bind(usd_amount)
                .bind(timestamp)
                .execute(dbtx.as_mut())
                .await
            {
                Ok(_) => {
                    debug!(
                        "Updated USD stats for outbound transfer: client={}, amount=${:.2} (raw amount={}, price=${:.4})",
                        client_id, usd_amount, amount_value, price
                    );
                    Ok(())
                }
                Err(e) => {
                    error!(
                        "Error updating USD stats for outbound transfer: {}",
                        e
                    );

                    // Fallback to just incrementing tx count if USD update fails
                    if let Err(e) = sqlx::query(
                        r"
                        UPDATE ibc_stats
                        SET
                            unshielded_tx_count = unshielded_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        ",
                    )
                        .bind(client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await
                    {
                        error!("Failed to update fallback stats for outbound transfer: {}", e);
                    }

                    Err(e.into())
                }
            }
        }
    }
}

/// Process IBC events from a block
///
/// # Errors
/// Returns an error if database operations fail
#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_wrap,
    clippy::needless_raw_string_hashes
)]
pub async fn process_events(
    dbtx: &mut PgTransaction<'_>,
    events: &[ContextualizedEvent<'_>],
    height: u64,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    debug!(
        "Processing IBC events for block {} with {} events",
        height,
        events.len()
    );

    // First, process candlestick events to update asset prices
    for event in events {
        if event.event.kind.as_str() == "penumbra.core.component.dex.v1.EventCandlestickData" {
            debug!("Found candlestick event in block {}", height);
            if let Err(e) = process_candlestick_data(dbtx, event, timestamp).await {
                error!("Failed to process candlestick data: {}", e);
            }
        }
    }

    let mut client_connections: HashMap<String, String> = HashMap::new();
    let mut connection_channels: HashMap<String, String> = HashMap::new();

    let mut refunded_sequences: HashMap<String, bool> = HashMap::new();
    let mut error_acknowledgments: HashMap<String, bool> = HashMap::new();

    for event in events {
        if event.event.kind.as_str()
            == "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund"
        {
            let mut sequence = None;

            if let Some(meta) = find_attribute_value(event, "meta") {
                if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                    sequence = meta_json
                        .get("sequence")
                        .and_then(|s| s.as_str())
                        .map(ToString::to_string);
                } else if let Ok(re) = Regex::new(r#""sequence":"([^"]+)""#) {
                    if let Some(captures) = re.captures(meta) {
                        if let Some(seq_match) = captures.get(1) {
                            sequence = Some(seq_match.as_str().to_string());
                        }
                    }
                }
            }

            if sequence.is_none() {
                sequence = find_attribute_value(event, "sequence").map(ToString::to_string);
            }

            if let Some(seq) = sequence {
                refunded_sequences.insert(seq.clone(), true);
                debug!("Found refund event for sequence {}", seq);

                if let Some(reason) = find_attribute_value(event, "reason") {
                    debug!("Refund reason for sequence {}: {}", seq, reason);
                    if reason.contains("ERROR") || reason.contains("REASON_ERROR") {
                        debug!("Confirmed error via reason attribute for sequence {}", seq);
                    }
                }
            }
        } else if event.event.kind.as_str() == "acknowledge_packet" {
            if let Some(sequence) = find_attribute_value(event, "packet_sequence") {
                if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    if extract_error_from_ack(ack_data) {
                        debug!(
                            "Found error in ack_data for sequence {}: {}",
                            sequence, ack_data
                        );
                        error_acknowledgments.insert(sequence.to_string(), true);
                    }
                }
            }
        }
    }

    let mut known_clients = Vec::new();
    let client_rows = sqlx::query("SELECT client_id FROM ibc_clients")
        .fetch_all(dbtx.as_mut())
        .await?;
    for row in client_rows {
        let client_id: String = row.get(0);
        known_clients.push(client_id);
    }

    if known_clients.is_empty() {
        info!("No clients found in database");
    } else {
        debug!("Found {} existing clients in database", known_clients.len());
    }

    for event in events {
        match event.event.kind.as_str() {
            "create_client" => {
                if let Some(client_id) = find_attribute_value(event, "client_id") {
                    sqlx::query(
                        r"
                        INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (client_id)
                        DO UPDATE SET
                            last_active_height = $2,
                            last_active_time = $3
                        ",
                    )
                        .bind(client_id)
                        .bind(height as i64)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    sqlx::query(
                        r"
                        INSERT INTO ibc_stats (
                            client_id,
                            shielded_volume, shielded_tx_count,
                            unshielded_volume, unshielded_tx_count,
                            pending_tx_count, expired_tx_count,
                            last_updated
                        )
                        VALUES ($1, 0, 0, 0, 0, 0, 0, $2)
                        ON CONFLICT (client_id) DO NOTHING
                        ",
                    )
                        .bind(client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if !known_clients.contains(&client_id.to_string()) {
                        known_clients.push(client_id.to_string());
                    }

                    debug!("Processed create_client: {}", client_id);
                }
            }
            "connection_open_init" => {
                if let (Some(client_id), Some(connection_id)) = (
                    find_attribute_value(event, "client_id"),
                    find_attribute_value(event, "connection_id"),
                ) {
                    client_connections.insert(connection_id.to_string(), client_id.to_string());

                    sqlx::query(
                        r"
                        INSERT INTO ibc_clients (client_id, last_active_height, last_active_time)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (client_id) DO NOTHING
                        ",
                    )
                        .bind(client_id)
                        .bind(height as i64)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if !known_clients.contains(&client_id.to_string()) {
                        known_clients.push(client_id.to_string());
                    }

                    debug!(
                        "Processed connection_open_init: {} -> {}",
                        connection_id, client_id
                    );
                }
            }
            "channel_open_ack" => {
                if let (Some(channel_id), Some(counterparty_channel_id)) = (
                    find_attribute_value(event, "channel_id"),
                    find_attribute_value(event, "counterparty_channel_id"),
                ) {
                    debug!(
                        "Found channel_open_ack for channel {} with counterparty channel {}",
                        channel_id, counterparty_channel_id
                    );

                    sqlx::query(
                        r"
                        UPDATE ibc_channels
                        SET counterparty_channel_id = $2
                        WHERE channel_id = $1
                        ",
                    )
                        .bind(channel_id)
                        .bind(counterparty_channel_id)
                        .execute(dbtx.as_mut())
                        .await?;
                }
            }
            "channel_open_init" => {
                if let (Some(channel_id), Some(connection_id)) = (
                    find_attribute_value(event, "channel_id"),
                    find_attribute_value(event, "connection_id"),
                ) {
                    connection_channels.insert(connection_id.to_string(), channel_id.to_string());

                    let client_id = client_connections.get(connection_id).cloned();

                    if let Some(client_id) = client_id {
                        sqlx::query(
                            r"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (channel_id)
                            DO UPDATE SET
                                client_id = $2,
                                connection_id = $3
                            ",
                        )
                            .bind(channel_id)
                            .bind(&client_id)
                            .bind(connection_id)
                            .execute(dbtx.as_mut())
                            .await?;

                        debug!(
                            "Processed channel_open_init: {} -> {}",
                            channel_id, client_id
                        );
                    } else if let Some(channel_num) = extract_number_from_channel(channel_id) {
                        if known_clients.is_empty() {
                            warn!(
                                "Cannot associate channel {}: no clients available",
                                channel_id
                            );
                        } else {
                            let idx =
                                usize::try_from(channel_num).unwrap_or(0) % known_clients.len();
                            let selected_client = &known_clients[idx];

                            sqlx::query(
                                r"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (channel_id)
                                DO UPDATE SET
                                    client_id = $2,
                                    connection_id = $3
                                ",
                            )
                                .bind(channel_id)
                                .bind(selected_client)
                                .bind(connection_id)
                                .execute(dbtx.as_mut())
                                .await?;

                            debug!(
                                "Associated channel {} with client {} (deterministic mapping)",
                                channel_id, selected_client
                            );
                        }
                    } else if !known_clients.is_empty() {
                        let default_client = &known_clients[0];

                        sqlx::query(
                            r"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (channel_id)
                            DO UPDATE SET
                                client_id = $2,
                                connection_id = $3
                            ",
                        )
                            .bind(channel_id)
                            .bind(default_client)
                            .bind(connection_id)
                            .execute(dbtx.as_mut())
                            .await?;

                        debug!(
                            "Associated channel {} with first available client {}",
                            channel_id, default_client
                        );
                    } else {
                        warn!(
                            "Cannot associate channel {}: no clients available",
                            channel_id
                        );
                    }
                }
            }
            _ => {}
        }
    }

    for event in events {
        match event.event.kind.as_str() {
            "send_packet" => {
                let (Some(src_channel), Some(dst_channel), Some(sequence)) = (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) else {
                    continue;
                };

                let packet_data = find_attribute_value(event, "packet_data").unwrap_or_default();

                let direction = if packet_data.contains("\"receiver\":\"penumbra") {
                    Direction::Inbound
                } else if packet_data.contains("\"sender\":\"penumbra") {
                    Direction::Outbound
                } else {
                    continue;
                };

                let our_channel = match direction {
                    Direction::Inbound => dst_channel,
                    Direction::Outbound => src_channel,
                };

                let counterparty_channel = match direction {
                    Direction::Inbound => src_channel,
                    Direction::Outbound => dst_channel,
                };

                // Update counterparty channel ID and also add debugging
                debug!(
                    "Updating counterparty for channel {} to {} (direction: {})",
                    our_channel, counterparty_channel, direction
                );

                sqlx::query(
                    r"
                    UPDATE ibc_channels
                    SET counterparty_channel_id = $2
                    WHERE channel_id = $1
                    ",
                )
                    .bind(our_channel)
                    .bind(counterparty_channel)
                    .execute(dbtx.as_mut())
                    .await?;

                if let Some(rows_affected) = sqlx::query(
                    r"
                    UPDATE ibc_channels
                    SET counterparty_channel_id = $2
                    WHERE channel_id = $1
                    AND connection_id = 'auto-connection'
                    AND (counterparty_channel_id IS NULL OR counterparty_channel_id = '')
                    ",
                )
                    .bind(counterparty_channel)
                    .bind(our_channel)
                    .execute(dbtx.as_mut())
                    .await
                    .ok()
                    .map(|r| r.rows_affected())
                {
                    if rows_affected > 0 {
                        debug!(
                            "Also updated reverse mapping: {} -> {}",
                            counterparty_channel, our_channel
                        );
                    }
                }

                let mut final_client_id: Option<String> = None;

                let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT client_id FROM ibc_channels WHERE channel_id = $1",
                )
                    .bind(our_channel)
                    .fetch_optional(dbtx.as_mut())
                    .await?;

                // FIX #1: Properly handle Option<String>
                final_client_id = db_client_id.flatten();

                if final_client_id.is_none() && our_channel.starts_with("channel-") {
                    let available_clients: Vec<String> = known_clients.clone();

                    if available_clients.is_empty() {
                        warn!(
                            "Cannot associate channel {}: no clients available",
                            our_channel
                        );
                    } else if let Some(channel_num) = extract_number_from_channel(our_channel) {
                        let idx =
                            usize::try_from(channel_num).unwrap_or(0) % available_clients.len();
                        let selected_client = available_clients[idx].clone();

                        info!(
                            "Associating channel {} with client {} via deterministic mapping",
                            our_channel, selected_client
                        );

                        sqlx::query(
                            r"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id, counterparty_channel_id)
                            VALUES ($1, $2, 'auto-connection', $3)
                            ON CONFLICT (channel_id)
                            DO UPDATE SET
                                counterparty_channel_id = $3
                            ",
                        )
                            .bind(our_channel)
                            .bind(&selected_client)
                            .bind(counterparty_channel)
                            .execute(dbtx.as_mut())
                            .await?;

                        final_client_id = Some(selected_client);
                    } else {
                        let selected_client = available_clients[0].clone();

                        info!(
                            "Associating unnumbered channel {} with default client {}",
                            our_channel, selected_client
                        );

                        sqlx::query(
                            r"
                            INSERT INTO ibc_channels (channel_id, client_id, connection_id, counterparty_channel_id)
                            VALUES ($1, $2, 'auto-connection', $3)
                            ON CONFLICT (channel_id)
                            DO UPDATE SET
                                counterparty_channel_id = $3
                            ",
                        )
                            .bind(our_channel)
                            .bind(&selected_client)
                            .bind(counterparty_channel)
                            .execute(dbtx.as_mut())
                            .await?;

                        final_client_id = Some(selected_client);
                    }
                }

                if let (Some(client_id), Some(tx_hash)) = (final_client_id, event.tx_hash()) {
                    // Update explorer transaction
                    sqlx::query(
                        r"
                        UPDATE explorer_transactions
                        SET
                            ibc_channel_id = $2,
                            ibc_client_id = $3,
                            ibc_status = $4,
                            ibc_direction = $5,
                            ibc_sequence = $6
                        WHERE tx_hash = $1
                        ",
                    )
                        .bind(tx_hash)
                        .bind(our_channel)
                        .bind(&client_id)
                        .bind(TransactionStatus::Pending.to_string())
                        .bind(direction.to_string())
                        .bind(sequence)
                        .execute(dbtx.as_mut())
                        .await?;

                    sqlx::query(
                        r"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = pending_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        ",
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if let Err(e) = record_transfer(
                        dbtx,
                        &client_id,
                        our_channel,
                        direction,
                        "0",
                        timestamp,
                        Some(tx_hash.to_vec()),
                        TransactionStatus::Pending,
                    )
                        .await
                    {
                        error!("Failed to record pending transfer: {}", e);
                    }

                    debug!(
                        "Processed send_packet for channel {} with client {}",
                        our_channel, client_id
                    );
                }
            }
            "acknowledge_packet" => {
                let (Some(src_channel), Some(dst_channel), Some(sequence)) = (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) else {
                    continue;
                };

                let is_error = if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    extract_error_from_ack(ack_data)
                } else {
                    false
                };

                let has_refund = refunded_sequences.contains_key(sequence);

                let has_direct_refund = has_refund_event(events, sequence);

                let status = if is_error || has_refund || has_direct_refund {
                    TransactionStatus::Error
                } else {
                    TransactionStatus::Completed
                };

                debug!(
                    "Processing acknowledge_packet for sequence {}: status={} (is_error={}, has_refund={}, has_direct_refund={})",
                    sequence, status, is_error, has_refund, has_direct_refund
                );

                if let Some(ack_data) = find_attribute_value(event, "packet_ack") {
                    if !ack_data.is_empty() {
                        debug!("Ack data for sequence {}: {}", sequence, ack_data);
                    }
                }

                let updated_rows = sqlx::query(
                    r"
                    WITH updated_tx AS (
                        UPDATE explorer_transactions
                        SET ibc_status = $1
                        WHERE ibc_sequence = $2
                        AND (
                            (ibc_direction = 'inbound' AND ibc_channel_id = $3)
                            OR
                            (ibc_direction = 'outbound' AND ibc_channel_id = $4)
                        )
                        AND ibc_status = 'pending'
                        RETURNING ibc_client_id, tx_hash
                    )
                    SELECT ibc_client_id, tx_hash FROM updated_tx
                    ",
                )
                    .bind(status.to_string())
                    .bind(sequence)
                    .bind(dst_channel)
                    .bind(src_channel)
                    .fetch_all(dbtx.as_mut())
                    .await?;

                for row in updated_rows {
                    let client_id: String = row.get(0);
                    let tx_hash: Vec<u8> = row.try_get(1)?;

                    sqlx::query(
                        r"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = GREATEST(0, pending_tx_count - 1),
                            last_updated = $2
                        WHERE client_id = $1
                        ",
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if let Err(e) = update_transfer_status(dbtx, &tx_hash, status).await {
                        error!("Failed to update transfer status: {}", e);
                    }

                    if status == TransactionStatus::Error {
                        debug!("Updated transaction {} to ERROR for client {} (error indicators found)",
                               hex::encode(&tx_hash), client_id);
                    } else {
                        debug!(
                            "Updated transaction {} to COMPLETED for client {}",
                            hex::encode(&tx_hash),
                            client_id
                        );
                    }
                }
            }
            "timeout_packet" => {
                let (Some(src_channel), Some(dst_channel), Some(sequence)) = (
                    find_attribute_value(event, "packet_src_channel"),
                    find_attribute_value(event, "packet_dst_channel"),
                    find_attribute_value(event, "packet_sequence"),
                ) else {
                    continue;
                };

                let updated_rows = sqlx::query(
                    r"
                    WITH updated_tx AS (
                        UPDATE explorer_transactions
                        SET ibc_status = $1
                        WHERE ibc_sequence = $2
                        AND (
                            (ibc_direction = 'inbound' AND ibc_channel_id = $3)
                            OR
                            (ibc_direction = 'outbound' AND ibc_channel_id = $4)
                        )
                        AND ibc_status = 'pending'
                        RETURNING ibc_client_id, tx_hash
                    )
                    SELECT ibc_client_id, tx_hash FROM updated_tx
                    ",
                )
                    .bind(TransactionStatus::Expired.to_string())
                    .bind(sequence)
                    .bind(dst_channel)
                    .bind(src_channel)
                    .fetch_all(dbtx.as_mut())
                    .await?;

                for row in updated_rows {
                    let client_id: String = row.get(0);
                    let tx_hash: Vec<u8> = row.try_get(1)?;

                    sqlx::query(
                        r"
                        UPDATE ibc_stats
                        SET
                            pending_tx_count = GREATEST(0, pending_tx_count - 1),
                            expired_tx_count = expired_tx_count + 1,
                            last_updated = $2
                        WHERE client_id = $1
                        ",
                    )
                        .bind(&client_id)
                        .bind(timestamp)
                        .execute(dbtx.as_mut())
                        .await?;

                    if let Err(e) =
                        update_transfer_status(dbtx, &tx_hash, TransactionStatus::Expired).await
                    {
                        error!("Failed to update transfer status: {}", e);
                    }

                    debug!("Updated transaction to expired for client {}", client_id);
                }
            }
            "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenRefund" => {
                let mut sequence = None;

                if let Some(meta) = find_attribute_value(event, "meta") {
                    if let Ok(meta_json) = serde_json::from_str::<Value>(meta) {
                        sequence = meta_json
                            .get("sequence")
                            .and_then(|s| s.as_str())
                            .map(ToString::to_string);
                    } else if let Ok(re) = Regex::new(r#""sequence":"([^"]+)""#) {
                        if let Some(captures) = re.captures(meta) {
                            if let Some(seq_match) = captures.get(1) {
                                sequence = Some(seq_match.as_str().to_string());
                            }
                        }
                    }
                }

                if sequence.is_none() {
                    sequence = find_attribute_value(event, "sequence").map(ToString::to_string);
                }

                if let Some(seq) = sequence {
                    let is_error = if let Some(reason) = find_attribute_value(event, "reason") {
                        debug!("Refund reason for sequence {}: {}", seq, reason);
                        reason.contains("ERROR") || reason.contains("REASON_ERROR")
                    } else {
                        true
                    };

                    if is_error {
                        let updated_rows = sqlx::query(
                            r"
                            WITH updated_tx AS (
                                UPDATE explorer_transactions
                                SET ibc_status = $1
                                WHERE ibc_sequence = $2
                                RETURNING ibc_client_id, tx_hash, ibc_status
                            )
                            SELECT ibc_client_id, tx_hash, ibc_status FROM updated_tx
                            ",
                        )
                            .bind(TransactionStatus::Error.to_string())
                            .bind(&seq)
                            .fetch_all(dbtx.as_mut())
                            .await?;

                        for row in updated_rows {
                            let client_id: String = row.get(0);
                            let tx_hash: Vec<u8> = row.try_get(1)?;
                            let previous_status: String = row.try_get(2).unwrap_or_default();

                            if previous_status == "pending" {
                                sqlx::query(
                                    r"
                                    UPDATE ibc_stats
                                    SET
                                        pending_tx_count = GREATEST(0, pending_tx_count - 1),
                                        last_updated = $2
                                    WHERE client_id = $1
                                    ",
                                )
                                    .bind(&client_id)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await?;
                            }

                            if let Err(e) =
                                update_transfer_status(dbtx, &tx_hash, TransactionStatus::Error)
                                    .await
                            {
                                error!("Failed to update transfer status for refund: {}", e);
                            }

                            debug!(
                                "Set transaction {} to ERROR due to direct refund event with REASON_ERROR (sequence {})",
                                hex::encode(&tx_hash), seq
                            );
                        }
                    }
                }
            }
            "penumbra.core.component.shielded_pool.v1.EventInboundFungibleTokenTransfer" => {
                let Some(meta) = find_attribute_value(event, "meta") else {
                    continue;
                };

                let Some(value) = find_attribute_value(event, "value") else {
                    continue;
                };

                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let Some(channel_id) = meta.get("channel").and_then(|v| v.as_str()) else {
                        continue;
                    };

                    let amount_raw = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.to_string().trim_matches('"').to_string(),
                        },
                        None => continue,
                    };

                    // Extract asset ID for USD conversion
                    let asset_id = extract_asset_id(&meta, &value);

                    if asset_id.is_none() {
                        debug!("Could not extract asset ID for inbound transfer");
                    } else {
                        debug!("Found asset ID for inbound transfer: {}",
                               hex::encode(asset_id.as_ref().unwrap()));
                    }

                    let mut resolved_client_id: Option<String> = None;

                    let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1",
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    // FIX #2: Properly handle Option<String>
                    resolved_client_id = db_client_id.flatten();

                    if resolved_client_id.is_none() {
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                let idx = usize::try_from(channel_num).unwrap_or(0) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                info!(
                                    "Associating channel {} with client {} via deterministic mapping",
                                    channel_id, selected_client
                                );

                                sqlx::query(
                                    r"
                                    INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                    VALUES ($1, $2, 'auto-connection')
                                    ON CONFLICT (channel_id) DO NOTHING
                                    ",
                                )
                                    .bind(channel_id)
                                    .bind(&selected_client)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                resolved_client_id = Some(selected_client);
                            }
                        } else if !known_clients.is_empty() {
                            let selected_client = known_clients[0].clone();

                            info!(
                                "Associating unnumbered channel {} with first available client {}",
                                channel_id, selected_client
                            );

                            sqlx::query(
                                r"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                ",
                            )
                                .bind(channel_id)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            resolved_client_id = Some(selected_client);
                        } else {
                            warn!(
                                "Cannot attribute transfer: no client found for channel {}",
                                channel_id
                            );
                        }
                    }

                    if let Some(client_id) = resolved_client_id {
                        // First, record the transfer in the standard way
                        if let Err(e) = record_transfer(
                            dbtx,
                            &client_id,
                            channel_id,
                            Direction::Inbound,
                            &amount_raw,
                            timestamp,
                            event.tx_hash().map(|tx| tx.to_vec()),
                            TransactionStatus::Completed,
                        )
                            .await
                        {
                            error!("Failed to record inbound transfer: {}", e);
                        }

                        // Then update the stats with USD amounts if we have an asset ID
                        if let Some(ref asset_id) = asset_id {
                            if let Err(e) = update_client_stats_with_usd(
                                dbtx,
                                &client_id,
                                Direction::Inbound,
                                &amount_raw,
                                asset_id,
                                timestamp,
                            )
                                .await
                            {
                                error!("Failed to update USD stats for inbound transfer: {}", e);

                                // Fall back to legacy update if USD conversion fails
                                if let Err(e) = sqlx::query(
                                    r"
                                    UPDATE ibc_stats
                                    SET
                                        -- Convert to NUMERIC first, then add safely
                                        shielded_volume =
                                            CASE
                                                WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                                    COALESCE(shielded_volume, 0) +
                                                    CASE
                                                        WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                        ELSE CAST($2 AS NUMERIC)
                                                    END
                                                ELSE shielded_volume -- If not valid, don't change
                                            END,
                                        shielded_tx_count = shielded_tx_count + 1,
                                        last_updated = $3
                                    WHERE client_id = $1
                                    ",
                                )
                                    .bind(&client_id)
                                    .bind(&amount_raw)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await
                                {
                                    error!("Failed to update legacy stats (fallback) for inbound transfer: {}", e);
                                }
                            }
                        } else {
                            // Use legacy update mechanism if we couldn't extract asset ID
                            if let Err(e) = sqlx::query(
                                r"
                                UPDATE ibc_stats
                                SET
                                    -- Convert to NUMERIC first, then add safely
                                    shielded_volume =
                                        CASE
                                            WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                                COALESCE(shielded_volume, 0) +
                                                CASE
                                                    WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                    ELSE CAST($2 AS NUMERIC)
                                                END
                                            ELSE shielded_volume -- If not valid, don't change
                                        END,
                                    shielded_tx_count = shielded_tx_count + 1,
                                    last_updated = $3
                                WHERE client_id = $1
                                ",
                            )
                                .bind(&client_id)
                                .bind(&amount_raw)
                                .bind(timestamp)
                                .execute(dbtx.as_mut())
                                .await
                            {
                                error!("Error updating legacy stats for inbound transfer: {}", e);
                            }
                        }
                    } else {
                        warn!(
                            "Cannot attribute transfer: no client found for channel {}",
                            channel_id
                        );
                    }
                }
            }
            "penumbra.core.component.shielded_pool.v1.EventOutboundFungibleTokenTransfer" => {
                let Some(meta) = find_attribute_value(event, "meta") else {
                    continue;
                };

                let Some(value) = find_attribute_value(event, "value") else {
                    continue;
                };

                let meta: Result<serde_json::Value, _> = serde_json::from_str(meta);
                let value: Result<serde_json::Value, _> = serde_json::from_str(value);

                if let (Ok(meta), Ok(value)) = (meta, value) {
                    let Some(channel_id) = meta.get("channel").and_then(|v| v.as_str()) else {
                        continue;
                    };

                    let amount_raw = match value.get("amount").and_then(|v| v.get("lo")) {
                        Some(amount) => match amount.as_str() {
                            Some(s) => s.to_string(),
                            None => amount.to_string().trim_matches('"').to_string(),
                        },
                        None => continue,
                    };

                    // Extract asset ID for USD conversion
                    let asset_id = extract_asset_id(&meta, &value);

                    if asset_id.is_none() {
                        debug!("Could not extract asset ID for outbound transfer");
                    } else {
                        debug!("Found asset ID for outbound transfer: {}",
                               hex::encode(asset_id.as_ref().unwrap()));
                    }

                    let mut resolved_client_id: Option<String> = None;

                    let db_client_id = sqlx::query_scalar::<_, Option<String>>(
                        "SELECT client_id FROM ibc_channels WHERE channel_id = $1",
                    )
                        .bind(channel_id)
                        .fetch_optional(dbtx.as_mut())
                        .await?;

                    // FIX #3: Properly handle Option<String>
                    resolved_client_id = db_client_id.flatten();

                    if resolved_client_id.is_none() {
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            let all_clients: Vec<String> = known_clients.clone();

                            if !all_clients.is_empty() {
                                let idx = usize::try_from(channel_num).unwrap_or(0) % all_clients.len();
                                let selected_client = all_clients[idx].clone();

                                info!(
                                    "Associating channel {} with client {} via deterministic mapping",
                                    channel_id, selected_client
                                );

                                sqlx::query(
                                    r"
                                    INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                    VALUES ($1, $2, 'auto-connection')
                                    ON CONFLICT (channel_id) DO NOTHING
                                    ",
                                )
                                    .bind(channel_id)
                                    .bind(&selected_client)
                                    .execute(dbtx.as_mut())
                                    .await?;

                                resolved_client_id = Some(selected_client);
                            }
                        } else if !known_clients.is_empty() {
                            let selected_client = known_clients[0].clone();

                            info!(
                                "Associating unnumbered channel {} with first available client {}",
                                channel_id, selected_client
                            );

                            sqlx::query(
                                r"
                                INSERT INTO ibc_channels (channel_id, client_id, connection_id)
                                VALUES ($1, $2, 'auto-connection')
                                ON CONFLICT (channel_id) DO NOTHING
                                ",
                            )
                                .bind(channel_id)
                                .bind(&selected_client)
                                .execute(dbtx.as_mut())
                                .await?;

                            resolved_client_id = Some(selected_client);
                        } else {
                            warn!(
                                "Cannot associate channel {}: no clients available",
                                channel_id
                            );
                        }
                    }

                    if let Some(client_id) = resolved_client_id {
                        // First, record the transfer in the standard way
                        if let Err(e) = record_transfer(
                            dbtx,
                            &client_id,
                            channel_id,
                            Direction::Outbound,
                            &amount_raw,
                            timestamp,
                            event.tx_hash().map(|tx| tx.to_vec()),
                            TransactionStatus::Completed,
                        )
                            .await
                        {
                            error!("Failed to record outbound transfer: {}", e);
                        }

                        // Then update the stats with USD amounts if we have an asset ID
                        if let Some(ref asset_id) = asset_id {
                            if let Err(e) = update_client_stats_with_usd(
                                dbtx,
                                &client_id,
                                Direction::Outbound,
                                &amount_raw,
                                asset_id,
                                timestamp,
                            )
                                .await
                            {
                                error!("Failed to update USD stats for outbound transfer: {}", e);

                                // Fall back to legacy update if USD conversion fails
                                if let Err(e) = sqlx::query(
                                    r#"
                                    UPDATE ibc_stats
                                    SET
                                        -- Convert to NUMERIC first, then add safely
                                        unshielded_volume =
                                            CASE
                                                WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                                    COALESCE(unshielded_volume, 0) +
                                                    CASE
                                                        WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                        ELSE CAST($2 AS NUMERIC)
                                                    END
                                                ELSE unshielded_volume -- If not valid, don't change
                                            END,
                                        unshielded_tx_count = unshielded_tx_count + 1,
                                        last_updated = $3
                                    WHERE client_id = $1
                                    "#,
                                )
                                    .bind(&client_id)
                                    .bind(&amount_raw)
                                    .bind(timestamp)
                                    .execute(dbtx.as_mut())
                                    .await
                                {
                                    error!("Failed to update legacy stats (fallback) for outbound transfer: {}", e);
                                }
                            }
                        } else {
                            // Use legacy update mechanism if we couldn't extract asset ID
                            if let Err(e) = sqlx::query(
                                r#"
                                UPDATE ibc_stats
                                SET
                                    -- Convert to NUMERIC first, then add safely
                                    unshielded_volume =
                                        CASE
                                            WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's
                                            UPDATE ibc_stats
                                SET
                                    -- Convert to NUMERIC first, then add safely
                                    unshielded_volume =
                                        CASE
                                            WHEN $2 ~ '^[0-9]+$' THEN -- Check if it's a valid number
                                                COALESCE(unshielded_volume, 0) +
                                                CASE
                                                    WHEN LENGTH($2) > 15 THEN 0 -- If too large, use 0
                                                    ELSE CAST($2 AS NUMERIC)
                                                END
                                            ELSE unshielded_volume -- If not valid, don't change
                                        END,
                                    unshielded_tx_count = unshielded_tx_count + 1,
                                    last_updated = $3
                                WHERE client_id = $1
                                "#,
                            )
                                .bind(&client_id)
                                .bind(&amount_raw)
                                .bind(timestamp)
                                .execute(dbtx.as_mut())
                                .await
                            {
                                error!("Error updating legacy stats for outbound transfer: {}", e);
                            }
                        }
                    } else {
                        warn!(
                            "Cannot attribute transfer: no client found for channel {}",
                            channel_id
                        );
                    }
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Update old pending transactions to error status
///
/// # Errors
/// Returns an error if database operations fail
pub async fn update_old_pending_transactions(
    dbtx: &mut PgTransaction<'_>,
) -> Result<(), anyhow::Error> {
    let day_ago = Utc::now() - chrono::Duration::hours(24);

    debug!("Checking for old pending IBC transactions (older than 24h)");

    let pending_count: i64 = match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM explorer_transactions WHERE ibc_status = 'pending' AND ibc_client_id IS NOT NULL"
    )
        .fetch_one(dbtx.as_mut())
        .await {
        Ok(count) => count,
        Err(e) => {
            warn!("Failed to count pending transactions: {}", e);
            0
        }
    };

    let updated_rows = sqlx::query(
        r"
        WITH updated_tx AS (
            UPDATE explorer_transactions tx
            SET ibc_status = $1
            FROM explorer_block_details bd
            WHERE tx.block_height = bd.height
            AND bd.timestamp < $2
            AND tx.ibc_status = 'pending'
            RETURNING tx.ibc_client_id, tx.tx_hash
        )
        SELECT ibc_client_id, tx_hash FROM updated_tx
        WHERE ibc_client_id IS NOT NULL
        ",
    )
        .bind(TransactionStatus::Error.to_string())
        .bind(day_ago)
        .fetch_all(dbtx.as_mut())
        .await?;

    let updated_count = updated_rows.len();
    if updated_count > 0 {
        info!(
            "Updated {} IBC transactions from pending to error status",
            updated_count
        );
    }

    for row in updated_rows {
        let client_id: String = row.get(0);
        let tx_hash: Vec<u8> = row.get(1);

        let result = sqlx::query(
            r"
            UPDATE ibc_stats
            SET
                pending_tx_count = GREATEST(0, pending_tx_count - 1),
                last_updated = $1
            WHERE client_id = $2
            ",
        )
            .bind(Utc::now())
            .bind(&client_id)
            .execute(dbtx.as_mut())
            .await;

        if let Err(e) = result {
            warn!(
                "Failed to update legacy stats for client {}: {}",
                client_id, e
            );
        }

        if let Err(e) = update_transfer_status(dbtx, &tx_hash, TransactionStatus::Error).await {
            warn!("Failed to update transfer status to error: {}", e);
        }
    }

    let remaining_pending: i64 = match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM explorer_transactions WHERE ibc_status = 'pending' AND ibc_client_id IS NOT NULL"
    )
        .fetch_one(dbtx.as_mut())
        .await {
        Ok(count) => count,
        Err(e) => {
            warn!("Failed to count remaining pending transactions: {}", e);
            0
        }
    };

    debug!(
        "IBC transactions: {} were pending, {} updated to error, {} still pending",
        pending_count, updated_count, remaining_pending
    );

    Ok(())
}