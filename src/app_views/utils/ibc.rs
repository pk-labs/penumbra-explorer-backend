use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use regex::Regex;
use serde_json::Value;
use sqlx::{
    types::chrono::{DateTime, Utc},
    Row,
};
use std::collections::HashMap;
use std::cmp::min;
use tracing::{debug, error, info, warn};
use base64::Engine;

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

// Standard token decimal places - most tokens use 6 decimals like USDC
const DEFAULT_TOKEN_DECIMALS: u32 = 6;

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

/// Update asset price from candlestick data or other sources
///
/// # Errors
/// Update asset price from candlestick data or other sources
///
/// # Errors
/// Returns an error if database operations fail
async fn update_asset_price(
    dbtx: &mut PgTransaction<'_>,
    asset_id: &[u8],
    price_usd: f64,
    timestamp: DateTime<Utc>,
    symbol: Option<String>,
) -> Result<(), anyhow::Error> {
    if let Some(symbol_val) = &symbol {
        // Update current price with symbol if available
        sqlx::query(
            r"
            INSERT INTO asset_prices (asset_id, price_usd, last_updated, symbol)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (asset_id)
            DO UPDATE SET
                price_usd = $2,
                last_updated = $3,
                symbol = COALESCE($4, asset_prices.symbol)
            ",
        )
            .bind(asset_id)
            .bind(price_usd)
            .bind(timestamp)
            .bind(symbol_val)
            .execute(dbtx.as_mut())
            .await?;
    } else {
        // Update current price without changing the symbol
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
    }

    info!(
        "🟢 PRICE STORED: Updated price for asset {} (hex: {}): ${:.8} with symbol {:?}",
        String::from_utf8_lossy(asset_id),
        hex::encode(asset_id),
        price_usd,
        symbol.as_ref()  // Use as_ref() to avoid consuming symbol
    );

    Ok(())
}

/// Get the latest price for an asset with improved reliability
/// Returns None if no price is available rather than using fallbacks
///
/// # Errors
/// Returns an error if database operations fail
/// Get the latest price for an asset with improved reliability
/// Returns None if no price is available rather than using fallbacks
///
/// # Errors
/// Returns an error if database operations fail
async fn get_asset_price(
    dbtx: &mut PgTransaction<'_>,
    asset_id: &[u8],
) -> Result<Option<f64>, anyhow::Error> {
    let now = Utc::now();

    // Try to extract a readable symbol from the asset ID if possible
    let mut asset_symbol = None;
    if let Ok(s) = std::str::from_utf8(asset_id) {
        if s.chars().all(|c| c.is_ascii_alphabetic()) {
            asset_symbol = Some(s.to_uppercase());
        }
    }

    // Special case handling for stablecoins only
    // USDC: fixed at 1.0
    if asset_id == USDC_ASSET_ID {
        // Make sure it's in the database for future lookups
        let _ = update_asset_price(dbtx, asset_id, 1.0, now, Some("USDC".to_string())).await;
        return Ok(Some(1.0));
    }

    // Handle USDT as a stablecoin (1.0)
    if asset_id == b"usdt" {
        // Make sure it's in the database for future lookups
        let _ = update_asset_price(dbtx, asset_id, 1.0, now, Some("USDT".to_string())).await;
        return Ok(Some(1.0));
    }

    // Other stablecoins with "usd" in their name
    let asset_hex = hex::encode(asset_id);
    if asset_hex.contains("usd") || asset_hex.contains("dai") {
        // Determine if this is likely a stablecoin
        let is_likely_stablecoin = asset_hex.contains("usd") &&
            (asset_hex.contains("usdc") ||
                asset_hex.contains("usdt") ||
                asset_hex.contains("busd") ||
                asset_hex.contains("tusd") ||
                asset_hex.contains("dai"));

        if is_likely_stablecoin {
            info!("Identified stablecoin: {}", asset_hex);
            // Try to derive a symbol
            let symbol = if asset_symbol.is_some() {
                asset_symbol.clone()
            } else if asset_hex.contains("dai") {
                Some("DAI".to_string())
            } else {
                Some(format!("USD_{}", &asset_hex[0..min(8, asset_hex.len())]))
            };

            // Save this to the database for future lookups
            let _ = update_asset_price(dbtx, asset_id, 1.0, now, symbol).await;
            return Ok(Some(1.0));
        }
    }

    // Query the latest price from the database
    let price: Option<f64> = sqlx::query_scalar(
        "SELECT price_usd FROM asset_prices WHERE asset_id = $1"
    )
        .bind(asset_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

    // If we have a price in the database, use it
    if let Some(price) = price {
        info!("Found existing price for asset {}: ${:.8}",
               hex::encode(asset_id), price);
        return Ok(Some(price));
    }

    // Check the first price entry for this asset to see if we should backfill
    let first_price: Option<(f64, DateTime<Utc>)> = sqlx::query_as(
        "SELECT price_usd, last_updated FROM asset_prices
         WHERE asset_id = $1
         ORDER BY last_updated ASC
         LIMIT 1"
    )
        .bind(asset_id)
        .fetch_optional(dbtx.as_mut())
        .await?;

    // If we have historical data for this asset, use the earliest price
    // This retroactively applies the first known price to earlier transactions
    if let Some((first_price, _)) = first_price {
        info!("Using earliest known price for asset {}: ${:.8}",
              hex::encode(asset_id), first_price);
        return Ok(Some(first_price));
    }

    // If we get here, we have no price information for this asset
    info!("No price data available for asset {}", hex::encode(asset_id));
    Ok(None)
}

/// Update client statistics with USD-denominated volume
/// Only updates if a valid price is available
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
    if amount_value <= 0 {
        info!("Skipping USD stats update for invalid amount: {}", amount);
        return Ok(());
    }

    // Get USD price and calculate USD amount
    match get_asset_price(dbtx, asset_id).await? {
        Some(price) if price > 0.0 => {
            // Tokens typically use 6 decimal places (like USDC), so divide by 1,000,000
            // This converts raw amounts to actual token amounts before multiplying by price
            let decimal_adjusted_amount = amount_value as f64 / 1_000_000.0;
            let usd_amount = decimal_adjusted_amount * price;

            // Use separate queries based on direction instead of dynamic SQL
            match direction {
                Direction::Inbound => {
                    info!("Updating USD stats for inbound transfer: client={}, amount=${:.2} (raw amount={}, price=${:.4})",
                         client_id, usd_amount, amount_value, price);

                    sqlx::query(
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
                        .await?;

                    info!(
                        "✅ Updated USD stats for inbound transfer: client={}, amount=${:.2}",
                        client_id, usd_amount
                    );
                }
                Direction::Outbound => {
                    info!("Updating USD stats for outbound transfer: client={}, amount=${:.2} (raw amount={}, price=${:.4})",
                         client_id, usd_amount, amount_value, price);

                    sqlx::query(
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
                        .await?;

                    info!(
                        "✅ Updated USD stats for outbound transfer: client={}, amount=${:.2}",
                        client_id, usd_amount
                    );
                }
            }

            Ok(())
        },
        _ => {
            // We don't have a valid price, just update the transaction count without the amount
            match direction {
                Direction::Inbound => {
                    info!("No valid price for asset {}. Updating only tx count for inbound transfer",
                          hex::encode(asset_id));

                    sqlx::query(
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
                        .await?;
                },
                Direction::Outbound => {
                    info!("No valid price for asset {}. Updating only tx count for outbound transfer",
                          hex::encode(asset_id));

                    sqlx::query(
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
                        .await?;
                }
            }

            Ok(())
        }
    }
}

/// Record an individual IBC transfer in the time series table
/// Only calculates USD amounts if valid price data is available
///
/// # Errors
/// Returns an error if database operations fail
pub async fn record_transfer(
    dbtx: &mut PgTransaction<'_>,
    client_id: &str,
    channel_id: &str,
    direction: Direction,
    amount: &str,
    timestamp: DateTime<Utc>,
    tx_hash: Option<Vec<u8>>,
    status: TransactionStatus,
    asset_id: Option<Vec<u8>>,
) -> Result<(), anyhow::Error> {
    // Parse the amount as numeric
    let amount_value = amount.parse::<i64>().unwrap_or_default();
    let tx_status = status.to_string();

    // Calculate USD amount if we have an asset ID
    let usd_amount: Option<f64> = if let Some(asset) = &asset_id {
        match get_asset_price(dbtx, asset).await? {
            Some(price) if price > 0.0 => {
                // Tokens typically use 6 decimal places (like USDC), so divide by 1,000,000
                // This converts raw amounts to actual token amounts before multiplying by price
                let decimal_adjusted_amount = amount_value as f64 / 1_000_000.0;
                let amount_usd = decimal_adjusted_amount * price;
                info!(
                    "Calculated USD amount for transfer: ${:.2} (raw_amount={}, adjusted_amount={:.8}, price=${:.8})",
                    amount_usd, amount_value, decimal_adjusted_amount, price
                );
                Some(amount_usd)
            },
            _ => {
                info!("No valid price for asset {}, not calculating USD amount", hex::encode(asset));
                None
            }
        }
    } else {
        None
    };

    sqlx::query(
        r"
        INSERT INTO ibc_transfers (
            client_id,
            channel_id,
            direction,
            amount,
            asset_id,
            usd_amount,
            timestamp,
            tx_hash,
            status
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ",
    )
        .bind(client_id)
        .bind(channel_id)
        .bind(direction.to_string())
        .bind(amount_value)
        .bind(asset_id)
        .bind(usd_amount)
        .bind(timestamp)
        .bind(tx_hash)
        .bind(&tx_status)
        .execute(dbtx.as_mut())
        .await?;

    debug!(
        "Recorded {} IBC transfer: client={}, channel={}, amount={}, usd_amount=${:?}, status={}",
        direction, client_id, channel_id, amount_value, usd_amount, tx_status
    );

    Ok(())
}

/// Process candlestick data to update asset prices
async fn process_candlestick_data(
    dbtx: &mut PgTransaction<'_>,
    event: &ContextualizedEvent<'_>,
    timestamp: DateTime<Utc>,
) -> Result<(), anyhow::Error> {
    // Log all event kinds when debugging
    info!("Event kind: {} at height {}", event.event.kind.as_str(), event.block_height);

    if event.event.kind.as_str() != "penumbra.core.component.dex.v1.EventCandlestickData" {
        return Ok(());
    }

    info!("Processing candlestick data from event at height {}", event.block_height);

    // Extract candlestick data from event attributes
    let mut base_asset_id: Option<Vec<u8>> = None;
    let mut quote_asset_id: Option<Vec<u8>> = None;
    let mut close_price: Option<f64> = None;
    let mut base_symbol: Option<String> = None;
    let mut quote_symbol: Option<String> = None;

    // Store parsed data elements
    let mut pair_data: Option<Value> = None;

    // Log all attributes for debugging and collect data
    for attr in &event.event.attributes {
        if let (Ok(key), Ok(value)) = (attr.key_str(), attr.value_str()) {
            info!("Candlestick attribute: {}={}", key, value);

            // Parse the new format attributes
            if key == "pair" && !value.is_empty() {
                if let Ok(json_data) = serde_json::from_str::<Value>(value) {
                    pair_data = Some(json_data);
                }
            } else if key == "stick" && !value.is_empty() {
                if let Ok(json_data) = serde_json::from_str::<Value>(value) {
                    // Extract the close price
                    if let Some(close) = json_data.get("close") {
                        if let Some(close_val) = close.as_f64() {
                            close_price = Some(close_val);
                            info!("Found close price: {}", close_val);
                        } else if let Some(close_val) = close.as_str() {
                            if let Ok(parsed_price) = close_val.parse::<f64>() {
                                close_price = Some(parsed_price);
                                info!("Found close price (string): {}", parsed_price);
                            }
                        }
                    }
                }
            }
        }
    }

    // Try to extract the base and quote asset IDs from the pair data
    if let Some(pair) = pair_data {
        if let (Some(start), Some(end)) = (pair.get("start"), pair.get("end")) {
            // Asset IDs are encoded in base64 in the "inner" field
            if let Some(start_inner) = start.get("inner").and_then(|v| v.as_str()) {
                // Try to decode the base64 string using the modern method
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(start_inner) {
                    base_asset_id = Some(decoded);
                    info!("Decoded base asset ID: {}", hex::encode(base_asset_id.as_ref().unwrap()));

                    // Try to determine if this is the USDC asset ID
                    if base_asset_id.as_ref().unwrap() == USDC_ASSET_ID {
                        base_symbol = Some("USDC".to_string());
                        info!("Identified base asset as USDC");
                    }
                }
            }

            if let Some(end_inner) = end.get("inner").and_then(|v| v.as_str()) {
                // Try to decode the base64 string using the modern method
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(end_inner) {
                    quote_asset_id = Some(decoded);
                    info!("Decoded quote asset ID: {}", hex::encode(quote_asset_id.as_ref().unwrap()));

                    // Try to determine if this is the USDC asset ID
                    if quote_asset_id.as_ref().unwrap() == USDC_ASSET_ID {
                        quote_symbol = Some("USDC".to_string());
                        info!("Identified quote asset as USDC");
                    }
                }
            }
        }
    }

    // Try legacy attribute parsing if needed
    if base_asset_id.is_none() || quote_asset_id.is_none() || close_price.is_none() {
        info!("Trying legacy attribute parsing method for candlestick data");
        for attr in &event.event.attributes {
            if let (Ok(key), Ok(value)) = (attr.key_str(), attr.value_str()) {
                match key {
                    "pair.base" => {
                        base_asset_id = Some(hex::decode(value).unwrap_or_default());
                        info!("Found base asset ID from legacy attribute: {}", value);

                        // Try to extract symbol from asset ID if possible
                        if let Ok(bytes) = hex::decode(value) {
                            if let Ok(s) = std::str::from_utf8(&bytes) {
                                if s.chars().all(|c| c.is_ascii_alphabetic()) {
                                    base_symbol = Some(s.to_uppercase());
                                    info!("Extracted base symbol from legacy: {}", s.to_uppercase());
                                }
                            }
                        }
                    },
                    "pair.quote" => {
                        quote_asset_id = Some(hex::decode(value).unwrap_or_default());
                        info!("Found quote asset ID from legacy attribute: {}", value);

                        // Try to extract symbol from asset ID if possible
                        if let Ok(bytes) = hex::decode(value) {
                            if let Ok(s) = std::str::from_utf8(&bytes) {
                                if s.chars().all(|c| c.is_ascii_alphabetic()) {
                                    quote_symbol = Some(s.to_uppercase());
                                    info!("Extracted quote symbol from legacy: {}", s.to_uppercase());
                                }
                            }
                        }
                    },
                    "stick.close" => {
                        close_price = value.parse::<f64>().ok();
                        info!("Found close price from legacy attribute: {}", value);
                    },
                    _ => {}
                }
            }
        }
    }

    // Process the candlestick data if we have all components
    if let (Some(base), Some(quote), Some(price)) = (&base_asset_id, &quote_asset_id, &close_price) {
        if *price <= 0.0 {
            info!("Skipping invalid non-positive price: {}", price);
            return Ok(());
        }

        info!(
            "Processing candlestick with base={}, quote={}, price={}",
            hex::encode(base),
            hex::encode(quote),
            price
        );

        // Temporary helper to determine if an asset ID is USDC-like
        let is_usdc = |asset: &[u8]| -> bool {
            asset == USDC_ASSET_ID ||
                // Check for hex encoded "usdc" in the asset ID
                hex::encode(asset).contains("75736463")
        };

        if is_usdc(quote) {
            // This is a direct USDC pair, so we have USD price directly
            info!("💰 USDC DIRECT PAIR: base={} quote=USDC price=${}", hex::encode(base), price);
            update_asset_price(dbtx, base, *price, timestamp, base_symbol).await?;
        } else if is_usdc(base) {
            // This is an inverse USDC pair, calculate USD price as 1/price
            if *price > 0.0 {
                let inverse_price = 1.0 / *price;
                info!("💰 USDC INVERSE PAIR: base=USDC quote={} price=${:.8}",
                      hex::encode(quote), inverse_price);
                update_asset_price(dbtx, quote, inverse_price, timestamp, quote_symbol).await?;
            }
        } else {
            // Even if we don't have a USDC pair, still store the price data
            // This can be useful later for cross-rates
            let asset_id_to_store = base;
            let symbol_to_use = base_symbol.clone();
            let price_to_store = *price;

            info!("📊 NON-USDC PAIR: Storing direct price data for {} at price {}",
                  hex::encode(asset_id_to_store), price_to_store);
            update_asset_price(dbtx, asset_id_to_store, price_to_store, timestamp, symbol_to_use).await?;

            // Also store the inverse relationship
            let inverse_asset_id = quote;
            let inverse_symbol = quote_symbol.clone();
            if *price > 0.0 {
                let inverse_price = 1.0 / *price;
                info!("📊 NON-USDC PAIR: Storing inverse price data for {} at price {:.8}",
                      hex::encode(inverse_asset_id), inverse_price);
                update_asset_price(dbtx, inverse_asset_id, inverse_price, timestamp, inverse_symbol).await?;
            }
        }
    } else {
        info!("Incomplete candlestick data: base={:?}, quote={:?}, price={:?}",
               base_asset_id.as_ref().map(|v| hex::encode(v)),
               quote_asset_id.as_ref().map(|v| hex::encode(v)),
               close_price);
    }

    Ok(())
}

/// Extract asset ID from event metadata and value
fn extract_asset_id(meta: &Value, value: &Value) -> Option<Vec<u8>> {
    // Log the incoming metadata and value for debugging
    info!("Extracting asset ID from meta: {}, value: {}", meta, value);

    // Try to extract assetId.inner (camelCase) from value - this is the most common case in Penumbra
    if let Some(asset_id) = value.get("assetId") {
        if let Some(inner) = asset_id.get("inner") {
            if let Some(inner_str) = inner.as_str() {
                info!("Found assetId.inner: {}", inner_str);
                // Try to decode from base64
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(inner_str) {
                    info!("Successfully decoded assetId.inner from base64: {}", hex::encode(&decoded));
                    return Some(decoded);
                }
            }
        }
    }

    // Try to get from value.asset_id (snake_case)
    if let Some(asset_id) = value.get("asset_id") {
        if let Some(asset_id_str) = asset_id.as_str() {
            info!("Found asset_id directly: {}", asset_id_str);
            return Some(hex::decode(asset_id_str).unwrap_or_default());
        }
    }

    // Try to get from value.value.asset_id
    if let Some(value_inner) = value.get("value") {
        if let Some(asset_id) = value_inner.get("asset_id") {
            if let Some(asset_id_str) = asset_id.as_str() {
                info!("Found asset_id in value.value: {}", asset_id_str);
                return Some(hex::decode(asset_id_str).unwrap_or_default());
            }
        }
    }

    // Try to get from value.asset (for Penumbra specific format)
    if let Some(asset) = value.get("asset") {
        if let Some(asset_inner) = asset.get("inner") {
            if let Some(asset_inner_str) = asset_inner.as_str() {
                info!("Found asset.inner: {}", asset_inner_str);
                // Try to decode from base64
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(asset_inner_str) {
                    info!("Decoded asset.inner as base64: {}", hex::encode(&decoded));
                    return Some(decoded);
                }
                // Try as hex
                if let Ok(decoded) = hex::decode(asset_inner_str) {
                    info!("Decoded asset.inner as hex: {}", hex::encode(&decoded));
                    return Some(decoded);
                }
            }
        }
    }

    // Try to get from meta
    if let Some(asset_id) = meta.get("asset_id") {
        if let Some(asset_id_str) = asset_id.as_str() {
            info!("Found asset_id in meta: {}", asset_id_str);
            return Some(hex::decode(asset_id_str).unwrap_or_default());
        }
    }

    // Try to get from meta.denom
    if let Some(denom) = meta.get("denom") {
        if let Some(denom_str) = denom.as_str() {
            info!("Found denom in meta: {}", denom_str);
            return Some(denom_str.as_bytes().to_vec());
        }
    }

    // Look deeper in meta - sometimes it's nested in metadata
    if let Some(metadata) = meta.get("metadata") {
        if let Some(denom) = metadata.get("denom") {
            if let Some(denom_str) = denom.as_str() {
                info!("Found denom in meta.metadata: {}", denom_str);
                return Some(denom_str.as_bytes().to_vec());
            }
        }

        if let Some(asset_id) = metadata.get("asset_id") {
            if let Some(asset_id_str) = asset_id.as_str() {
                info!("Found asset_id in meta.metadata: {}", asset_id_str);
                return Some(hex::decode(asset_id_str).unwrap_or_default());
            }
        }
    }

    // If we got here, we couldn't find the asset_id
    info!("Could not extract asset_id from metadata and value");
    None
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
    info!(
        "Processing IBC events for block {} with {} events",
        height,
        events.len()
    );



    // First, process candlestick events to update asset prices
    let mut candlestick_count = 0;
    for event in events {
        if event.event.kind.as_str() == "penumbra.core.component.dex.v1.EventCandlestickData" {
            info!("Found candlestick event in block {}", height);
            candlestick_count += 1;
            if let Err(e) = process_candlestick_data(dbtx, event, timestamp).await {
                error!("Failed to process candlestick data: {}", e);
            }
        }
    }

    if candlestick_count > 0 {
        info!("Processed {} candlestick events in block {}", candlestick_count, height);
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
                        None, // No asset_id for pending transfers yet
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
                    let mut asset_id = extract_asset_id(&meta, &value);

                    // Get channel name as a fallback for asset ID
                    let fallback_asset_id = if channel_id.starts_with("channel-") {
                        // Extract the channel number and use it to create a predictable asset ID
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Create a deterministic asset ID based on the channel number
                            let channel_based_asset = format!("ibc_channel_{}", channel_num);
                            info!("Using channel-based fallback asset ID: {}", channel_based_asset);
                            Some(channel_based_asset.as_bytes().to_vec())
                        } else {
                            info!("Could not extract channel number from {}", channel_id);
                            Some(channel_id.as_bytes().to_vec())
                        }
                    } else {
                        Some(channel_id.as_bytes().to_vec())
                    };

                    if asset_id.is_none() {
                        info!("Could not extract asset ID for inbound transfer, using fallback");
                        asset_id = fallback_asset_id;
                    }

                    if let Some(asset_id_ref) = asset_id.as_ref() {
                        info!("Using asset ID for inbound transfer: {}",
                              hex::encode(asset_id_ref));

                        // Try to extract denom or symbol information
                        let mut symbol = None;
                        if let Ok(s) = std::str::from_utf8(asset_id_ref) {
                            if s.chars().all(|c| c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_') {
                                symbol = Some(s.to_uppercase());
                                info!("Extracted asset symbol for inbound transfer: {}", s.to_uppercase());
                            }
                        }

                        // Check if we already have a price for this asset
                        let existing_price = sqlx::query_scalar::<_, Option<f64>>(
                            "SELECT price_usd FROM asset_prices WHERE asset_id = $1"
                        )
                            .bind(asset_id_ref)
                            .fetch_optional(dbtx.as_mut())
                            .await;

                        if existing_price.is_err() || existing_price.as_ref().unwrap().is_none() {
                            // We don't have a price yet, so call get_asset_price which will save it to the DB
                            info!("No existing price for asset {}, getting fallback price", hex::encode(asset_id_ref));
                            if let Err(e) = get_asset_price(dbtx, asset_id_ref).await {
                                error!("Failed to get and store price for asset {}: {}",
                                      hex::encode(asset_id_ref), e);
                            }
                        }
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
                            asset_id.clone(), // Include asset_id for completed transfers
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
                    let mut asset_id = extract_asset_id(&meta, &value);

                    // Get channel name as a fallback for asset ID
                    let fallback_asset_id = if channel_id.starts_with("channel-") {
                        // Extract the channel number and use it to create a predictable asset ID
                        if let Some(channel_num) = extract_number_from_channel(channel_id) {
                            // Create a deterministic asset ID based on the channel number
                            let channel_based_asset = format!("ibc_channel_{}", channel_num);
                            info!("Using channel-based fallback asset ID: {}", channel_based_asset);
                            Some(channel_based_asset.as_bytes().to_vec())
                        } else {
                            info!("Could not extract channel number from {}", channel_id);
                            Some(channel_id.as_bytes().to_vec())
                        }
                    } else {
                        Some(channel_id.as_bytes().to_vec())
                    };

                    if asset_id.is_none() {
                        info!("Could not extract asset ID for outbound transfer, using fallback");
                        asset_id = fallback_asset_id;
                    }

                    if let Some(asset_id_ref) = asset_id.as_ref() {
                        info!("Using asset ID for outbound transfer: {}",
                              hex::encode(asset_id_ref));

                        // Try to extract denom or symbol information
                        let mut symbol = None;
                        if let Ok(s) = std::str::from_utf8(asset_id_ref) {
                            if s.chars().all(|c| c.is_ascii_alphabetic() || c.is_ascii_digit() || c == '_') {
                                symbol = Some(s.to_uppercase());
                                info!("Extracted asset symbol for outbound transfer: {}", s.to_uppercase());
                            }
                        }

                        // Check if we already have a price for this asset
                        let existing_price = sqlx::query_scalar::<_, Option<f64>>(
                            "SELECT price_usd FROM asset_prices WHERE asset_id = $1"
                        )
                            .bind(asset_id_ref)
                            .fetch_optional(dbtx.as_mut())
                            .await;

                        if existing_price.is_err() || existing_price.as_ref().unwrap().is_none() {
                            // We don't have a price yet, so call get_asset_price which will save it to the DB
                            info!("No existing price for asset {}, getting fallback price", hex::encode(asset_id_ref));
                            if let Err(e) = get_asset_price(dbtx, asset_id_ref).await {
                                error!("Failed to get and store price for asset {}: {}",
                                      hex::encode(asset_id_ref), e);
                            }
                        }
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
                            asset_id.clone(), // Include asset_id for completed transfers
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