use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::time::Instant;

use crate::parsing::encode_to_hex;

pub struct Metadata<'a> {
    pub tx_hash: [u8; 32],
    pub height: u64,
    pub timestamp: DateTime<Utc>,
    pub fee_amount: u64,
    pub chain_id: &'a str,
    pub tx_bytes_base64: String,
    pub decoded_tx_json: Value,
}

/// Extract key-value pair from an attribute string
/// This handles common formats while doing minimal processing
fn extract_attribute_kv(attr_str: &str) -> Option<(String, String)> {
    if attr_str.contains("EventAttribute") {
        if let Some(key_start) = attr_str.find("key: \"") {
            let key_start = key_start + 6;
            if let Some(key_end) = attr_str[key_start..].find('"') {
                let key = attr_str[key_start..key_start + key_end].to_string();

                if let Some(value_start) = attr_str.find("value: \"") {
                    let value_start = value_start + 8;
                    if let Some(value_end) = attr_str[value_start..].rfind('"') {
                        let value = attr_str[value_start..value_start + value_end].to_string();
                        return Some((key, value));
                    }
                }
            }
        }
    } else if let Some(colon_pos) = attr_str.find(':') {
        let key = attr_str[..colon_pos].trim().trim_matches('"').to_string();
        let value = attr_str[colon_pos + 1..]
            .trim()
            .trim_matches('"')
            .to_string();
        return Some((key, value));
    }

    None
}

/// Process a raw value to handle potential JSON objects
fn process_value(raw_value: &str) -> Value {
    if raw_value.contains("\\\"") {
        let unescaped = raw_value.replace("\\\"", "\"").replace("\\\\", "\\");

        if unescaped.trim().starts_with('{') || unescaped.trim().starts_with('[') {
            let balanced = ensure_balanced_json(&unescaped);
            if let Ok(json_value) = serde_json::from_str::<Value>(&balanced) {
                return json_value;
            }
        }

        if unescaped.starts_with('"') && unescaped.ends_with('"') {
            return json!(unescaped.trim_matches('"'));
        }

        json!(unescaped)
    } else if raw_value.trim().starts_with('{') || raw_value.trim().starts_with('[') {
        let balanced = ensure_balanced_json(raw_value);
        match serde_json::from_str::<Value>(&balanced) {
            Ok(json_value) => json_value,
            Err(_) => json!(raw_value),
        }
    } else if raw_value.starts_with('"') && raw_value.ends_with('"') {
        json!(raw_value.trim_matches('"'))
    } else {
        json!(raw_value)
    }
}

/// Ensure JSON strings have balanced braces
fn ensure_balanced_json(json_str: &str) -> String {
    let mut balanced = json_str.to_string();

    // Balance curly braces
    let open_curly = balanced.chars().filter(|&c| c == '{').count();
    let close_curly = balanced.chars().filter(|&c| c == '}').count();
    if open_curly > close_curly {
        for _ in 0..(open_curly - close_curly) {
            balanced.push('}');
        }
    }

    // Balance square brackets
    let open_bracket = balanced.chars().filter(|&c| c == '[').count();
    let close_bracket = balanced.chars().filter(|&c| c == ']').count();
    if open_bracket > close_bracket {
        for _ in 0..(open_bracket - close_bracket) {
            balanced.push(']');
        }
    }

    balanced
}

/// Process event attributes into JSON
fn process_event_attributes(event: &ContextualizedEvent<'_>) -> Vec<Value> {
    event
        .event
        .attributes
        .iter()
        .filter_map(|attr| {
            let attr_str = format!("{attr:?}");

            if let Some((key, raw_value)) = extract_attribute_kv(&attr_str) {
                if raw_value.trim().is_empty() || raw_value == "{\"amount\":{}}" {
                    return None;
                }

                let processed_value = process_value(&raw_value);

                Some(json!({
                    "key": key,
                    "value": processed_value
                }))
            } else {
                None
            }
        })
        .collect()
}

/// Convert an event to JSON with minimal processing
fn simplified_event_to_json(event: &ContextualizedEvent<'_>, _tx_hash: Option<[u8; 32]>) -> Value {
    let event_type = event.event.kind.to_string();
    let attributes = process_event_attributes(event);

    json!({
        "event_id": event.local_rowid,
        "type": event_type,
        "attributes": attributes
    })
}

/// Decode transaction bytes to JSON
#[must_use]
pub fn decode(tx_hash: [u8; 32], tx_bytes: &[u8]) -> Value {
    let start = Instant::now();
    let hash_hex = encode_to_hex(tx_hash);

    match TransactionView::decode(tx_bytes) {
        Ok(tx_view) => {
            tracing::debug!(
                "Decoded tx {} with TransactionView in {:?}",
                hash_hex,
                start.elapsed()
            );
            serde_json::to_value(&tx_view).unwrap_or(json!({}))
        }
        Err(e) => {
            tracing::debug!(
                "Error decoding tx {} with TransactionView: {:?}, trying Transaction",
                hash_hex,
                e
            );

            match Transaction::decode(tx_bytes) {
                Ok(tx) => {
                    tracing::debug!(
                        "Decoded tx {} with Transaction in {:?}",
                        hash_hex,
                        start.elapsed()
                    );
                    serde_json::to_value(&tx).unwrap_or(json!({}))
                }
                Err(e2) => {
                    tracing::warn!(
                        "Failed to decode tx {} with both methods: {:?}",
                        hash_hex,
                        e2
                    );
                    json!({})
                }
            }
        }
    }
}

/// Create transaction JSON with simplified approach
#[must_use]
pub fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<Utc>,
    tx_index: u64,
    tx_events: &[ContextualizedEvent<'_>],
) -> Value {
    let tx_result_decoded = decode(tx_hash, tx_bytes);

    // Process all events consistently - no special handling for tx events
    let processed_events: Vec<Value> = tx_events
        .iter()
        .map(|event| simplified_event_to_json(event, Some(tx_hash)))
        .collect();

    // Construct the final transaction JSON
    json!({
        "hash": encode_to_hex(tx_hash),
        "block_height": height.to_string(),
        "index": tx_index.to_string(),
        "timestamp": timestamp.to_rfc3339(),
        "transaction_view": tx_result_decoded,
        "events": processed_events
    })
}

/// Insert transaction into database
///
/// # Errors
/// Returns an error if the database query fails
pub async fn insert(dbtx: &mut PgTransaction<'_>, meta: Metadata<'_>) -> Result<(), sqlx::Error> {
    let height_i64 = i64::try_from(meta.height).map_err(|_| {
        sqlx::Error::Decode(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Height value too large: {}", meta.height),
        )))
    })?;

    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM explorer_transactions WHERE tx_hash = $1)",
    )
    .bind(meta.tx_hash.as_ref())
    .fetch_one(dbtx.as_mut())
    .await?;

    if exists {
        sqlx::query(
            r"
            UPDATE explorer_transactions
            SET
                block_height = $2,
                timestamp = $3,
                fee_amount = $4,
                chain_id = $5,
                raw_data = $6,
                raw_json = $7
            WHERE tx_hash = $1
            ",
        )
        .bind(meta.tx_hash.as_ref())
        .bind(height_i64)
        .bind(meta.timestamp)
        .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(&meta.tx_bytes_base64)
        .bind(&meta.decoded_tx_json)
        .execute(dbtx.as_mut())
        .await?;
    } else {
        sqlx::query(
            r"
            INSERT INTO explorer_transactions
            (tx_hash, block_height, timestamp, fee_amount, chain_id, raw_data, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
        )
        .bind(meta.tx_hash.as_ref())
        .bind(height_i64)
        .bind(meta.timestamp)
        .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(&meta.tx_bytes_base64)
        .bind(&meta.decoded_tx_json)
        .execute(dbtx.as_mut())
        .await?;
    }

    Ok(())
}

/// Extract fee amount from transaction result
#[must_use]
pub fn extract_fee_amount(tx_result: &Value) -> u64 {
    tx_result
        .get("body")
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("fee"))
        .and_then(|fee| fee.get("amount"))
        .and_then(|amount| amount.get("lo"))
        .and_then(|lo| lo.as_str())
        .and_then(|lo_str| lo_str.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Extract chain ID from transaction result
pub fn extract_chain_id(tx_result: &Value) -> Option<String> {
    tx_result
        .get("body")
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("chainId"))
        .and_then(|chain_id| chain_id.as_str())
        .map(String::from)
}

/// Extract chain ID from transaction bytes
#[must_use]
pub fn extract_chain_id_from_bytes(tx_bytes: &[u8]) -> Option<String> {
    match TransactionView::decode(tx_bytes) {
        Ok(tx_view) => {
            if let Some(body) = &tx_view.body_view {
                if let Some(params) = &body.transaction_parameters {
                    return Some(params.chain_id.clone());
                }
            }
        }
        Err(_) => {
            if let Ok(tx) = Transaction::decode(tx_bytes) {
                if let Some(body) = &tx.body {
                    if let Some(params) = &body.transaction_parameters {
                        return Some(params.chain_id.clone());
                    }
                }
            }
        }
    }

    None
}
