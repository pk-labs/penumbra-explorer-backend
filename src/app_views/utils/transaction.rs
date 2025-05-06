use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::time::Instant;

use crate::parsing::{encode_to_hex, parse_attribute_string};

pub struct Metadata<'a> {
    pub tx_hash: [u8; 32],
    pub height: u64,
    pub timestamp: DateTime<Utc>,
    pub fee_amount: u64,
    pub chain_id: &'a str,
    pub tx_bytes_base64: String,
    pub decoded_tx_json: String,
}

/// Insert transaction into database
///
/// # Errors
/// Returns an error if the database query fails
pub async fn insert(dbtx: &mut PgTransaction<'_>, meta: Metadata<'_>) -> Result<(), sqlx::Error> {
    let Ok(height_i64) = i64::try_from(meta.height) else {
        return Err(sqlx::Error::Decode(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Height value too large: {}", meta.height),
        ))));
    };

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

/// Create transaction JSON
#[must_use]
pub fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<Utc>,
    tx_index: u64,
    tx_events: &[ContextualizedEvent<'_>],
) -> String {
    let mut processed_events = Vec::with_capacity(tx_events.len() + 1);

    processed_events.push(json!({
        "type": "tx",
        "attributes": [
            {"key": "hash", "value": encode_to_hex(tx_hash)},
            {"key": "height", "value": height.to_string()}
        ]
    }));

    for event in tx_events {
        let attr_capacity = event.event.attributes.len();
        let mut attributes = Vec::with_capacity(attr_capacity);

        for attr in &event.event.attributes {
            let attr_str = format!("{attr:?}");

            if let Some((key, value)) = parse_attribute_string(&attr_str) {
                if value.contains("{\"amount\":{}}") || value.trim().is_empty() {
                    continue;
                }

                attributes.push(json!({
                    "key": key,
                    "value": value
                }));
            } else {
                attributes.push(json!({
                    "key": attr_str,
                    "value": "Unknown"
                }));
            }
        }

        if !attributes.is_empty() {
            processed_events.push(json!({
                "type": event.event.kind,
                "attributes": attributes
            }));
        }
    }

    let tx_result_decoded = decode(tx_hash, tx_bytes);
    let tx_hash_hex = encode_to_hex(tx_hash);

    let mut json_str = String::new();

    json_str.push_str("{\n");

    json_str.push_str(&format!("  \"hash\": \"{tx_hash_hex}\",\n"));
    json_str.push_str(&format!("  \"block_height\": \"{height}\",\n"));
    json_str.push_str(&format!("  \"index\": \"{tx_index}\",\n"));
    json_str.push_str(&format!(
        "  \"timestamp\": \"{}\",\n",
        timestamp.to_rfc3339()
    ));

    json_str.push_str("  \"transaction_view\": ");
    let tx_view_json =
        serde_json::to_string_pretty(&tx_result_decoded).unwrap_or_else(|_| "{}".to_string());
    let tx_view_indented = tx_view_json.replace('\n', "\n  ");
    json_str.push_str(&tx_view_indented);
    json_str.push_str(",\n");

    json_str.push_str("  \"events\": ");
    let events_json =
        serde_json::to_string_pretty(&processed_events).unwrap_or_else(|_| "[]".to_string());
    let events_indented = events_json.replace('\n', "\n  ");
    json_str.push_str(&events_indented);
    json_str.push('\n');

    json_str.push('}');

    json_str
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
        .map(std::string::ToString::to_string)
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
