use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::app_views::utils::transaction;
use crate::parsing::encode_to_hex;

/// Metadata for a block to be inserted into the database
pub struct Metadata<'a> {
    pub height: u64,
    pub root: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub tx_count: usize,
    pub chain_id: &'a str,
    pub raw_json: Value,
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
    }
    // Handle simple key-value format
    else if let Some(colon_pos) = attr_str.find(':') {
        let key = attr_str[..colon_pos].trim().trim_matches('"').to_string();
        let value = attr_str[colon_pos + 1..].trim().trim_matches('"').to_string();
        return Some((key, value));
    }

    None
}

fn process_event_attributes(event: &ContextualizedEvent<'_>) -> Vec<Value> {
    event.event.attributes.iter()
        .filter_map(|attr| {
            let attr_str = format!("{attr:?}");

            if let Some((key, raw_value)) = extract_attribute_kv(&attr_str) {
                // Skip empty values or certain known empty patterns
                if raw_value.trim().is_empty() {
                    return None;
                }

                // The actual value processing - preserve structure but ensure valid JSON
                let processed_value = if raw_value.contains("\\\"") {
                    // Handle double-escaped JSON strings
                    let unescaped = raw_value.replace("\\\"", "\"").replace("\\\\", "\\");

                    if unescaped.trim().starts_with('{') && unescaped.trim().ends_with('}') {
                        // Try to parse as JSON object
                        serde_json::from_str(&unescaped).unwrap_or(json!(unescaped))
                    } else if unescaped.trim().starts_with('[') && unescaped.trim().ends_with(']') {
                        // Try to parse as JSON array
                        serde_json::from_str(&unescaped).unwrap_or(json!(unescaped))
                    } else if unescaped.starts_with('"') && unescaped.ends_with('"') {
                        // Handle quoted strings - try to extract inner value
                        let inner = unescaped.trim_matches('"');

                        // Check if inner content might be JSON
                        if inner.trim().starts_with('{') || inner.trim().starts_with('[') {
                            serde_json::from_str(inner).unwrap_or(json!(inner))
                        } else {
                            json!(inner)
                        }
                    } else {
                        // Any other format
                        json!(unescaped)
                    }
                } else if raw_value.trim().starts_with('{') || raw_value.trim().starts_with('[') {
                    // Direct JSON objects or arrays
                    serde_json::from_str(&raw_value).unwrap_or(json!(raw_value))
                } else if raw_value.starts_with('"') && raw_value.ends_with('"') {
                    // Try to extract inner content from quoted strings
                    let inner = raw_value.trim_matches('"');

                    // See if the inner content might be JSON
                    if inner.trim().starts_with('{') || inner.trim().starts_with('[') {
                        serde_json::from_str(inner).unwrap_or(json!(inner))
                    } else {
                        json!(inner)
                    }
                } else {
                    // Keep as is
                    json!(raw_value)
                };

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
fn simplified_event_to_json(
    event: &ContextualizedEvent<'_>,
    _tx_hash: Option<[u8; 32]>,  // Added underscore to silence the warning
) -> Value {
    let event_type = event.event.kind.to_string();
    let attributes = process_event_attributes(event);

    json!({
        "event_id": event.local_rowid,
        "type": event_type,
        "attributes": attributes
    })
}

/// Process batch events to extract block data
///
/// # Errors
/// Returns an error if there are issues processing the events
#[allow(clippy::needless_lifetimes)]
pub async fn process_block_events<'a>(
    batch: &'a cometindex::index::EventBatch,
) -> Result<Vec<(
    u64,
    Vec<u8>,
    DateTime<Utc>,
    usize,
    Option<String>,
    Value,
    Vec<([u8; 32], Vec<u8>, u64, Vec<ContextualizedEvent<'static>>)>,
)>, anyhow::Error> {
    let mut results = Vec::new();

    for block_data in batch.events_by_block() {
        let height = block_data.height();
        let tx_count = block_data.transactions().count();

        tracing::info!(
            "Processing block height {} with {} transactions",
            height,
            tx_count
        );

        let mut block_root = None;
        let mut timestamp = None;
        let mut chain_id: Option<String> = None;
        let mut block_events = Vec::new();
        let mut tx_events = Vec::new();

        let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent>> = HashMap::new();

        for event in block_data.events() {
            if let Ok(pe) = pb::EventBlockRoot::from_event(event.event) {
                let timestamp_proto = pe.timestamp.unwrap_or_default();
                timestamp = DateTime::from_timestamp(
                    timestamp_proto.seconds,
                    u32::try_from(timestamp_proto.nanos)?,
                );
                block_root = pe.root.map(|r| r.inner);
            }

            let event_json = simplified_event_to_json(&event, event.tx_hash());

            if let Some(tx_hash) = event.tx_hash() {
                let owned_event = clone_event(event);
                events_by_tx_hash
                    .entry(tx_hash)
                    .or_default()
                    .push(owned_event);
                tx_events.push(event_json);
            } else {
                block_events.push(event_json);
            }
        }

        if tx_count > 0 {
            if let Some((_, tx_bytes)) = block_data.transactions().next() {
                chain_id = transaction::extract_chain_id_from_bytes(tx_bytes);
            }
        }

        let transactions: Vec<Value> = block_data
            .transactions()
            .enumerate()
            .map(|(index, (tx_hash, _))| {
                json!({
                    "block_id": height,
                    "index": index,
                    "created_at": timestamp,
                    "tx_hash": encode_to_hex(tx_hash)
                })
            })
            .collect();

        let all_events = [block_events, tx_events].concat();

        let raw_json = json!({
            "block": {
                "height": height,
                "chain_id": chain_id.as_deref().unwrap_or("unknown"),
                "created_at": timestamp,
                "transactions": transactions,
                "events": all_events
            }
        });

        if let (Some(root), Some(ts)) = (block_root, timestamp) {
            let mut block_txs = Vec::new();

            for (tx_index, (tx_hash, tx_bytes)) in block_data.transactions().enumerate() {
                let tx_bytes_vec = tx_bytes.to_vec();
                let tx_events = events_by_tx_hash.get(&tx_hash).cloned().unwrap_or_default();

                block_txs.push((tx_hash, tx_bytes_vec, tx_index as u64, tx_events));
            }

            results.push((height, root, ts, tx_count, chain_id, raw_json, block_txs));
        }
    }

    Ok(results)
}

/// Create block JSON from block data
#[must_use]
pub fn create_block_json(
    height: u64,
    chain_id: &str,
    timestamp: DateTime<Utc>,
    transactions: &[Value],
    events: &[Value],
) -> Value {
    json!({
        "height": height,
        "chain_id": chain_id,
        "timestamp": timestamp.to_rfc3339(),
        "transactions": transactions,
        "events": events
    })
}

/// Insert block into database
///
/// # Errors
/// Returns an error if the database query fails
pub async fn insert(dbtx: &mut PgTransaction<'_>, meta: Metadata<'_>) -> Result<(), anyhow::Error> {
    let height_i64 = i64::try_from(meta.height)
        .map_err(|e| anyhow::anyhow!("Height conversion error: {}", e))?;

    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM explorer_block_details WHERE height = $1)",
    )
        .bind(height_i64)
        .fetch_one(dbtx.as_mut())
        .await?;

    let validator_key = None::<String>;
    let previous_hash = None::<Vec<u8>>;
    let block_hash = None::<Vec<u8>>;

    if exists {
        sqlx::query(
            r"
            UPDATE explorer_block_details
            SET
                root = $2,
                timestamp = $3,
                num_transactions = $4,
                chain_id = $5,
                raw_json = $6
            WHERE height = $1
            ",
        )
            .bind(height_i64)
            .bind(&meta.root)
            .bind(meta.timestamp)
            .bind(i32::try_from(meta.tx_count).unwrap_or(0))
            .bind(meta.chain_id)
            .bind(&meta.raw_json)
            .execute(dbtx.as_mut())
            .await?;

        tracing::debug!("Updated block {}", meta.height);
    } else {
        sqlx::query(
            r"
            INSERT INTO explorer_block_details
            (height, root, timestamp, num_transactions, chain_id,
             validator_identity_key, previous_block_hash, block_hash, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ",
        )
            .bind(height_i64)
            .bind(&meta.root)
            .bind(meta.timestamp)
            .bind(i32::try_from(meta.tx_count).unwrap_or(0))
            .bind(meta.chain_id)
            .bind(validator_key)
            .bind(previous_hash)
            .bind(block_hash)
            .bind(&meta.raw_json)
            .execute(dbtx.as_mut())
            .await?;

        tracing::debug!("Inserted block {}", meta.height);
    }

    Ok(())
}

/// Extract transactions from block JSON
#[must_use]
pub fn collect_block_transactions(raw_json: &Value, timestamp: DateTime<Utc>) -> Vec<Value> {
    raw_json
        .get("block")
        .and_then(|block| block.get("transactions"))
        .and_then(|txs| txs.as_array())
        .map(|txs_array| {
            txs_array
                .iter()
                .map(|tx| {
                    json!({
                        "index": tx.get("index").and_then(Value::as_u64).unwrap_or(0),
                        "hash": tx.get("tx_hash").and_then(|v| v.as_str()).unwrap_or(""),
                        "timestamp": timestamp.to_rfc3339()
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract events from block JSON
#[must_use]
/// Extract events from block JSON
#[must_use]
pub fn collect_block_events(raw_json: &Value) -> Vec<Value> {
    raw_json
        .get("block")
        .and_then(|block| block.get("events"))
        .and_then(|events| events.as_array())
        .map(|events_array| {
            events_array
                .iter()
                .filter_map(|event| {
                    // Get the event_id field
                    let event_id = event.get("event_id").cloned().unwrap_or(json!(null));

                    let event_type = event
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("unknown");

                    let attributes = event
                        .get("attributes")
                        .and_then(|attrs| attrs.as_array())
                        .map(|attrs| {
                            attrs
                                .iter()
                                .filter_map(|attr| {
                                    let key = attr.get("key").and_then(|k| k.as_str()).unwrap_or("");

                                    if key.trim().is_empty() {
                                        return None;
                                    }

                                    let value = attr.get("value").cloned().unwrap_or(json!(null));

                                    Some(json!({
                                        "key": key,
                                        "value": value
                                    }))
                                })
                                .collect::<Vec<Value>>()
                        })
                        .unwrap_or_default();

                    if attributes.is_empty() {
                        None
                    } else {
                        Some(json!({
                            "event_id": event_id,
                            "type": event_type,
                            "attributes": attributes
                        }))
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}
/// Clone a contextualized event to make it have a static lifetime
#[must_use]
pub fn clone_event(event: ContextualizedEvent<'_>) -> ContextualizedEvent<'static> {
    let event_clone = event.event.clone();

    let tx_clone = event.tx.map(|(hash, bytes)| (hash, bytes.to_vec()));

    ContextualizedEvent {
        block_height: event.block_height,
        event: &*Box::leak(Box::new(event_clone)),
        tx: tx_clone.map(|(hash, bytes)| {
            let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
            (hash, static_bytes)
        }),
        local_rowid: event.local_rowid,
    }
}