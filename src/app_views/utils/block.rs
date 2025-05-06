use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::app_views::utils::transaction;
use crate::parsing::{encode_to_hex, event_to_json, parse_attribute_string};

pub struct Metadata<'a> {
    pub height: u64,
    pub root: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub tx_count: usize,
    pub chain_id: &'a str,
    pub raw_json: String,
}

/// Process batch events to extract block data
///
/// # Errors
/// Returns an error if there are issues processing the events
#[allow(clippy::needless_lifetimes, clippy::unused_async)]
pub async fn process_block_events<'a>(
    batch: &'a cometindex::index::EventBatch,
) -> Result<
    Vec<(
        u64,
        Vec<u8>,
        DateTime<Utc>,
        usize,
        Option<String>,
        Value,
        Vec<([u8; 32], Vec<u8>, u64, Vec<ContextualizedEvent<'static>>)>,
    )>,
    anyhow::Error,
> {
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

            let event_json = event_to_json(event, event.tx_hash())?;

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

        let mut all_events = Vec::new();
        all_events.extend(block_events);
        all_events.extend(tx_events);

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
) -> String {
    let mut json_str = String::new();

    json_str.push_str("{\n");

    json_str.push_str(&format!("  \"height\": {height},\n"));
    json_str.push_str(&format!("  \"chain_id\": \"{chain_id}\",\n"));
    json_str.push_str(&format!(
        "  \"timestamp\": \"{}\",\n",
        timestamp.to_rfc3339()
    ));

    json_str.push_str("  \"transactions\": ");
    let tx_json = serde_json::to_string_pretty(transactions).unwrap_or_else(|_| "[]".to_string());
    let tx_json_indented = tx_json.replace('\n', "\n  ");
    json_str.push_str(&tx_json_indented);
    json_str.push_str(",\n");

    json_str.push_str("  \"events\": ");
    let events_json = serde_json::to_string_pretty(events).unwrap_or_else(|_| "[]".to_string());
    let events_json_indented = events_json.replace('\n', "\n  ");
    json_str.push_str(&events_json_indented);
    json_str.push('\n');

    json_str.push('}');

    json_str
}

/// Insert block into database
///
/// # Errors
/// Returns an error if the database query fails
pub async fn insert(dbtx: &mut PgTransaction<'_>, meta: Metadata<'_>) -> Result<(), anyhow::Error> {
    let height_i64 = match i64::try_from(meta.height) {
        Ok(h) => h,
        Err(e) => return Err(anyhow::anyhow!("Height conversion error: {}", e)),
    };

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

/// Collect transactions from block JSON
#[must_use]
pub fn collect_block_transactions(raw_json: &Value, timestamp: DateTime<Utc>) -> Vec<Value> {
    if let Some(block) = raw_json.get("block") {
        if let Some(txs) = block.get("transactions") {
            if let Some(txs_array) = txs.as_array() {
                return txs_array
                    .iter()
                    .map(|tx| {
                        json!({
                            "index": tx.get("index").and_then(Value::as_u64).unwrap_or(0),
                            "hash": tx.get("tx_hash").and_then(|v| v.as_str()).unwrap_or(""),
                            "timestamp": timestamp.to_rfc3339()
                        })
                    })
                    .collect();
            }
        }
    }
    Vec::new()
}

/// Collect events from block JSON
#[must_use]
pub fn collect_block_events(raw_json: &Value) -> Vec<Value> {
    if let Some(block) = raw_json.get("block") {
        if let Some(events) = block.get("events") {
            if let Some(events_array) = events.as_array() {
                let mut result = Vec::new();

                for event in events_array {
                    let event_type = event
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("unknown");

                    let mut attributes = Vec::new();

                    if let Some(attrs) = event.get("attributes").and_then(|a| a.as_array()) {
                        for attr in attrs {
                            let key = attr.get("key").and_then(|k| k.as_str()).unwrap_or("");
                            let value = attr
                                .get("value")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown");

                            if value.contains("{\"amount\":{}}") || key.trim().is_empty() {
                                continue;
                            }

                            if let Some((parsed_key, parsed_value)) = parse_attribute_string(key) {
                                if parsed_value.contains("{\"amount\":{}}")
                                    || parsed_value.trim().is_empty()
                                {
                                    continue;
                                }

                                attributes.push(json!({
                                    "key": parsed_key,
                                    "value": parsed_value
                                }));
                            } else {
                                attributes.push(json!({
                                    "key": key,
                                    "value": value
                                }));
                            }
                        }
                    }

                    if !attributes.is_empty() {
                        result.push(json!({
                            "type": event_type,
                            "attributes": attributes
                        }));
                    }
                }

                return result;
            }
        }
    }
    Vec::new()
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
