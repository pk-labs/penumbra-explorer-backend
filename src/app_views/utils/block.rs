use anyhow::Result;
use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::app_views::utils::transaction;
use crate::parsing::{encode_to_hex, event_to_json};

pub struct Metadata<'a> {
    pub height: u64,
    pub root: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub tx_count: usize,
    pub chain_id: &'a str,
    pub raw_json: Value,
}

/// Process batch events to extract block data
///
/// # Errors
/// Returns an error if there are issues processing the events
#[allow(clippy::needless_lifetimes, clippy::unused_async, clippy::too_many_lines)]
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

            let mut event_json = event_to_json(event, event.tx_hash())?;
            
            if let Some(attrs) = event_json.get_mut("attributes").and_then(|a| a.as_array_mut()) {
                for attr in attrs {
                    let key = attr.get("key").and_then(|k| k.as_str()).unwrap_or("").to_string();
                    if let Some(value) = attr.get("value").and_then(|v| v.as_str()) {
                        if (key == "identityKey" || key == "anchor" || key == "root") && value.contains("inner") {
                            let clean_value = value.replace("\\\"", "\"")
                                .replace("\\\\", "\\")
                                .replace("\\n", "\n");
                            
                            if let Ok(json_value) = serde_json::from_str::<Value>(&clean_value) {
                                attr["value"] = json_value;
                                continue;
                            }
                        }
                        
                        if value.trim().starts_with('{') && value.trim().ends_with('}') {
                            if let Ok(json_value) = serde_json::from_str::<Value>(value) {
                                attr["value"] = json_value;
                            }
                        }
                    }
                }
            }

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
        
        for event in &block_events {
            let _event_type = event.get("type").and_then(|t| t.as_str()).unwrap_or("");
            
            let mut event_copy = event.clone();
            if let Some(attrs) = event_copy.get_mut("attributes").and_then(|a| a.as_array_mut()) {
                for attr in attrs {
                    let key = attr.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    if let Some(val_str) = attr.get("value").and_then(|v| v.as_str()) {
                        if (key == "identityKey" || key == "anchor" || key == "root") && val_str.contains("inner") {
                            if let Ok(json_value) = serde_json::from_str::<Value>(val_str) {
                                attr["value"] = json_value;
                            }
                        }
                    }
                }
            }
            
            all_events.push(event_copy);
        }
        
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
) -> Value {
    let processed_events = events.iter().map(|event| {
        let event_type = event["type"].as_str().unwrap_or("unknown").to_string();
        
        let attributes = if let Some(attrs) = event["attributes"].as_array() {
            attrs.iter().map(|attr| {
                let key = attr["key"].as_str().unwrap_or("").to_string();
                let value = attr["value"].clone();
                
                if let Some(val_str) = value.as_str() {
                    if val_str.trim().starts_with('{') && val_str.trim().ends_with('}') {
                        if let Ok(json_value) = serde_json::from_str::<Value>(val_str) {
                            return json!({
                                "key": key,
                                "value": json_value
                            });
                        }
                    }
                    
                    let mut clean_val = val_str
                        .replace("\\\"", "\"")
                        .replace("\\\\", "\\")
                        .replace("\\n", "\n");
                        
                    if clean_val.starts_with('"') && clean_val.ends_with('\\') {
                        clean_val = clean_val
                            .trim_start_matches('"')
                            .trim_end_matches('\\')
                            .to_string();
                    }
                        
                    if clean_val.trim().starts_with('{') && clean_val.trim().ends_with('}') {
                        if let Ok(json_value) = serde_json::from_str::<Value>(&clean_val) {
                            return json!({
                                "key": key,
                                "value": json_value
                            });
                        }
                    }
                    
                    return json!({
                        "key": key,
                        "value": clean_val
                    });
                }
                
                json!({
                    "key": key,
                    "value": value
                })
            }).collect::<Vec<Value>>()
        } else {
            Vec::new()
        };
        
        json!({
            "type": event_type,
            "attributes": attributes
        })
    }).collect::<Vec<Value>>();
    
    json!({
        "height": height,
        "chain_id": chain_id,
        "timestamp": timestamp.to_rfc3339(),
        "transactions": transactions,
        "events": processed_events
    })
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
#[allow(clippy::too_many_lines)]
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
                            
                            if key.trim().is_empty() {
                                continue;
                            }
                            
                            let attr_value = attr.get("value");
                            
                            if let Some(value) = attr_value {
                                if value.is_object() {
                                    if key == "gasUsed" {
                                        if let Some(block_space) = value.get("blockSpace") {
                                            let mut full_gas_used = serde_json::json!({
                                                "blockSpace": block_space,
                                            });
                                            
                                            if let Some(compact) = value.get("compactBlockSpace") {
                                                full_gas_used["compactBlockSpace"] = compact.clone();
                                            }
                                            if let Some(exec) = value.get("execution") {
                                                full_gas_used["execution"] = exec.clone();
                                            }
                                            if let Some(verify) = value.get("verification") {
                                                full_gas_used["verification"] = verify.clone();
                                            }
                                            
                                            attributes.push(json!({
                                                "key": key,
                                                "value": full_gas_used
                                            }));
                                            continue;
                                        }
                                    }
                                    
                                    attributes.push(json!({
                                        "key": key,
                                        "value": value
                                    }));
                                    continue;
                                }
                                
                                if let Some(value_str) = value.as_str() {
                                    if value_str.contains("{\"amount\":{}}") {
                                        continue;
                                    }
                                    
                                    if key == "position" && value_str.contains("closeOnFill") && value_str.starts_with('{') && !value_str.trim().ends_with('}') {
                                        continue;
                                    }
                                    
                                    if key == "tradingPair" && value_str.contains("asset1") && value_str.starts_with('{') && !value_str.trim().ends_with('}') {
                                        continue;
                                    }
                                    
                                    if (value_str.starts_with('"') && value_str.ends_with('"')) &&
                                       value_str.contains('{') && value_str.contains('}') {
                                        let unquoted = value_str.trim_start_matches('"').trim_end_matches('"');
                                        if let Ok(parsed_json) = serde_json::from_str::<Value>(unquoted) {
                                            attributes.push(json!({
                                                "key": key,
                                                "value": parsed_json
                                            }));
                                            continue;
                                        }
                                    }
                                    
                                    if value_str.trim().starts_with('{') && value_str.trim().ends_with('}') {
                                        if let Ok(parsed_json) = serde_json::from_str::<Value>(value_str) {
                                            attributes.push(json!({
                                                "key": key,
                                                "value": parsed_json
                                            }));
                                            continue;
                                        }
                                    }
                                    
                                    let clean_str = if value_str.starts_with('"') && value_str.ends_with('"') {
                                        value_str.trim_start_matches('"').trim_end_matches('"')
                                    } else {
                                        value_str
                                    };
                                    
                                    attributes.push(json!({
                                        "key": key,
                                        "value": clean_str
                                    }));
                                    continue;
                                }
                                
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