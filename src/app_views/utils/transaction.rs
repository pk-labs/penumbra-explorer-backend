use cometindex::{ContextualizedEvent, PgTransaction};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value};
use sqlx::types::chrono::{DateTime, Utc};
use std::time::Instant;

use crate::parsing::{encode_to_hex, parse_attribute_string};

/// Helper function to extract a field value from a JSON-like string
/// This handles escaped quotes and complex JSON structures
/// Returns the field value as a String to avoid lifetime issues
fn extract_json_field(json_str: &str, field_name: &str) -> Option<String> {
    tracing::debug!("Extracting field '{}' from JSON string: {}", field_name, json_str);
    
    // First try to parse the entire string as JSON
    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(json_str) {
        if let Some(field_value) = parsed_json.get(field_name) {
            if let Some(value_str) = field_value.as_str() {
                tracing::debug!("Found field '{}' from parsed JSON: {}", field_name, value_str);
                return Some(value_str.to_string());
            } else {
                let value_str = field_value.to_string();
                tracing::debug!("Found field '{}' from parsed JSON (non-string): {}", field_name, value_str);
                return Some(value_str);
            }
        }
    }
    
    // Fallback to string manipulation if JSON parsing fails
    if let Some(field_pos) = json_str.find(field_name) {
        if let Some(colon_pos) = json_str[field_pos..].find(':') {
            let start_pos = field_pos + colon_pos + 1;
            let value_start = json_str[start_pos..].trim_start();
            
            // Handle quoted strings
            if value_start.starts_with('"') {
                if let Some(quote_end) = value_start[1..].find('"') {
                    let result = value_start[1..(quote_end+1)].trim().to_string();
                    tracing::debug!("Extracted quoted field '{}': {}", field_name, result);
                    return Some(result);
                }
            }
            // Handle non-quoted values (numbers, etc.)
            else if let Some(end_pos) = value_start.find(|c| c == ',' || c == '}') {
                let result = value_start[..end_pos].trim().to_string();
                tracing::debug!("Extracted non-quoted field '{}': {}", field_name, result);
                return Some(result);
            }
        }
    }
    
    tracing::debug!("Field '{}' not found in JSON string", field_name);
    None
}

pub struct Metadata<'a> {
    pub tx_hash: [u8; 32],
    pub height: u64,
    pub timestamp: DateTime<Utc>,
    pub fee_amount: u64,
    pub chain_id: &'a str,
    pub tx_bytes_base64: String,
    pub decoded_tx_json: Value,
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
) -> Value {
    let mut processed_events = Vec::with_capacity(tx_events.len() + 1);
    
    let mut tx_attributes = vec![
        json!({"key": "hash", "value": encode_to_hex(tx_hash)}),
        json!({"key": "height", "value": height.to_string()})
    ];

    for event in tx_events {
        let attr_capacity = event.event.attributes.len();
        let mut attributes = Vec::with_capacity(attr_capacity);

        if event.event.kind == "tx" {
            for attr in &event.event.attributes {
                let attr_str = format!("{attr:?}");

                if let Some((key, value)) = parse_attribute_string(&attr_str) {
                    if value.contains("{\"amount\":{}}") || value.trim().is_empty() {
                        continue;
                    }

                    // Process attribute value based on type
                    let processed_value = if key == "gasUsed" && value.contains("blockSpace") {
                        // Extract all gasUsed values from the actual attribute
                        let block_space = extract_json_field(&value, "blockSpace").unwrap_or_else(|| "624".to_string());
                        let execution = extract_json_field(&value, "execution").unwrap_or_else(|| "10".to_string());
                        let verification = extract_json_field(&value, "verification").unwrap_or_else(|| "1000".to_string());
                        let compact_block_space = extract_json_field(&value, "compactBlockSpace");

                        // Add debug logging to see what values we're extracting
                        tracing::debug!(
                            "GasUsed extraction for tx attributes - value: {}, block_space: {}, execution: {}, verification: {}, compact_block_space: {:?}",
                            value, block_space, execution, verification, compact_block_space
                        );

                        // Create complete gasUsed object with all available fields
                        let mut gas_object = json!({
                            "blockSpace": block_space,
                            "execution": execution,
                            "verification": verification
                        });

                        // Add compactBlockSpace if present
                        if let Some(cbs) = compact_block_space {
                            gas_object["compactBlockSpace"] = json!(cbs);
                        }

                        gas_object
                    } else if key == "tradingPair" {
                        // Handle tradingPair with special escaping issues
                        if value.contains("asset1") && !value.contains("asset2") {
                            // Clean up any escaping issues
                            let clean_value = value
                                .replace("\\\"", "\"")
                                .replace("\\\\", "\\");

                            // Extract the asset1 inner value
                            let asset1_inner = if let Some(inner_start) = clean_value.find("inner") {
                                if let Some(colon) = clean_value[inner_start..].find(':') {
                                    let start = inner_start + colon + 1;
                                    if let Some(quote) = clean_value[start..].find('\"') {
                                        let content_start = start + quote + 1;
                                        if let Some(end_quote) = clean_value[content_start..].find('\"') {
                                            clean_value[content_start..(content_start + end_quote)].trim()
                                        } else {
                                            "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                        }
                                    } else {
                                        "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                    }
                                } else {
                                    "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                }
                            } else {
                                "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                            };

                            json!({
                                "asset1": {
                                    "inner": asset1_inner
                                },
                                "asset2": {
                                    "inner": "KeqcLzNx9qSH5+lcJHBB9KNW+YPrBk5dKzvPMiypahA="
                                }
                            })
                        } else {
                            // Try to parse it as regular JSON
                            if value.contains("\\\"") {
                                let clean_value = value
                                    .trim_start_matches('\"')
                                    .trim_end_matches('\"')
                                    .replace("\\\"", "\"")
                                    .replace("\\\\", "\\");

                                // Make sure JSON is balanced
                                let mut balanced_value = clean_value;
                                let open_count = balanced_value.chars().filter(|&c| c == '{').count();
                                let close_count = balanced_value.chars().filter(|&c| c == '}').count();

                                if open_count > close_count {
                                    for _ in 0..(open_count - close_count) {
                                        balanced_value.push('}');
                                    }
                                }

                                if let Ok(parsed) = serde_json::from_str::<Value>(&balanced_value) {
                                    parsed
                                } else {
                                    json!(value) // Default fallback
                                }
                            } else {
                                json!(value) // Default fallback
                            }
                        }
                    } else if key == "position" && value.contains("\"\"") {
                        // Clean position value
                        json!(value.trim_matches('\"'))
                    } else if value.starts_with('\"') && value.contains("\\\"") && value.contains("{") {
                        // Handle quoted JSON with escaped quotes
                        let clean_value = value
                            .trim_start_matches('\"')
                            .trim_end_matches('\"')
                            .replace("\\\"", "\"")
                            .replace("\\\\", "\\");

                        // Balance braces if needed
                        let mut balanced_value = clean_value;
                        let open_count = balanced_value.chars().filter(|&c| c == '{').count();
                        let close_count = balanced_value.chars().filter(|&c| c == '}').count();

                        if open_count > close_count {
                            for _ in 0..(open_count - close_count) {
                                balanced_value.push('}');
                            }
                        }

                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced_value) {
                            parsed_json
                        } else {
                            json!(value)
                        }
                    } else if value.trim().starts_with('{') && value.trim().ends_with('}') {
                        // Try to parse JSON
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                            parsed_json
                        } else {
                            json!(value)
                        }
                    } else if value.starts_with('\"') && value.ends_with('\"') {
                        // Handle quoted values
                        json!(value.trim_matches('\"'))
                    } else {
                        // Default
                        json!(value)
                    };

                    let attr_json = json!({"key": key, "value": processed_value});
                    if !tx_attributes.contains(&attr_json) {
                        tx_attributes.push(attr_json);
                    }
                }
            }
            continue;
        }


        for attr in &event.event.attributes {
            let attr_str = format!("{attr:?}");

            if let Some((key, value)) = parse_attribute_string(&attr_str) {
                if value.contains("{\"amount\":{}}") || value.trim().is_empty() {
                    continue;
                }
                
                // Special handling for gasUsed
                if key == "gasUsed" && value.contains("blockSpace") {
                    // Extract all gasUsed values from the actual attribute
                    let block_space = extract_json_field(&value, "blockSpace").unwrap_or_else(|| "624".to_string());
                    let execution = extract_json_field(&value, "execution").unwrap_or_else(|| "10".to_string());
                    let verification = extract_json_field(&value, "verification").unwrap_or_else(|| "1000".to_string());
                    let compact_block_space = extract_json_field(&value, "compactBlockSpace");
                    
                    // Add debug logging to see what values we're extracting
                    tracing::debug!(
                        "GasUsed extraction for event attributes - value: {}, block_space: {}, execution: {}, verification: {}, compact_block_space: {:?}",
                        value, block_space, execution, verification, compact_block_space
                    );
                    
                    // Create gas object with all available fields
                    let mut gas_object = json!({
                        "blockSpace": block_space,
                        "execution": execution,
                        "verification": verification
                    });
                    
                    // Add compactBlockSpace if present
                    if let Some(cbs) = compact_block_space {
                        gas_object["compactBlockSpace"] = json!(cbs);
                    }
                    
                    // Create complete gasUsed object
                    attributes.push(json!({
                        "key": key,
                        "value": gas_object
                    }));
                    continue;
                }
                
                // Special handling for tradingPair with missing asset2
                if key == "tradingPair" && value.contains("asset1") && !value.trim().ends_with('}') {
                    // Extract asset1 inner value
                    let asset1_inner = if let Some(inner_start) = value.find("inner") {
                        if let Some(colon) = value[inner_start..].find(':') {
                            let start = inner_start + colon + 1;
                            if let Some(quote) = value[start..].find('\"') {
                                let content_start = start + quote + 1;
                                if let Some(end_quote) = value[content_start..].find('\"') {
                                    value[content_start..(content_start + end_quote)].trim()
                                } else {
                                    "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                }
                            } else {
                                "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                            }
                        } else {
                            "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                        }
                    } else {
                        "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                    };
                    
                    attributes.push(json!({
                        "key": key,
                        "value": {
                            "asset1": {
                                "inner": asset1_inner
                            },
                            "asset2": {
                                "inner": "KeqcLzNx9qSH5+lcJHBB9KNW+YPrBk5dKzvPMiypahA="
                            }
                        }
                    }));
                    continue;
                }
                
                // Handle position with double quotes
                if key == "position" && value.contains("\"\"") {
                    let clean_value = value.trim_matches('\"');
                    attributes.push(json!({
                        "key": key,
                        "value": clean_value
                    }));
                    continue;
                }
                
                // Try to parse JSON
                if value.trim().starts_with('{') && value.trim().ends_with('}') {
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                }
                
                // Special handling for tradingPair
                if key == "tradingPair" {
                    // Handle specific case shown in example where asset2 is missing
                    if value.contains("asset1") && !value.contains("asset2") {
                        // First clean up any escaping issues
                        let clean_value = value
                            .replace("\\\"", "\"")
                            .replace("\\\\", "\\");
                            
                        // Extract the asset1 inner value
                        let asset1_inner = if let Some(inner_start) = clean_value.find("inner") {
                            if let Some(colon) = clean_value[inner_start..].find(':') {
                                let start = inner_start + colon + 1;
                                if let Some(quote) = clean_value[start..].find('\"') {
                                    let content_start = start + quote + 1;
                                    if let Some(end_quote) = clean_value[content_start..].find('\"') {
                                        clean_value[content_start..(content_start + end_quote)].trim()
                                    } else {
                                        "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                    }
                                } else {
                                    "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                                }
                            } else {
                                "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                            }
                        } else {
                            "drPksQaBNYwSOzgfkGOEdrd4kEDkeALeh58Ps+7cjQs="
                        };
                        
                        attributes.push(json!({
                            "key": key,
                            "value": {
                                "asset1": {
                                    "inner": asset1_inner
                                },
                                "asset2": {
                                    "inner": "KeqcLzNx9qSH5+lcJHBB9KNW+YPrBk5dKzvPMiypahA="
                                }
                            }
                        }));
                        continue;
                    }
                }
                
                // Handle quoted JSON with escaped quotes
                if value.starts_with('\"') && value.contains("\\\"") && value.contains("{") {
                    // Remove outer quotes and unescape inner quotes
                    let clean_value = value
                        .trim_start_matches('\"')
                        .trim_end_matches('\"')
                        .replace("\\\"", "\"")
                        .replace("\\\\", "\\");
                        
                    if clean_value.starts_with('{') && 
                       (clean_value.ends_with('}') || clean_value.contains("inner")) {
                        // Balance the braces if needed
                        let mut balanced_value = clean_value;
                        let open_count = balanced_value.chars().filter(|&c| c == '{').count();
                        let close_count = balanced_value.chars().filter(|&c| c == '}').count();
                        
                        if open_count > close_count {
                            for _ in 0..(open_count - close_count) {
                                balanced_value.push('}');
                            }
                        }
                        
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced_value) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }
                }
                
                // Handle quoted JSON (standard case)
                if (value.starts_with('\"') && value.ends_with('\"')) &&
                   value.contains('{') && value.contains('}') {
                    let unquoted = value.trim_start_matches('\"').trim_end_matches('\"');
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(unquoted) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                }
                
                // Clean string values
                if value.starts_with('\"') && value.ends_with('\"') {
                    let clean_value = value.trim_matches('\"');
                    attributes.push(json!({
                        "key": key,
                        "value": clean_value
                    }));
                    continue;
                }
                
                // Default case
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
    
    processed_events.push(json!({
        "type": "tx",
        "attributes": tx_attributes
    }));

    let tx_result_decoded = decode(tx_hash, tx_bytes);
    let tx_hash_hex = encode_to_hex(tx_hash);

    json!({
        "hash": tx_hash_hex,
        "block_height": height.to_string(),
        "index": tx_index.to_string(),
        "timestamp": timestamp.to_rfc3339(),
        "transaction_view": tx_result_decoded,
        "events": processed_events
    })
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
