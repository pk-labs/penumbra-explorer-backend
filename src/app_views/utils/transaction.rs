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
#[allow(clippy::redundant_else, clippy::manual_strip, clippy::range_plus_one, clippy::manual_pattern_char_comparison)]
fn extract_json_field(json_str: &str, field_name: &str) -> Option<String> {
    tracing::debug!("Extracting field '{}' from JSON string: {}", field_name, json_str);
    
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
    
    if let Some(field_pos) = json_str.find(field_name) {
        if let Some(colon_pos) = json_str[field_pos..].find(':') {
            let start_pos = field_pos + colon_pos + 1;
            let value_start = json_str[start_pos..].trim_start();
            
            if value_start.starts_with('"') {
                if let Some(quote_end) = value_start[1..].find('"') {
                    let result = value_start[1..(quote_end+1)].trim().to_string();
                    tracing::debug!("Extracted quoted field '{}': {}", field_name, result);
                    return Some(result);
                }
            }
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

/// Extract gas used fields from a JSON string, using the same pattern as in block.rs
/// by only including fields that are present in the raw data
fn extract_gas_used_values(value: &str) -> serde_json::Value {
    let clean_value = if value.starts_with('"') && value.contains("\\\"") {
        value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
    } else {
        value.to_string()
    };

    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&clean_value) {
        if let Some(obj) = parsed_json.as_object() {
            let mut gas_used = serde_json::json!({});

            if let Some(block_space) = obj.get("blockSpace") {
                gas_used["blockSpace"] = block_space.clone();
            }
            
            if let Some(compact) = obj.get("compactBlockSpace") {
                gas_used["compactBlockSpace"] = compact.clone();
            }
            
            if let Some(exec) = obj.get("execution") {
                gas_used["execution"] = exec.clone();
            }
            
            if let Some(verify) = obj.get("verification") {
                gas_used["verification"] = verify.clone();
            }

            if !gas_used.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
                return gas_used;
            }

            return parsed_json;
        }

        return parsed_json;
    }

    if clean_value.trim().starts_with('{') {
        let mut balanced_value = clean_value.to_string();
        let open_count = balanced_value.chars().filter(|&c| c == '{').count();
        let close_count = balanced_value.chars().filter(|&c| c == '}').count();
        
        if open_count > close_count {
            for _ in 0..(open_count - close_count) {
                balanced_value.push('}');
            }
        }
        
        if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&balanced_value) {
            if let Some(obj) = parsed_json.as_object() {
                let mut gas_used = serde_json::json!({});
                
                if let Some(block_space) = obj.get("blockSpace") {
                    gas_used["blockSpace"] = block_space.clone();
                }
                
                if let Some(compact) = obj.get("compactBlockSpace") {
                    gas_used["compactBlockSpace"] = compact.clone();
                }
                
                if let Some(exec) = obj.get("execution") {
                    gas_used["execution"] = exec.clone();
                }
                
                if let Some(verify) = obj.get("verification") {
                    gas_used["verification"] = verify.clone();
                }
                
                if !gas_used.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
                    return gas_used;
                }
                
                return parsed_json;
            }
            
            return parsed_json;
        }
    }

    let mut gas_object = json!({});

    if let Some(block_space) = extract_json_field(value, "blockSpace") {
        gas_object["blockSpace"] = json!(block_space);
    }
    
    if let Some(execution) = extract_json_field(value, "execution") {
        gas_object["execution"] = json!(execution);
    }
    
    if let Some(verification) = extract_json_field(value, "verification") {
        gas_object["verification"] = json!(verification);
    }
    
    if let Some(compact_block_space) = extract_json_field(value, "compactBlockSpace") {
        gas_object["compactBlockSpace"] = json!(compact_block_space);
    }

    if !gas_object.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
        tracing::debug!("Extracted gas object with found fields: {}", gas_object);
        return gas_object;
    }

    json!({})
}

/// Extracts trading pair asset information from transaction view JSON
fn extract_trading_pair_from_tx_view(tx_json: &Value) -> Option<Value> {
    if let Some(body) = tx_json.get("body") {
        if let Some(actions) = body.get("actions").and_then(|a| a.as_array()) {
            for action in actions {
                if let Some(position_open) = action.get("positionOpen") {
                    if let Some(position) = position_open.get("position") {
                        if let Some(phi) = position.get("phi") {
                            if let Some(pair) = phi.get("pair") {
                                tracing::debug!("Found trading pair in position.phi.pair: {}", pair);
                                return Some(pair.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(view) = tx_json.get("view") {
        if let Some(action) = view.get("action") {
            if let Some(swap) = action.get("Swap") {
                if let Some(trading_pair) = swap.get("trading_pair") {
                    tracing::debug!("Found trading pair in transaction view: {}", trading_pair);
                    return Some(trading_pair.clone());
                }
            }
            
            if let Some(position_open) = action.get("PositionOpen") {
                if let Some(trading_pair) = position_open.get("trading_pair") {
                    tracing::debug!("Found trading pair in PositionOpen: {}", trading_pair);
                    return Some(trading_pair.clone());
                }
            }
            
            if let Some(position_close) = action.get("PositionClose") {
                if let Some(trading_pair) = position_close.get("trading_pair") {
                    tracing::debug!("Found trading pair in PositionClose: {}", trading_pair);
                    return Some(trading_pair.clone());
                }
            }
        }
    }
    
    None
}

/// Extracts complete position object from transaction view JSON
#[allow(dead_code)]
fn extract_position_from_tx_view(tx_json: &Value) -> Option<Value> {
    if let Some(body) = tx_json.get("body") {
        if let Some(actions) = body.get("actions").and_then(|a| a.as_array()) {
            for action in actions {
                if let Some(position_open) = action.get("positionOpen") {
                    if let Some(position) = position_open.get("position") {
                        tracing::debug!("Found position data in transaction view: {}", position);
                        return Some(position.clone());
                    }
                }
            }
        }
    }
    
    if let Some(view) = tx_json.get("view") {
        if let Some(action) = view.get("action") {
            if let Some(position_open) = action.get("PositionOpen") {
                if let Some(position) = position_open.get("position") {
                    tracing::debug!("Found position in PositionOpen: {}", position);
                    return Some(position.clone());
                }
            }
        }
    }
    
    None
}

/// Helper function to extract nested JSON from a string
#[allow(dead_code)]
fn try_extract_nested_json(json_str: &str, field_name: &str) -> Value {
    if let Some(field_pos) = json_str.find(field_name) {
        if let Some(colon_pos) = json_str[field_pos..].find(':') {
            let start_pos = field_pos + colon_pos + 1;
            let value_start = json_str[start_pos..].trim_start();
            
            if value_start.starts_with('{') {
                let mut brace_level = 0;
                let mut end_pos = 0;
                let mut in_quotes = false;
                let mut escaped = false;
                
                for (i, c) in value_start.char_indices() {
                    if in_quotes && c == '\\' {
                        escaped = !escaped;
                        continue;
                    }
                    
                    if c == '"' && !escaped {
                        in_quotes = !in_quotes;
                        escaped = false;
                        continue;
                    }
                    
                    if escaped {
                        escaped = false;
                    }
                    
                    if !in_quotes {
                        if c == '{' {
                            brace_level += 1;
                        } else if c == '}' {
                            brace_level -= 1;
                            if brace_level == 0 {
                                end_pos = i + 1;
                                break;
                            }
                        }
                    }
                }
                
                if end_pos > 0 {
                    let json_part = &value_start[..end_pos];
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(json_part) {
                        return parsed_json;
                    }
                }
            }
        }
    }
    
    Value::Null
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
#[allow(clippy::too_many_lines, clippy::single_char_pattern)]
pub fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<Utc>,
    tx_index: u64,
    tx_events: &[ContextualizedEvent<'_>],
) -> Value {
    let tx_result_decoded = decode(tx_hash, tx_bytes);
    let trading_pair_info = extract_trading_pair_from_tx_view(&tx_result_decoded);
    tracing::debug!("Extracted trading pair from transaction view: {:?}", trading_pair_info);
    
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

                    let processed_value = if key == "gasUsed" {
                        tracing::debug!("Processing gasUsed in tx attributes: {}", value);
                        
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                            if let Some(obj) = parsed_json.as_object() {
                                let mut full_gas_used = serde_json::json!({});
                                
                                if let Some(block_space) = obj.get("blockSpace") {
                                    full_gas_used["blockSpace"] = block_space.clone();
                                }
                                
                                if let Some(compact) = obj.get("compactBlockSpace") {
                                    full_gas_used["compactBlockSpace"] = compact.clone();
                                }
                                
                                if let Some(exec) = obj.get("execution") {
                                    full_gas_used["execution"] = exec.clone();
                                }
                                
                                if let Some(verify) = obj.get("verification") {
                                    full_gas_used["verification"] = verify.clone();
                                }
                                
                                if full_gas_used.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
                                    parsed_json
                                } else {
                                    full_gas_used
                                }
                            } else if let Some(block_space_str) = parsed_json.as_str() {
                                json!({"blockSpace": block_space_str})
                            } else {
                                parsed_json
                            }
                        } else {
                            extract_gas_used_values(&value)
                        }
                    } else if key == "tradingPair" {
                        if let Some(trading_pair) = &trading_pair_info {
                            tracing::debug!("Using trading pair from transaction view: {}", trading_pair);
                            trading_pair.clone()
                        } else {
                            let clean_value = if value.starts_with('"') && value.contains("\\\"") {
                                value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
                            } else {
                                value.to_string()
                            };
                            
                            if clean_value.trim().starts_with('{') {
                                let mut balanced_value = clean_value.to_string();
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
                                    json!(clean_value)
                                }
                            } else {
                                json!(clean_value)
                            }
                        }
                    } else if value.trim().starts_with('{') || (value.starts_with('"') && value.contains("\\\"") && value.contains("{")) {
                        let clean_value = if value.starts_with('"') && value.contains("\\\"") {
                            value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
                        } else {
                            value.to_string()
                        };
                        
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
                        } else if let Some(json_start) = balanced_value.find('{') {
                            let json_part = &balanced_value[json_start..];
                            
                            let mut balanced_json = json_part.to_string();
                            let json_open_count = balanced_json.chars().filter(|&c| c == '{').count();
                            let json_close_count = balanced_json.chars().filter(|&c| c == '}').count();
                            
                            if json_open_count > json_close_count {
                                for _ in 0..(json_open_count - json_close_count) {
                                    balanced_json.push('}');
                                }
                            }
                            
                            if let Ok(parsed_substring) = serde_json::from_str::<Value>(&balanced_json) {
                                parsed_substring
                            } else {
                                json!(balanced_value)
                            }
                        } else {
                            json!(balanced_value)
                        }
                    } else if key == "position" {
                        if value.trim().chars().all(|c| c.is_ascii_digit()) {
                            json!(value.trim_matches('\"'))
                        } else if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                            parsed_json
                        } else {
                            json!(value)
                        }
                    } else if value.trim().starts_with('{') && value.trim().ends_with('}') {
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                            parsed_json
                        } else {
                            json!(value)
                        }
                    } else if value.starts_with('\"') && value.ends_with('\"') {
                        json!(value.trim_matches('\"'))
                    } else {
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
                
                if key == "gasUsed" {
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                        if let Some(obj) = parsed_json.as_object() {
                            let mut full_gas_used = serde_json::json!({});
                            
                            if let Some(block_space) = obj.get("blockSpace") {
                                full_gas_used["blockSpace"] = block_space.clone();
                                
                                if let Some(compact) = obj.get("compactBlockSpace") {
                                    full_gas_used["compactBlockSpace"] = compact.clone();
                                }
                                if let Some(exec) = obj.get("execution") {
                                    full_gas_used["execution"] = exec.clone();
                                }
                                if let Some(verify) = obj.get("verification") {
                                    full_gas_used["verification"] = verify.clone();
                                }
                                
                                attributes.push(json!({
                                    "key": key,
                                    "value": full_gas_used
                                }));
                                continue;
                            }
                            
                            let mut has_fields = false;
                            
                            if let Some(compact) = obj.get("compactBlockSpace") {
                                full_gas_used["compactBlockSpace"] = compact.clone();
                                has_fields = true;
                            }
                            if let Some(exec) = obj.get("execution") {
                                full_gas_used["execution"] = exec.clone();
                                has_fields = true;
                            }
                            if let Some(verify) = obj.get("verification") {
                                full_gas_used["verification"] = verify.clone();
                                has_fields = true;
                            }
                            
                            if has_fields {
                                attributes.push(json!({
                                    "key": key,
                                    "value": full_gas_used
                                }));
                                continue;
                            }
                            
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                        
                        if let Some(block_space_str) = parsed_json.as_str() {
                            attributes.push(json!({
                                "key": key,
                                "value": json!({"blockSpace": block_space_str})
                            }));
                            continue;
                        }
                        
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                    
                    let clean_value = if value.starts_with('"') && value.contains("\\\"") {
                        value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
                    } else {
                        value.to_string()
                    };
                    
                    if let Ok(parsed_clean) = serde_json::from_str::<Value>(&clean_value) {
                        let extracted = extract_gas_used_values(&clean_value);
                        if extracted.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_clean
                            }));
                        } else {
                            attributes.push(json!({
                                "key": key,
                                "value": extracted
                            }));
                        }
                        continue;
                    }
                    
                    let extracted = extract_gas_used_values(&value);
                    if extracted.as_object().unwrap_or(&serde_json::Map::new()).is_empty() {
                        attributes.push(json!({
                            "key": key,
                            "value": value
                        }));
                    } else {
                        attributes.push(json!({
                            "key": key,
                            "value": extracted
                        }));
                    }
                    continue;
                }
                
                if key == "tradingPair" {
                    if let Some(trading_pair) = &trading_pair_info {
                        tracing::debug!("Using complete trading pair from transaction view for event attribute: {}", trading_pair);
                        attributes.push(json!({
                            "key": key,
                            "value": trading_pair
                        }));
                        continue;
                    }
                    
                    let clean_value = if value.starts_with('"') && value.contains("\\\"") {
                        value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
                    } else {
                        value.to_string()
                    };
                    
                    if clean_value.trim().starts_with('{') {
                        let mut balanced_value = clean_value.to_string();
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
                        
                        if let Some(json_start) = balanced_value.find('{') {
                            let json_fragment = &balanced_value[json_start..];
                            let mut balanced_fragment = json_fragment.to_string();
                            
                            let frag_open = balanced_fragment.chars().filter(|&c| c == '{').count();
                            let frag_close = balanced_fragment.chars().filter(|&c| c == '}').count();
                            
                            if frag_open > frag_close {
                                for _ in 0..(frag_open - frag_close) {
                                    balanced_fragment.push('}');
                                }
                            }
                            
                            if let Ok(parsed_fragment) = serde_json::from_str::<Value>(&balanced_fragment) {
                                attributes.push(json!({
                                    "key": key,
                                    "value": parsed_fragment
                                }));
                                continue;
                            }
                        }
                    }
                    
                    attributes.push(json!({
                        "key": key,
                        "value": clean_value
                    }));
                    continue;
                }
                
                if value.trim().starts_with('{') || (value.starts_with('"') && value.contains("\\\"") && value.contains("{")) {
                    let clean_value = if value.starts_with('"') && value.contains("\\\"") {
                        value.trim_matches('"').replace("\\\"", "\"").replace("\\\\", "\\")
                    } else {
                        value.to_string()
                    };
                    
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
                    
                    if let Some(json_start) = balanced_value.find('{') {
                        let json_part = &balanced_value[json_start..];
                        
                        let mut balanced_json = json_part.to_string();
                        let json_open_count = balanced_json.chars().filter(|&c| c == '{').count();
                        let json_close_count = balanced_json.chars().filter(|&c| c == '}').count();
                        
                        if json_open_count > json_close_count {
                            for _ in 0..(json_open_count - json_close_count) {
                                balanced_json.push('}');
                            }
                        }
                        
                        if let Ok(parsed_substring) = serde_json::from_str::<Value>(&balanced_json) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_substring
                            }));
                            continue;
                        }
                    }
                    
                    attributes.push(json!({
                        "key": key,
                        "value": balanced_value
                    }));
                    continue;
                } else if key == "position" {
                    if value.trim().chars().all(|c| c.is_ascii_digit()) {
                        attributes.push(json!({
                            "key": key,
                            "value": value.trim_matches('\"')
                        }));
                        continue;
                    }
                    
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                    
                    tracing::debug!("Original position value from DB: {}", value);
                    
                    let clean_value = if value.starts_with('\"') && value.contains("\\\"") {
                        value.trim_matches('\"').replace("\\\"", "\"").replace("\\\\", "\\")
                    } else {
                        value.to_string()
                    };
                    
                    if clean_value.trim().starts_with('{') {
                        let mut balanced_value = clean_value.to_string();
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
                    
                    attributes.push(json!({
                        "key": key,
                        "value": value.trim_matches('\"')
                    }));
                    continue;
                }
                
                if value.trim().starts_with('{') && value.trim().ends_with('}') {
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                }
                

                if value.starts_with('\"') && value.contains("\\\"") && value.contains("{") {
                    let clean_value = value
                        .trim_start_matches('\"')
                        .trim_end_matches('\"')
                        .replace("\\\"", "\"")
                        .replace("\\\\", "\\");
                        
                    if clean_value.starts_with('{') && 
                       (clean_value.ends_with('}') || clean_value.contains("inner")) {
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
                
                if value.starts_with('\"') && value.ends_with('\"') {
                    let clean_value = value.trim_matches('\"');
                    attributes.push(json!({
                        "key": key,
                        "value": clean_value
                    }));
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
    
    processed_events.push(json!({
        "type": "tx",
        "attributes": tx_attributes
    }));

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
