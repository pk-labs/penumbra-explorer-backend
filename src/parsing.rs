use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use cometindex::ContextualizedEvent;
use serde_json::{json, Value};
use std::fmt::Write;

/// Helper function to convert bytes to a hexadecimal string
#[must_use]
pub fn encode_to_hex<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    let mut hex_string = String::with_capacity(bytes.len() * 2);

    for &byte in bytes {
        let _ = write!(&mut hex_string, "{byte:02X}");
    }

    hex_string
}

/// Helper function to convert bytes to a base64 string
#[must_use]
pub fn encode_to_base64<T: AsRef<[u8]>>(data: T) -> String {
    let bytes = data.as_ref();
    BASE64.encode(bytes)
}

/// Parse attribute string from an event
///
/// This function extracts key-value pairs from attribute strings in various formats,
/// handling complex nested JSON structures and properly preserving all values.
#[must_use]
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
    let (key, raw_value) = if attr_str.contains("EventAttribute")
        && attr_str.contains("key:")
        && attr_str.contains("value:")
    {
        extract_key_value_from_event_attribute(attr_str)
    } else if attr_str.contains("key:") && attr_str.contains("value:") {
        extract_key_value_from_key_value(attr_str)
    } else if attr_str.contains('{') {
        extract_key_value_from_json(attr_str)?
    } else {
        return None;
    };

    if raw_value.trim().is_empty() || raw_value == "{\"amount\":{}}" {
        return None;
    }

    let processed_value = process_value(&key, &raw_value);

    Some((key, processed_value))
}

/// Process a value string based on its content and key
fn process_value(key: &str, value: &str) -> String {
    if key == "swappedBaseFeeTotal" || key == "swappedFeeTotal" || key == "swappedTipTotal" {
        return value.to_string();
    }

    let clean_value = clean_value_string(value);

    if clean_value.trim().starts_with('{') {
        return process_json_value(&clean_value);
    }

    clean_value
}

/// Clean a value string from escape sequences
fn clean_value_string(value: &str) -> String {
    let mut clean_value = value
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
        .replace("\\n", "\n");

    if clean_value.starts_with('\"') && clean_value.ends_with('\\') {
        if let Some(stripped) = clean_value.strip_prefix('"') {
            if let Some(stripped2) = stripped.strip_suffix('\\') {
                clean_value = stripped2.to_string();
            }
        }
    }

    if clean_value.starts_with('\"') && clean_value.ends_with('\"') {
        clean_value = clean_value
            .trim_start_matches('"')
            .trim_end_matches('"')
            .to_string();
    }

    clean_value
}

/// Process a JSON-formatted value
fn process_json_value(value: &str) -> String {
    let balanced_value = ensure_balanced_braces(value);

    // Try to parse and re-serialize to ensure valid JSON
    if let Ok(parsed_json) = serde_json::from_str::<serde_json::Value>(&balanced_value) {
        // If parsing succeeded, use the serde_json serialization to ensure valid format
        return serde_json::to_string(&parsed_json).unwrap_or_else(|_| balanced_value.to_string());
    }

    balanced_value
}

/// Ensure JSON braces are balanced
fn ensure_balanced_braces(value: &str) -> String {
    let mut balanced_value = value.to_string();

    let open_braces = balanced_value.chars().filter(|&c| c == '{').count();
    let close_braces = balanced_value.chars().filter(|&c| c == '}').count();

    if open_braces > close_braces {
        for _ in 0..(open_braces - close_braces) {
            balanced_value.push('}');
        }
    }

    balanced_value
}

/// Extract key and value from an `EventAttribute` format string
fn extract_key_value_from_event_attribute(attr_str: &str) -> (String, String) {
    let key_start = attr_str.find("key:").unwrap_or(0) + 5;
    let mut key_end = attr_str[key_start..]
        .find(',')
        .map_or(attr_str.len(), |pos| key_start + pos);

    if attr_str[key_start..key_end].contains('"')
        && attr_str[key_start..key_end].matches('"').count() % 2 != 0
    {
        if let Some(next_comma) = attr_str[key_end + 1..].find(',') {
            key_end = key_end + 1 + next_comma;
        }
    }

    let key = attr_str[key_start..key_end]
        .trim()
        .trim_matches('"')
        .to_string();

    let value_start = attr_str.find("value:").unwrap_or(0) + 7;
    let value = extract_complex_value(&attr_str[value_start..]);

    (key, value)
}

/// Extract key and value from a generic key-value format string
fn extract_key_value_from_key_value(attr_str: &str) -> (String, String) {
    let key_start = attr_str.find("key:").unwrap_or(0) + 4;
    let mut key_end = attr_str[key_start..]
        .find(',')
        .map_or(attr_str.len(), |pos| key_start + pos);

    if attr_str[key_start..key_end].contains('"')
        && attr_str[key_start..key_end].matches('"').count() % 2 != 0
    {
        if let Some(next_comma) = attr_str[key_end + 1..].find(',') {
            key_end = key_end + 1 + next_comma;
        }
    }

    let key = attr_str[key_start..key_end]
        .trim()
        .trim_matches('"')
        .to_string();

    let value_start = attr_str.find("value:").unwrap_or(0) + 6;
    let value = extract_complex_value(&attr_str[value_start..]);

    (key, value)
}

/// Extract key and value from a JSON object format string
fn extract_key_value_from_json(attr_str: &str) -> Option<(String, String)> {
    let json_start = attr_str.find('{').unwrap_or(0);
    let field_name = attr_str[0..json_start].trim().to_string();

    if field_name.is_empty() {
        return None;
    }

    let json_content = extract_complex_value(&attr_str[json_start..]);

    Some((field_name, json_content))
}

/// Process a quoted JSON-like string by unescaping it
fn process_quoted_json_string(trimmed: &str) -> Option<String> {
    if trimmed.len() > 2 && trimmed.ends_with('"') {
        let inner_content = &trimmed[1..trimmed.len() - 1];
        let unescaped = inner_content.replace("\\\"", "\"").replace("\\\\", "\\");

        if unescaped.contains('{') {
            let balanced = ensure_balanced_braces(&unescaped);

            if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced) {
                return Some(serde_json::to_string(&parsed_json).unwrap_or(balanced));
            }

            return Some(balanced);
        }

        return Some(unescaped);
    }
    None
}

/// Find the end of a JSON object with proper brace matching
fn find_json_object_end(value_str: &str) -> (usize, bool, i32) {
    let mut brace_level = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    let mut found_end = false;
    let mut value_end = value_str.find(',').unwrap_or(value_str.len());

    for (i, c) in value_str.char_indices() {
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
                    value_end = i + 1;
                    found_end = true;
                    break;
                }
            } else if c == ',' && brace_level == 0 {
                value_end = i;
                found_end = true;
                break;
            }
        }
    }

    (value_end, found_end, brace_level)
}

/// Process a string that might contain a JSON object
fn process_json_containing_string(extracted: &str) -> Option<String> {
    if let Some(start_idx) = extracted.find('{') {
        let json_part = &extracted[start_idx..];

        let balanced = ensure_balanced_braces(json_part);

        if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced) {
            return Some(serde_json::to_string(&parsed_json).unwrap_or(balanced));
        }

        if balanced.trim().starts_with('{') && balanced.trim().ends_with('}') {
            return Some(balanced);
        }
    }
    None
}

/// Process a quoted string that might contain JSON
fn process_quoted_content(extracted: &str) -> Option<String> {
    if extracted.starts_with('"') && extracted.ends_with('"') && extracted.len() > 2 {
        let inner_content = &extracted[1..extracted.len() - 1];
        let unescaped = inner_content.replace("\\\"", "\"").replace("\\\\", "\\");

        if unescaped.trim().starts_with('{') {
            if let Ok(parsed_json) = serde_json::from_str::<Value>(&unescaped) {
                return Some(serde_json::to_string(&parsed_json).unwrap_or(unescaped));
            }

            let open_count = unescaped.chars().filter(|&c| c == '{').count();
            let close_count = unescaped.chars().filter(|&c| c == '}').count();
            if open_count == close_count && open_count > 0 {
                return Some(unescaped);
            }
        }

        return Some(unescaped);
    }
    None
}

/// Extract a potentially complex value (like JSON) accounting for nesting
fn extract_complex_value(value_str: &str) -> String {
    // Handle quoted JSON-like strings
    let trimmed = value_str.trim();
    if trimmed.starts_with('"')
        && trimmed.contains("\\\"")
        && (trimmed.contains('{') || trimmed.contains('}'))
    {
        if let Some(result) = process_quoted_json_string(trimmed) {
            return result;
        }
    }

    // Find potentially JSON content
    let mut value_end = value_str.find(',').unwrap_or(value_str.len());

    if value_str[..min(value_end, value_str.len())]
        .trim()
        .starts_with('{')
    {
        let (new_end, found_end, brace_level) = find_json_object_end(value_str);
        value_end = new_end;

        if !found_end && brace_level > 0 {
            let mut balanced_value = value_str.to_string();
            for _ in 0..brace_level {
                balanced_value.push('}');
            }

            if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced_value) {
                return serde_json::to_string(&parsed_json)
                    .unwrap_or_else(|_| balanced_value.trim().to_string());
            }

            return balanced_value.trim().to_string();
        }
    }

    let extracted = value_str[..min(value_end, value_str.len())].trim();

    // Try to extract JSON if the string contains braces
    if extracted.contains('{') {
        if let Some(result) = process_json_containing_string(extracted) {
            return result;
        }
    }

    // Process quoted strings
    if let Some(result) = process_quoted_content(extracted) {
        return result;
    }

    extracted.to_string()
}

fn min(a: usize, b: usize) -> usize {
    if a < b {
        a
    } else {
        b
    }
}

/// Helper function to try to extract a complete position object from a partial string
/// This implements more aggressive position object extraction and reconstruction
#[allow(clippy::too_many_lines)]
fn extract_full_position_object(value: &str) -> Option<Value> {
    if !value.contains("nonce")
        && !value.contains("phi")
        && !value.contains("reserves")
        && !value.contains("state")
    {
        return None;
    }

    if let Ok(parsed_json) = serde_json::from_str::<Value>(value) {
        return Some(parsed_json);
    }

    let mut position_object = json!({});
    let mut found_at_least_one = false;

    if let Some(nonce_pos) = value.find("nonce") {
        found_at_least_one = true;
        if let Some(colon_pos) = value[nonce_pos..].find(':') {
            let start_pos = nonce_pos + colon_pos + 1;
            let value_start = value[start_pos..].trim_start();

            if let Some(stripped) = value_start.strip_prefix('"') {
                if let Some(quote_end) = stripped.find('"') {
                    let nonce_value = value_start[1..=quote_end].trim();
                    if !nonce_value.is_empty() {
                        position_object["nonce"] = json!(nonce_value);
                    }
                }
            } else if let Some(end_pos) = value_start.find([',', '}']) {
                let nonce_value = value_start[..end_pos].trim();
                if !nonce_value.is_empty() {
                    position_object["nonce"] = json!(nonce_value);
                }
            }
        }
    }

    if value.contains("phi") {
        found_at_least_one = true;
        if let Some(phi_pos) = value.find("phi") {
            if let Some(colon_pos) = value[phi_pos..].find(':') {
                let start_pos = phi_pos + colon_pos + 1;
                let value_start = value[start_pos..].trim_start();

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
                                    end_pos = i + 1; // Include the closing brace
                                    break;
                                }
                            }
                        }
                    }

                    if end_pos > 0 {
                        let phi_json = &value_start[..end_pos];
                        if let Ok(parsed_phi) = serde_json::from_str::<Value>(phi_json) {
                            position_object["phi"] = parsed_phi;
                        }
                    }
                }
            }
        }
    }

    // Extract state if present
    if value.contains("state") {
        found_at_least_one = true;
        if let Some(state_pos) = value.find("state") {
            if let Some(colon_pos) = value[state_pos..].find(':') {
                let start_pos = state_pos + colon_pos + 1;
                let value_start = value[start_pos..].trim_start();

                if value_start.starts_with('{') {
                    // Extract the state object
                    let mut brace_level = 0;
                    let mut end_pos = 0;
                    let mut in_quotes = false;
                    let mut escaped = false;

                    for (i, c) in value_start.char_indices() {
                        // Handle escaping within quotes
                        if in_quotes && c == '\\' {
                            escaped = !escaped;
                            continue;
                        }

                        // Handle quotes, but ignore escaped quotes
                        if c == '"' && !escaped {
                            in_quotes = !in_quotes;
                            escaped = false;
                            continue;
                        }

                        // Reset escaped flag
                        if escaped {
                            escaped = false;
                        }

                        // Only count braces outside quoted strings
                        if !in_quotes {
                            if c == '{' {
                                brace_level += 1;
                            } else if c == '}' {
                                brace_level -= 1;
                                if brace_level == 0 {
                                    end_pos = i + 1; // Include the closing brace
                                    break;
                                }
                            }
                        }
                    }

                    if end_pos > 0 {
                        let state_json = &value_start[..end_pos];
                        if let Ok(parsed_state) = serde_json::from_str::<Value>(state_json) {
                            position_object["state"] = parsed_state;
                        }
                    }
                }
            }
        }
    }

    // Extract reserves if present
    if value.contains("reserves") {
        found_at_least_one = true;
        if let Some(reserves_pos) = value.find("reserves") {
            if let Some(colon_pos) = value[reserves_pos..].find(':') {
                let start_pos = reserves_pos + colon_pos + 1;
                let value_start = value[start_pos..].trim_start();

                if value_start.starts_with('{') {
                    // Extract the reserves object
                    let mut brace_level = 0;
                    let mut end_pos = 0;
                    let mut in_quotes = false;
                    let mut escaped = false;

                    for (i, c) in value_start.char_indices() {
                        // Handle escaping within quotes
                        if in_quotes && c == '\\' {
                            escaped = !escaped;
                            continue;
                        }

                        // Handle quotes, but ignore escaped quotes
                        if c == '"' && !escaped {
                            in_quotes = !in_quotes;
                            escaped = false;
                            continue;
                        }

                        // Reset escaped flag
                        if escaped {
                            escaped = false;
                        }

                        // Only count braces outside quoted strings
                        if !in_quotes {
                            if c == '{' {
                                brace_level += 1;
                            } else if c == '}' {
                                brace_level -= 1;
                                if brace_level == 0 {
                                    end_pos = i + 1; // Include the closing brace
                                    break;
                                }
                            }
                        }
                    }

                    if end_pos > 0 {
                        let reserves_json = &value_start[..end_pos];
                        if let Ok(parsed_reserves) = serde_json::from_str::<Value>(reserves_json) {
                            position_object["reserves"] = parsed_reserves;
                        }
                    }
                }
            }
        }
    }

    if found_at_least_one {
        return Some(position_object);
    }

    None
}

/// Converts a Penumbra event to JSON format
///
/// # Errors
/// Returns an error if JSON serialization fails, or if attribute conversion fails
#[allow(clippy::too_many_lines)]
pub fn event_to_json(
    event: ContextualizedEvent<'_>,
    tx_hash: Option<[u8; 32]>,
) -> Result<Value, anyhow::Error> {
    let mut attributes = Vec::new();

    for attr in &event.event.attributes {
        let attr_str = format!("{attr:?}");

        if let Some((key, value)) = parse_attribute_string(&attr_str) {
            if (key == "swappedBaseFeeTotal"
                || key == "swappedFeeTotal"
                || key == "swappedTipTotal")
                && value.contains("{\"amount\":{}}")
            {
                attributes.push(json!({
                    "key": key,
                    "value": {"amount":{}}
                }));
                continue;
            }

            if value.trim().is_empty() || value == "{}" || value == "{" {
                continue;
            }

            if value.trim().starts_with('{') {
                if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                    attributes.push(json!({
                        "key": key,
                        "value": parsed_json
                    }));
                    continue;
                }
            }

            let clean_value = if value.starts_with('"') && value.ends_with('"') && value.len() > 2 {
                value[1..value.len() - 1]
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\")
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

            if value.contains("\\n") && value.contains('{') && value.contains('}') {
                let unescaped = value
                    .replace("\\n", "\n")
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\");

                if let Some(start) = unescaped.find('{') {
                    let potential_json = &unescaped[start..];
                    let mut brace_level = 0;
                    let mut end_pos = 0;

                    for (i, c) in potential_json.char_indices() {
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

                    if end_pos > 0 {
                        let json_part = &potential_json[..end_pos];
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(json_part) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }
                }
            }

            if value.contains('{') {
                if let Some(start) = value.find('{') {
                    let substring = &value[start..];
                    let mut brace_level = 0;
                    let mut end_pos = 0;
                    let mut in_quotes = false;
                    let mut escaped = false;

                    for (i, c) in substring.char_indices() {
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
                        let json_part = &substring[..end_pos];
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(json_part) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }

                    if end_pos == 0 {
                        let mut balanced_json = substring.to_string();
                        let open_count = balanced_json.chars().filter(|&c| c == '{').count();
                        let close_count = balanced_json.chars().filter(|&c| c == '}').count();

                        if open_count > close_count {
                            for _ in 0..(open_count - close_count) {
                                balanced_json.push('}');
                            }
                        }

                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced_json) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }
                }
            }

            match key.as_str() {
                "tradingPair" => {
                    if clean_value.contains("asset1") || clean_value.contains("asset2") {
                        if let Some(start) = clean_value.find('{') {
                            let substring = &clean_value[start..];
                            let mut balanced = substring.to_string();
                            let open_count = balanced.chars().filter(|&c| c == '{').count();
                            let close_count = balanced.chars().filter(|&c| c == '}').count();

                            if open_count > close_count {
                                for _ in 0..(open_count - close_count) {
                                    balanced.push('}');
                                }
                            }

                            if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced) {
                                attributes.push(json!({
                                    "key": key,
                                    "value": parsed_json
                                }));
                                continue;
                            }
                        }
                    }
                }
                "position" => {
                    if value.contains("nonce")
                        || value.contains("phi")
                        || value.contains("reserves")
                    {
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }

                        if let Some(position_data) = extract_full_position_object(&value) {
                            attributes.push(json!({
                                "key": key,
                                "value": position_data
                            }));
                            continue;
                        }

                        if let Some(start) = value.find('{') {
                            let substring = &value[start..];
                            let mut balanced = substring.to_string();
                            let open_count = balanced.chars().filter(|&c| c == '{').count();
                            let close_count = balanced.chars().filter(|&c| c == '}').count();

                            if open_count > close_count {
                                for _ in 0..(open_count - close_count) {
                                    balanced.push('}');
                                }
                            }

                            if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced) {
                                attributes.push(json!({
                                    "key": key,
                                    "value": parsed_json
                                }));
                                continue;
                            }

                            let mut position_object = json!({});

                            if let Some(nonce_pos) = balanced.find("nonce") {
                                if let Some(colon_pos) = balanced[nonce_pos..].find(':') {
                                    let start_pos = nonce_pos + colon_pos + 1;
                                    let value_start = balanced[start_pos..].trim_start();

                                    if let Some(stripped) = value_start.strip_prefix('"') {
                                        if let Some(quote_end) = stripped.find('"') {
                                            let nonce_value = value_start[1..=quote_end].trim();
                                            if !nonce_value.is_empty() {
                                                position_object["nonce"] = json!(nonce_value);
                                            }
                                        }
                                    } else if let Some(end_pos) = value_start.find([',', '}']) {
                                        let nonce_value = value_start[..end_pos].trim();
                                        if !nonce_value.is_empty() {
                                            position_object["nonce"] = json!(nonce_value);
                                        }
                                    }
                                }
                            }

                            if !position_object
                                .as_object()
                                .unwrap_or(&serde_json::Map::new())
                                .is_empty()
                            {
                                attributes.push(json!({
                                    "key": key,
                                    "value": position_object
                                }));
                                continue;
                            }
                        }
                    } else if value.trim().chars().all(|c| c.is_ascii_digit()) {
                        attributes.push(json!({
                            "key": key,
                            "value": value.trim_matches('"')
                        }));
                        continue;
                    }
                }
                "gasUsed" => {
                    tracing::debug!("Processing gasUsed with raw value: '{}'", value);

                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&value) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }

                    if value.starts_with('"') && value.contains("\\\"") {
                        let unescaped = value
                            .trim_matches('"')
                            .replace("\\\"", "\"")
                            .replace("\\\\", "\\");

                        tracing::debug!("Trying to parse unescaped gasUsed: {}", unescaped);

                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&unescaped) {
                            tracing::debug!(
                                "Successfully parsed unescaped gasUsed JSON: {}",
                                parsed_json
                            );
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }

                    if value.contains('{') {
                        if let Some(start) = value.find('{') {
                            let substring = &value[start..];
                            let mut balanced = substring.to_string();
                            let open_count = balanced.chars().filter(|&c| c == '{').count();
                            let close_count = balanced.chars().filter(|&c| c == '}').count();

                            if open_count > close_count {
                                for _ in 0..(open_count - close_count) {
                                    balanced.push('}');
                                }
                            }

                            tracing::debug!("Trying to parse balanced gasUsed JSON: {}", balanced);

                            if let Ok(parsed_json) = serde_json::from_str::<Value>(&balanced) {
                                tracing::debug!(
                                    "Successfully parsed balanced gasUsed JSON: {}",
                                    parsed_json
                                );
                                attributes.push(json!({
                                    "key": key,
                                    "value": parsed_json
                                }));
                                continue;
                            }
                        }
                    }

                    if value.contains("blockSpace")
                        || value.contains("compactBlockSpace")
                        || value.contains("execution")
                        || value.contains("verification")
                    {
                        let mut gas_object = json!({});
                        let fields = [
                            "blockSpace",
                            "compactBlockSpace",
                            "execution",
                            "verification",
                        ];
                        let mut found_at_least_one = false;

                        for field in &fields {
                            if let Some(field_pos) = value.find(field) {
                                if let Some(colon_pos) = value[field_pos..].find(':') {
                                    let start_pos = field_pos + colon_pos + 1;
                                    let value_start = value[start_pos..].trim_start();

                                    if let Some(stripped) = value_start.strip_prefix('"') {
                                        if let Some(quote_end) = stripped.find('"') {
                                            let field_value = value_start[1..=quote_end].trim();
                                            if !field_value.is_empty() {
                                                found_at_least_one = true;
                                                gas_object[field] = json!(field_value);
                                            }
                                        }
                                    } else if let Some(end_pos) = value_start.find([',', '}']) {
                                        let field_value = value_start[..end_pos].trim();
                                        if !field_value.is_empty() {
                                            found_at_least_one = true;
                                            gas_object[field] = json!(field_value);
                                        }
                                    }
                                }
                            }
                        }

                        if found_at_least_one {
                            tracing::debug!("Extracted gasUsed fields manually: {}", gas_object);
                            attributes.push(json!({
                                "key": key,
                                "value": gas_object
                            }));
                            continue;
                        }
                    }

                    if value.trim().chars().all(|c| c.is_ascii_digit() || c == '"') {
                        let clean_value = value.trim().trim_matches('"');
                        if !clean_value.is_empty() {
                            attributes.push(json!({
                                "key": key,
                                "value": {
                                    "blockSpace": clean_value
                                }
                            }));
                            continue;
                        }
                    }

                    tracing::debug!(
                        "No parseable gasUsed structure found, using raw value: {}",
                        value
                    );
                    attributes.push(json!({
                        "key": key,
                        "value": value
                    }));
                }
                _ => {}
            }

            if value.starts_with('"') && value.ends_with('"') && value.len() > 2 {
                attributes.push(json!({
                    "key": key,
                    "value": clean_value
                }));
            } else {
                attributes.push(json!({
                    "key": key,
                    "value": value
                }));
            }
        }
    }

    let json_event = json!({
        "block_id": event.block_height,
        "tx_id": tx_hash.map(encode_to_hex),
        "type": event.event.kind,
        "attributes": attributes
    });

    Ok(json_event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_to_hex() {
        assert_eq!(encode_to_hex([]), "");
        assert_eq!(encode_to_hex([0]), "00");
        assert_eq!(encode_to_hex([255]), "FF");
        assert_eq!(encode_to_hex([0, 1, 2, 3]), "00010203");
        assert_eq!(encode_to_hex([255, 254, 253, 252]), "FFFEFDFC");

        let vec_bytes = vec![10, 20, 30, 40, 50];
        assert_eq!(encode_to_hex(vec_bytes), "0A141E2832");

        let array = [171, 205, 239];
        assert_eq!(encode_to_hex(&array[..]), "ABCDEF");
    }

    #[test]
    fn test_encode_to_base64() {
        assert_eq!(encode_to_base64([]), "");
        assert_eq!(encode_to_base64([0]), "AA==");
        assert_eq!(encode_to_base64([255]), "/w==");

        assert_eq!(encode_to_base64([0, 1, 2, 3]), "AAECAw==");
        assert_eq!(encode_to_base64([255, 254, 253, 252]), "//79/A==");

        let vec_bytes = vec![72, 101, 108, 108, 111];
        assert_eq!(encode_to_base64(vec_bytes), "SGVsbG8=");

        let array = [84, 101, 115, 116, 105, 110, 103];
        assert_eq!(encode_to_base64(&array[..]), "VGVzdGluZw==");
    }

    #[test]
    fn test_parse_attribute_string() {
        let attr_with_key_value = "Attribute { key: \"action\", value: \"swap\" }";
        let result = parse_attribute_string(attr_with_key_value);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "action");
        assert!(value.contains("swap"));

        let complex_attr =
            "V037(EventAttribute { key: \"height\", value: \"82095\", index: false })";
        let result = parse_attribute_string(complex_attr);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "height");
        assert_eq!(value, "82095");

        let attr_with_json = "event_type {\"timestamp\": 12345, \"block\": 100}";
        let result = parse_attribute_string(attr_with_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "event_type");
        assert_eq!(value, "{\"timestamp\":12345,\"block\":100}");

        let incomplete_json = "position {\"closeOnFill\":true";
        let result = parse_attribute_string(incomplete_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "position");
        assert_eq!(value, "{\"closeOnFill\":true}");

        let trading_pair =
            "tradingPair {\"asset1\":{\"inner\":\"test1\"},\"asset2\":{\"inner\":\"test2\"}}";
        let result = parse_attribute_string(trading_pair);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "tradingPair");
        assert!(value.contains("asset1"));
        assert!(value.contains("asset2"));
        assert!(value.contains("test1"));
        assert!(value.contains("test2"));

        let nested_json = "nested {\"level1\":{\"level2\":{\"value\":123}}}";
        let result = parse_attribute_string(nested_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "nested");
        assert!(value.contains("level1"));
        assert!(value.contains("level2"));
        assert!(value.contains("123"));

        let quoted_json = "quoted \"{ \\\"key\\\": \\\"value\\\" }\"";
        let result = parse_attribute_string(quoted_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "quoted");
        assert!(value.contains("key"));
        assert!(value.contains("value"));

        let empty_amount = "swappedFeeTotal {\"amount\":{}}";
        let result = parse_attribute_string(empty_amount);
        assert!(result.is_none());

        let invalid_attr = "Something without key or value";
        let result = parse_attribute_string(invalid_attr);
        assert!(result.is_none());

        let empty_attr = "";
        let result = parse_attribute_string(empty_attr);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_complex_value() {
        assert_eq!(extract_complex_value("simple value"), "simple value");

        assert_eq!(
            extract_complex_value("{\"key\":\"value\"}"),
            "{\"key\":\"value\"}"
        );

        assert_eq!(
            extract_complex_value("{\"key\":\"value\", \"another\":123}"),
            "{\"key\":\"value\", \"another\":123}"
        );

        assert_eq!(
            extract_complex_value("{\"outer\":{\"inner\":\"value\"}}"),
            "{\"outer\":{\"inner\":\"value\"}}"
        );

        assert_eq!(
            extract_complex_value("{\"key\":\"value\", \"unbalanced\":{\"inner\":123"),
            "{\"key\":\"value\", \"unbalanced\":{\"inner\":123}}"
        );

        assert_eq!(
            extract_complex_value("{\"key\":\"value\"}, something else"),
            "{\"key\":\"value\"}"
        );

        assert_eq!(
            extract_complex_value("\"{\\\"quoted\\\":\\\"value\\\"}\""),
            "\"{\\\"quoted\\\":\\\"value\\\"}\""
        );
    }

    #[test]
    fn test_process_json_value() {
        let valid_json = "{\"key\":\"value\"}";
        assert_eq!(process_json_value(valid_json), "{\"key\":\"value\"}");

        let unbalanced_json = "{\"key\":\"value\", \"nested\":{\"inner\":123";
        assert_eq!(
            process_json_value(unbalanced_json),
            "{\"key\":\"value\",\"nested\":{\"inner\":123}}"
        );

        let malformed_json = "{this is not valid json}";
        assert_eq!(
            process_json_value(malformed_json),
            "{this is not valid json}"
        );
    }
}
