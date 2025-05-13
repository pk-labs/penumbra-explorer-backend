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
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
    if attr_str.contains("EventAttribute")
        && attr_str.contains("key:")
        && attr_str.contains("value:")
    {
        // Extract the key
        let key_start = attr_str.find("key:").unwrap_or(0) + 5;
        let mut key_end = attr_str[key_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| key_start + pos);
            
        // Adjust if the key contains a quoted comma
        if attr_str[key_start..key_end].contains('"') && 
           attr_str[key_start..key_end].matches('"').count() % 2 != 0 {
            if let Some(next_comma) = attr_str[key_end+1..].find(',') {
                key_end = key_end + 1 + next_comma;
            }
        }
        
        let key = attr_str[key_start..key_end]
            .trim()
            .trim_matches('"')
            .to_string();

        // Extract the full value part including potential JSON objects
        let value_start = attr_str.find("value:").unwrap_or(0) + 7;
        
        // If the value is a JSON object, we need to handle it separately
        let value_str = &attr_str[value_start..];
        let mut value_end = value_str.find(',').unwrap_or(value_str.len());
        
        // Handle JSON objects and arrays that might contain commas
        if value_str[..value_end].trim().starts_with('{') {
            // Count opening and closing braces
            let mut brace_level = 0;
            let mut in_quotes = false;
            let mut found_end = false;
            
            for (i, c) in value_str.char_indices() {
                if c == '"' && (i == 0 || value_str.chars().nth(i-1).unwrap_or(' ') != '\\') {
                    in_quotes = !in_quotes;
                    continue;
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
                    }
                }
            }
            
            if !found_end && brace_level > 0 {
                let mut pos = value_end + 1;
                while pos < value_str.len() {
                    if value_str[pos..].starts_with(',') && !in_quotes {
                        value_end = pos;
                        break;
                    }
                    if value_str[pos..].starts_with('"') && 
                       (pos == 0 || value_str.chars().nth(pos-1).unwrap_or(' ') != '\\') {
                        in_quotes = !in_quotes;
                    }
                    pos += 1;
                }
            }
        }
        
        let value = value_str[..value_end].trim().to_string();

        // Clean up the value
        let mut clean_value = value
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
            .replace("\\n", "\n");
        
        // Special handling for quoted strings with backslashes
        if clean_value.starts_with('\"') && clean_value.ends_with('\\') {
            clean_value = clean_value
                .trim_start_matches('"')
                .trim_end_matches('\\')
                .to_string();
        }
        
        // Fix JSON objects with unbalanced braces
        if clean_value.trim().starts_with('{') {
            let open_braces = clean_value.chars().filter(|&c| c == '{').count();
            let close_braces = clean_value.chars().filter(|&c| c == '}').count();
            
            if open_braces > close_braces {
                for _ in 0..(open_braces - close_braces) {
                    clean_value.push('}');
                }
            }
        }
        
        // Special handling for fee-related fields
        if key == "swappedBaseFeeTotal" || key == "swappedFeeTotal" || key == "swappedTipTotal" {
            return Some((key, clean_value));
        }

        if clean_value == "{\"amount\":{}}" || clean_value.trim().is_empty() {
            return None;
        }

        return Some((key, clean_value));
    }

    if attr_str.contains("key:") && attr_str.contains("value:") {
        // Extract the key
        let key_start = attr_str.find("key:").unwrap_or(0) + 4;
        let mut key_end = attr_str[key_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| key_start + pos);
            
        // Adjust if the key contains a quoted comma
        if attr_str[key_start..key_end].contains('"') && 
           attr_str[key_start..key_end].matches('"').count() % 2 != 0 {
            if let Some(next_comma) = attr_str[key_end+1..].find(',') {
                key_end = key_end + 1 + next_comma;
            }
        }
        
        let key = attr_str[key_start..key_end]
            .trim()
            .trim_matches('"')
            .to_string();

        // Extract the full value part including potential JSON objects
        let value_start = attr_str.find("value:").unwrap_or(0) + 6;
        
        // If the value is a JSON object, we need to handle it separately
        let value_str = &attr_str[value_start..];
        let mut value_end = value_str.find(',').unwrap_or(value_str.len());
        
        // Handle JSON objects and arrays that might contain commas
        if value_str[..value_end].trim().starts_with('{') {
            // Count opening and closing braces
            let mut brace_level = 0;
            let mut in_quotes = false;
            let mut found_end = false;
            
            for (i, c) in value_str.char_indices() {
                if c == '"' && (i == 0 || value_str.chars().nth(i-1).unwrap_or(' ') != '\\') {
                    in_quotes = !in_quotes;
                    continue;
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
                    }
                }
            }
            
            // If we couldn't find the end, try to extend to the next comma after the JSON object
            if !found_end && brace_level > 0 {
                // Try to find the next comma that's not in quotes
                let mut pos = value_end + 1;
                while pos < value_str.len() {
                    if value_str[pos..].starts_with(',') && !in_quotes {
                        value_end = pos;
                        break;
                    }
                    if value_str[pos..].starts_with('"') && 
                       (pos == 0 || value_str.chars().nth(pos-1).unwrap_or(' ') != '\\') {
                        in_quotes = !in_quotes;
                    }
                    pos += 1;
                }
            }
        }
        
        let value = value_str[..value_end].trim().to_string();

        // Clean up the value
        let mut clean_value = value
            .replace("\\\"", "\"")
            .replace("\\\\", "\\")
            .replace("\\n", "\n");
            
        // Special handling for quoted strings with backslashes
        if clean_value.starts_with('\"') && clean_value.ends_with('\\') {
            clean_value = clean_value
                .trim_start_matches('"')
                .trim_end_matches('\\')
                .to_string();
        }
        
        // Fix JSON objects with unbalanced braces
        if clean_value.trim().starts_with('{') {
            let open_braces = clean_value.chars().filter(|&c| c == '{').count();
            let close_braces = clean_value.chars().filter(|&c| c == '}').count();
            
            if open_braces > close_braces {
                for _ in 0..(open_braces - close_braces) {
                    clean_value.push('}');
                }
            }
        }
        
        // Special handling for fee-related fields
        if key == "swappedBaseFeeTotal" || key == "swappedFeeTotal" || key == "swappedTipTotal" {
            return Some((key, clean_value));
        }

        if clean_value == "{\"amount\":{}}" || clean_value.trim().is_empty() {
            return None;
        }

        return Some((key, clean_value));
    }

    if attr_str.contains('{') {
        let json_start = attr_str.find('{').unwrap_or(0);
        let field_name = attr_str[0..json_start].trim().to_string();

        if !field_name.is_empty() {
            // Extract the complete JSON object
            let json_str = &attr_str[json_start..];
            let mut end_pos = json_str.len();
            
            // Handle complex JSON objects with nested structures
            let mut brace_level = 0;
            let mut in_quotes = false;
            let mut found_end = false;
            
            for (i, c) in json_str.char_indices() {
                if c == '"' && (i == 0 || json_str.chars().nth(i-1).unwrap_or(' ') != '\\') {
                    in_quotes = !in_quotes;
                    continue;
                }
                
                if !in_quotes {
                    if c == '{' {
                        brace_level += 1;
                    } else if c == '}' {
                        brace_level -= 1;
                        if brace_level == 0 {
                            end_pos = i + 1;
                            found_end = true;
                            break;
                        }
                    }
                }
            }
            
            let mut json_content = if found_end {
                json_str[..end_pos].to_string()
            } else {
                json_str.to_string()
            };

            // Clean up and fix common issues
            json_content = json_content.replace("\\\"", "\"");
            
            // Balance quotes
            if json_content.matches('"').count() % 2 != 0 {
                json_content.push('"');
            }
            
            // Balance braces
            let open_braces = json_content.chars().filter(|&c| c == '{').count();
            let close_braces = json_content.chars().filter(|&c| c == '}').count();

            if open_braces > close_braces {
                for _ in 0..(open_braces - close_braces) {
                    json_content.push('}');
                }
            }

            // Final cleanup for special cases
            let clean_json = json_content
                .replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\n", "\n");

            // Filter out specific patterns that should be skipped
            if clean_json == "{\"amount\":{}}"
                || clean_json.trim().is_empty()
                || clean_json.ends_with(":{")
            {
                return None;
            }
            
            // Check if this is an amount field but allow it if it has values
            if clean_json.contains("{\"amount\":{") && 
               !clean_json.contains("{\"amount\":{}}") {
                return Some((field_name, clean_json));
            }

            return Some((field_name, clean_json));
        }
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
            if (key == "swappedBaseFeeTotal" || key == "swappedFeeTotal" || key == "swappedTipTotal")
                && value.contains("{\"amount\":{}}") 
            {
                attributes.push(json!({
                    "key": key,
                    "value": {"amount":{}}
                }));
                continue;
            }
            
            if value.trim().is_empty() || value == "{}" || value.ends_with(":{") {
                continue;
            }
            
            // First clean up the value 
            let mut fixed_value = value
                .replace("\\\"", "\"")
                .replace("\\\\", "\\")
                .replace("\\n", "\n");
            
            // Handle special fields that we know have specific formats
            if key == "meta" || key == "value" || key == "packet_data" || 
               key == "gasUsed" || key == "position" || key == "state" || 
               key == "clue" || key == "source" || key == "sender" || key == "receiver" {
                
                // Check if it looks like truncated JSON
                if fixed_value.starts_with('{') && !fixed_value.contains('}') {
                    // Try to complete the JSON object by balancing braces
                    let open_braces = fixed_value.chars().filter(|&c| c == '{').count();
                    let close_braces = fixed_value.chars().filter(|&c| c == '}').count();
                    
                    for _ in 0..(open_braces - close_braces) {
                        fixed_value.push('}');
                    }
                }
                
                // If it ends with a backslash, fix it if it seems to be a truncated JSON string
                if fixed_value.ends_with('\\') {
                    fixed_value = fixed_value.trim_end_matches('\\').to_string();
                    if fixed_value.trim().contains('{') && !fixed_value.trim().ends_with('}') {
                        fixed_value.push('}');
                    }
                }
                
                // Specific handling for gasUsed which often has a complex structure
                if key == "gasUsed" {
                    // This is a special case where we should fetch the entire value again from the attribute
                    // because this complex object often gets truncated in string representation
                    if let Some(attr_str) = attr_str.strip_prefix("V037(EventAttribute ") {
                        if let Some(attr_suffix) = attr_str.strip_suffix(")") {
                            if attr_suffix.contains("gasUsed") && attr_suffix.contains("blockSpace") {
                                // Try to extract the full JSON string, being careful about nested brackets
                                let mut in_value = false;
                                let mut in_json = false;
                                let mut brace_level = 0;
                                let mut json_text = String::new();
                                
                                for (i, c) in attr_suffix.char_indices() {
                                    if !in_value && attr_suffix[i..].starts_with("value:") {
                                        in_value = true;
                                        continue;
                                    }
                                    
                                    if in_value {
                                        if !in_json && c == '{' {
                                            in_json = true;
                                            brace_level = 1;
                                            json_text.push(c);
                                        } else if in_json {
                                            if c == '{' {
                                                brace_level += 1;
                                                json_text.push(c);
                                            } else if c == '}' {
                                                brace_level -= 1;
                                                json_text.push(c);
                                                if brace_level == 0 {
                                                    // We have a complete JSON object
                                                    break;
                                                }
                                            } else {
                                                json_text.push(c);
                                            }
                                        }
                                    }
                                }
                                
                                // If we have a valid looking JSON, try to parse it
                                if !json_text.is_empty() && json_text.starts_with('{') && json_text.ends_with('}') {
                                    let clean_json = json_text
                                        .replace("\\\"", "\"")
                                        .replace("\\\\", "\\")
                                        .replace("\\n", "\n");
                                    
                                    if let Ok(parsed_json) = serde_json::from_str::<Value>(&clean_json) {
                                        attributes.push(json!({
                                            "key": key,
                                            "value": parsed_json
                                        }));
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                
                    // Fallback to the original approach if the above special handling didn't work
                    if fixed_value.contains("blockSpace") && !fixed_value.trim().ends_with('}') {
                        // Check if we need to close the object
                        let open_braces = fixed_value.chars().filter(|&c| c == '{').count();
                        let close_braces = fixed_value.chars().filter(|&c| c == '}').count();
                        
                        for _ in 0..(open_braces - close_braces) {
                            fixed_value.push('}');
                        }
                    }
                }
            }

            // Special handling for fields that tend to be problematic
            if key == "position" && attr_str.contains("closeOnFill") {
                // Extract the complex position data from the raw attribute
                let event_type = event.event.kind.to_string();
                if event_type.contains("EventPositionOpen") || event_type.contains("Position") {
                    // Try to find the full JSON object
                    if let Some(pos) = attr_str.find("{\"closeOnFill\"") {
                        let mut brace_level = 0;
                        let mut in_quotes = false;
                        let mut json_text = String::new();
                        let mut found_end = false;
                        
                        for (i, c) in attr_str[pos..].char_indices() {
                            if c == '"' && (i == 0 || attr_str.chars().nth(pos + i - 1).unwrap_or(' ') != '\\') {
                                in_quotes = !in_quotes;
                                json_text.push(c);
                                continue;
                            }
                            
                            if in_quotes {
                                json_text.push(c);
                            } else if c == '{' {
                                brace_level += 1;
                                json_text.push(c);
                            } else if c == '}' {
                                brace_level -= 1;
                                json_text.push(c);
                                if brace_level == 0 {
                                    found_end = true;
                                    break;
                                }
                            } else {
                                json_text.push(c);
                            }
                        }
                        
                        if found_end && json_text.starts_with('{') && json_text.ends_with('}') {
                            let clean_json = json_text
                                .replace("\\\"", "\"")
                                .replace("\\\\", "\\")
                                .replace("\\n", "\n");
                                
                            if let Ok(parsed_json) = serde_json::from_str::<Value>(&clean_json) {
                                attributes.push(json!({
                                    "key": key,
                                    "value": parsed_json
                                }));
                                continue;
                            }
                        }
                    }
                }
            } else if key == "tradingPair" && attr_str.contains("asset1") && attr_str.contains("asset2") {
                // Extract the tradingPair data
                if let Some(pos) = attr_str.find("{\"asset1\"") {
                    let mut brace_level = 0;
                    let mut in_quotes = false;
                    let mut json_text = String::new();
                    let mut found_end = false;
                    
                    for (i, c) in attr_str[pos..].char_indices() {
                        if c == '"' && (i == 0 || attr_str.chars().nth(pos + i - 1).unwrap_or(' ') != '\\') {
                            in_quotes = !in_quotes;
                            json_text.push(c);
                            continue;
                        }
                        
                        if in_quotes {
                            json_text.push(c);
                        } else if c == '{' {
                            brace_level += 1;
                            json_text.push(c);
                        } else if c == '}' {
                            brace_level -= 1;
                            json_text.push(c);
                            if brace_level == 0 {
                                found_end = true;
                                break;
                            }
                        } else {
                            json_text.push(c);
                        }
                    }
                    
                    if found_end && json_text.starts_with('{') && json_text.ends_with('}') {
                        let clean_json = json_text
                            .replace("\\\"", "\"")
                            .replace("\\\\", "\\")
                            .replace("\\n", "\n");
                            
                        if let Ok(parsed_json) = serde_json::from_str::<Value>(&clean_json) {
                            attributes.push(json!({
                                "key": key,
                                "value": parsed_json
                            }));
                            continue;
                        }
                    }
                }
            } else if key == "gasUsed" {
                // Look for the full JSON structure with all fields in the original attribute
                if attr_str.contains("blockSpace") && attr_str.contains("compactBlockSpace") && 
                   attr_str.contains("execution") && attr_str.contains("verification") {
                    
                    // Extract the structured data from the original string
                    let mut block_space = "";
                    let mut compact_block_space = "";
                    let mut execution = "";
                    let mut verification = "";
                    
                    // Simple extraction of values (this is a bit hacky but works for our specific case)
                    if let Some(pos) = attr_str.find("blockSpace") {
                        if let Some(quote_pos) = attr_str[pos..].find(':') {
                            let start = pos + quote_pos + 1;
                            if let Some(end) = attr_str[start..].find(',') {
                                block_space = attr_str[start..(start+end)].trim_matches('"').trim();
                            }
                        }
                    }
                    
                    if let Some(pos) = attr_str.find("compactBlockSpace") {
                        if let Some(quote_pos) = attr_str[pos..].find(':') {
                            let start = pos + quote_pos + 1;
                            if let Some(end) = attr_str[start..].find(',') {
                                compact_block_space = attr_str[start..(start+end)].trim_matches('"').trim();
                            }
                        }
                    }
                    
                    if let Some(pos) = attr_str.find("execution") {
                        if let Some(quote_pos) = attr_str[pos..].find(':') {
                            let start = pos + quote_pos + 1;
                            if let Some(end) = attr_str[start..].find(',') {
                                execution = attr_str[start..(start+end)].trim_matches('"').trim();
                            } else if let Some(end) = attr_str[start..].find('}') {
                                execution = attr_str[start..(start+end)].trim_matches('"').trim();
                            }
                        }
                    }
                    
                    if let Some(pos) = attr_str.find("verification") {
                        if let Some(quote_pos) = attr_str[pos..].find(':') {
                            let start = pos + quote_pos + 1;
                            if let Some(end) = attr_str[start..].find(',') {
                                verification = attr_str[start..(start+end)].trim_matches('"').trim();
                            } else if let Some(end) = attr_str[start..].find('}') {
                                verification = attr_str[start..(start+end)].trim_matches('"').trim();
                            }
                        }
                    }
                    
                    // If we successfully extracted the values, create a complete JSON
                    if !block_space.is_empty() && !compact_block_space.is_empty() && 
                       !execution.is_empty() && !verification.is_empty() {
                        let complete_json = json!({
                            "blockSpace": block_space,
                            "compactBlockSpace": compact_block_space,
                            "execution": execution,
                            "verification": verification
                        });
                        
                        attributes.push(json!({
                            "key": key,
                            "value": complete_json
                        }));
                        continue;
                    }
                }
            }
            
            // General handling for all other known JSON fields
            if key == "identityKey" || key == "anchor" || key == "root" || 
               key == "nullifier" || key == "position" || key == "state" ||
               key == "meta" || key == "value" || key == "packet_data" || 
               key == "gasUsed" || key == "clue" || key == "source" || 
               key == "sender" || key == "receiver" || key == "tx" || 
               key == "tradingPair" || key == "output1Commitment" || 
               key == "output2Commitment" || key == "fee" || key == "baseFee" || 
               key == "tip" || key == "swappedBaseFeeTotal" || key == "swappedFeeTotal" || 
               key == "swappedTipTotal" {
                
                // Remove any outer quotes that might be interfering with JSON parsing
                let unquoted = if fixed_value.starts_with('"') && fixed_value.ends_with('"') {
                    fixed_value.trim_start_matches('"').trim_end_matches('"').to_string()
                } else {
                    fixed_value.clone()
                };
                
                // Additional cleanup for these special fields
                let extra_clean = unquoted
                    .replace("\\\"", "\"")
                    .replace("\\\\", "\\")
                    .replace("\\n", "\n");
                    
                if let Ok(parsed_json) = serde_json::from_str::<Value>(&extra_clean) {
                    attributes.push(json!({
                        "key": key,
                        "value": parsed_json
                    }));
                    continue;
                }
            }
            
            // General JSON parsing for any value that looks like JSON
            if fixed_value.trim().starts_with('{') && fixed_value.trim().ends_with('}') {
                if let Ok(parsed_json) = serde_json::from_str::<Value>(&fixed_value) {
                    attributes.push(json!({
                        "key": key,
                        "value": parsed_json
                    }));
                    continue;
                }
            }
            
            // Handle JSON strings that are wrapped in quotes
            if fixed_value.starts_with('"') && fixed_value.ends_with('"') {
                let unquoted = fixed_value.trim_start_matches('"').trim_end_matches('"');
                
                if unquoted.trim().starts_with('{') && unquoted.trim().ends_with('}') {
                    if let Ok(parsed_json) = serde_json::from_str::<Value>(unquoted) {
                        attributes.push(json!({
                            "key": key,
                            "value": parsed_json
                        }));
                        continue;
                    }
                }
                
                // For plain string values, remove extra quotes
                attributes.push(json!({
                    "key": key,
                    "value": unquoted
                }));
                continue;
            }
            
            // Handle potential JSON that might be partially formatted
            if fixed_value.contains('{') && fixed_value.contains('"') && 
               (fixed_value.contains(':') || fixed_value.contains(',')) {
                
                // If it's potentially a JSON string but lacks proper quoting
                // first attempt to fix it by ensuring it has opening and closing braces
                let mut cleaned = fixed_value.clone();
                if !cleaned.trim().starts_with('{') {
                    cleaned = "{".to_string() + &cleaned;
                }
                if !cleaned.trim().ends_with('}') {
                    // Count opening and closing braces to add the right number
                    let open_braces = cleaned.chars().filter(|&c| c == '{').count();
                    let close_braces = cleaned.chars().filter(|&c| c == '}').count();
                    
                    for _ in 0..(open_braces - close_braces) {
                        cleaned.push('}');
                    }
                }
                
                // Try to parse it after fixes
                if let Ok(parsed_json) = serde_json::from_str::<Value>(&cleaned) {
                    attributes.push(json!({
                        "key": key,
                        "value": parsed_json
                    }));
                    continue;
                }
            }
            
            attributes.push(json!({
                "key": key,
                "value": fixed_value
            }));
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
        assert_eq!(value, "{\"timestamp\": 12345, \"block\": 100}");

        let incomplete_json = "position {\"closeOnFill\":true";
        let result = parse_attribute_string(incomplete_json);
        assert!(result.is_some());
        let (key, value) = result.unwrap();
        assert_eq!(key, "position");
        assert_eq!(value, "{\"closeOnFill\":true}");

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
}
