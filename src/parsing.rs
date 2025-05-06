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
pub fn parse_attribute_string(attr_str: &str) -> Option<(String, String)> {
    if attr_str.contains("EventAttribute")
        && attr_str.contains("key:")
        && attr_str.contains("value:")
    {
        let key_start = attr_str.find("key:").unwrap_or(0) + 5;
        let key_end = attr_str[key_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| key_start + pos);
        let key = attr_str[key_start..key_end]
            .trim()
            .trim_matches('"')
            .to_string();

        let value_start = attr_str.find("value:").unwrap_or(0) + 7;
        let value_end = attr_str[value_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| value_start + pos);
        let value = attr_str[value_start..value_end]
            .trim()
            .trim_matches('"')
            .to_string();

        let clean_value = value.replace("\\\"", "\"");

        if clean_value == "{\"amount\":{}}" || clean_value.trim().is_empty() {
            return None;
        }

        return Some((key, clean_value));
    }

    if attr_str.contains("key:") && attr_str.contains("value:") {
        let key_start = attr_str.find("key:").unwrap_or(0) + 4;
        let key_end = attr_str[key_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| key_start + pos);
        let key = attr_str[key_start..key_end]
            .trim()
            .trim_matches('"')
            .to_string();

        let value_start = attr_str.find("value:").unwrap_or(0) + 6;
        let value_end = attr_str[value_start..]
            .find(',')
            .map_or(attr_str.len(), |pos| value_start + pos);
        let value = attr_str[value_start..value_end]
            .trim()
            .trim_matches('"')
            .to_string();

        let clean_value = value.replace("\\\"", "\"");

        if clean_value == "{\"amount\":{}}" || clean_value.trim().is_empty() {
            return None;
        }

        return Some((key, clean_value));
    }

    if attr_str.contains('{') {
        let json_start = attr_str.find('{').unwrap_or(0);
        let field_name = attr_str[0..json_start].trim().to_string();

        if !field_name.is_empty() {
            let mut json_content = attr_str[json_start..].to_string();

            let open_braces = json_content.chars().filter(|&c| c == '{').count();
            let close_braces = json_content.chars().filter(|&c| c == '}').count();

            if open_braces > close_braces {
                for _ in 0..(open_braces - close_braces) {
                    json_content.push('}');
                }
            }

            let clean_json = json_content.replace("\\\"", "\"").replace("\\\\", "\\");

            if clean_json == "{\"amount\":{}}"
                || clean_json.contains("{\"amount\":{}}")
                || clean_json.trim().is_empty()
                || clean_json.ends_with(":{")
            {
                return None;
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
pub fn event_to_json(
    event: ContextualizedEvent<'_>,
    tx_hash: Option<[u8; 32]>,
) -> Result<Value, anyhow::Error> {
    let mut attributes = Vec::new();

    for attr in &event.event.attributes {
        let attr_str = format!("{attr:?}");

        if let Some((key, value)) = parse_attribute_string(&attr_str) {
            if value.contains("{\"amount\":{}}")
                || value.trim().is_empty()
                || value == "{}"
                || value.ends_with(":{")
            {
                continue;
            }

            let fixed_value = if (key == "position" || key == "state") && !value.ends_with('}') {
                let mut fixed = value.clone();
                let open_braces = fixed.chars().filter(|&c| c == '{').count();
                let close_braces = fixed.chars().filter(|&c| c == '}').count();

                for _ in 0..(open_braces - close_braces) {
                    fixed.push('}');
                }
                fixed
            } else if key == "gasUsed" && !value.ends_with('"') {
                value.to_string() + "\""
            } else {
                value
            };

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
