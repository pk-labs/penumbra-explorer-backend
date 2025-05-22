use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
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

/// Convert a base64-encoded validator identity key to a bech32 "penumbravalid" address
/// 
/// Takes a base64 string like "AADLG+rOXS+9MbdthgyMqjVga507jmtVFJemUM6PJgE="
/// and returns a bech32 address like "penumbravalid1..."
pub fn identity_key_to_validator_address(base64_identity_key: &str) -> Result<String, anyhow::Error> {
    let identity_key_bytes = BASE64.decode(base64_identity_key)
        .map_err(|e| anyhow::anyhow!("Failed to decode base64 identity key: {}", e))?;
    
    let validator_address = penumbra_sdk_proto::serializers::bech32str::encode(
        &identity_key_bytes,
        penumbra_sdk_proto::serializers::bech32str::validator_identity_key::BECH32_PREFIX,
        penumbra_sdk_proto::serializers::bech32str::Bech32m,
    );
    
    Ok(validator_address)
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
    fn test_identity_key_to_validator_address() {
        let identity_key = "AADLG+rOXS+9MbdthgyMqjVga507jmtVFJemUM6PJgE=";
        let result = identity_key_to_validator_address(identity_key);
        assert!(result.is_ok());
        
        let validator_address = result.unwrap();
        assert!(validator_address.starts_with("penumbravalid1"));
        
        let invalid_key = "invalid-base64!@#";
        let result = identity_key_to_validator_address(invalid_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_specific_identity_key_to_validator_address() {
        let identity_key = "fnQuZXrkGEYXXsl9lY5uWPyTOq/s6ZCRk+eUkk40iwY=";
        let result = identity_key_to_validator_address(identity_key);
        
        assert!(result.is_ok(), "Failed to convert identity key to validator address");
        
        let validator_address = result.unwrap();
        println!("Identity Key: {}", identity_key);
        println!("Decoded Validator Address: {}", validator_address);
        
        assert!(validator_address.starts_with("penumbravalid1"));
        assert!(!validator_address.is_empty());
    }
}
