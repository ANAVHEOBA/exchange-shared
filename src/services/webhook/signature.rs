use hmac::{Hmac, Mac};
use sha2::Sha256;
use crate::services::webhook::WebhookError;

type HmacSha256 = Hmac<Sha256>;

/// Generate HMAC-SHA256 signature for webhook payload
pub fn generate_signature(secret: &str, timestamp: i64, payload: &str) -> String {
    let message = format!("{}.{}", timestamp, payload);
    
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    
    let result = mac.finalize();
    format!("sha256={}", hex::encode(result.into_bytes()))
}

/// Verify HMAC-SHA256 signature
pub fn verify_signature(
    secret: &str,
    signature: &str,
    timestamp: i64,
    payload: &str,
    tolerance_secs: i64,
) -> Result<(), WebhookError> {
    // Check timestamp freshness (±5 minutes default)
    let now = chrono::Utc::now().timestamp();
    if (now - timestamp).abs() > tolerance_secs {
        return Err(WebhookError::TimestampTooOld);
    }
    
    // Generate expected signature
    let expected = generate_signature(secret, timestamp, payload);
    
    // Constant-time comparison to prevent timing attacks
    if !constant_time_eq(signature.as_bytes(), expected.as_bytes()) {
        return Err(WebhookError::InvalidSignature);
    }
    
    Ok(())
}

/// Constant-time string comparison
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    
    result == 0
}

/// Generate cryptographically secure random secret key
pub fn generate_secret_key() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.random()).collect();
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_generation() {
        let secret = "test_secret_key_12345";
        let timestamp = 1640000000;
        let payload = r#"{"event":"test"}"#;
        
        let signature = generate_signature(secret, timestamp, payload);
        assert!(signature.starts_with("sha256="));
        assert_eq!(signature.len(), 71); // "sha256=" + 64 hex chars
    }

    #[test]
    fn test_signature_verification_success() {
        let secret = "test_secret_key_12345";
        let timestamp = chrono::Utc::now().timestamp();
        let payload = r#"{"event":"test"}"#;
        
        let signature = generate_signature(secret, timestamp, payload);
        let result = verify_signature(secret, &signature, timestamp, payload, 300);
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_signature_verification_invalid() {
        let secret = "test_secret_key_12345";
        let timestamp = chrono::Utc::now().timestamp();
        let payload = r#"{"event":"test"}"#;
        
        let wrong_signature = "sha256=0000000000000000000000000000000000000000000000000000000000000000";
        let result = verify_signature(secret, wrong_signature, timestamp, payload, 300);
        
        assert!(matches!(result, Err(WebhookError::InvalidSignature)));
    }

    #[test]
    fn test_signature_verification_old_timestamp() {
        let secret = "test_secret_key_12345";
        let old_timestamp = chrono::Utc::now().timestamp() - 600; // 10 minutes ago
        let payload = r#"{"event":"test"}"#;
        
        let signature = generate_signature(secret, old_timestamp, payload);
        let result = verify_signature(secret, &signature, old_timestamp, payload, 300); // 5 min tolerance
        
        assert!(matches!(result, Err(WebhookError::TimestampTooOld)));
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hello!"));
    }

    #[test]
    fn test_generate_secret_key() {
        let key1 = generate_secret_key();
        let key2 = generate_secret_key();
        
        assert_eq!(key1.len(), 64); // 32 bytes = 64 hex chars
        assert_eq!(key2.len(), 64);
        assert_ne!(key1, key2); // Should be random
    }
}
