use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn verify_meta_signature(app_secret: &str, signature_header: &str, body: &[u8]) -> bool {
    let received = signature_header
        .strip_prefix("sha256=")
        .unwrap_or(signature_header);

    let mut mac = match HmacSha256::new_from_slice(app_secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };

    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());
    constant_time_eq(expected.as_bytes(), received.as_bytes())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (left, right) in a.iter().zip(b.iter()) {
        result |= left ^ right;
    }

    result == 0
}

#[cfg(test)]
mod tests {
    use super::verify_meta_signature;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    #[test]
    fn verifies_valid_meta_signature() {
        let secret = "top_secret";
        let payload = br#"{"object":"whatsapp_business_account"}"#;

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        assert!(verify_meta_signature(secret, &signature, payload));
    }

    #[test]
    fn rejects_invalid_meta_signature() {
        let secret = "top_secret";
        let payload = br#"{"object":"whatsapp_business_account"}"#;

        assert!(!verify_meta_signature(secret, "sha256=deadbeef", payload));
    }
}
