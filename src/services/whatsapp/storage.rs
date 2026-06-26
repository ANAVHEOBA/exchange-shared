use aes_gcm::{
    aead::{rand_core::RngCore, Aead, OsRng},
    Aes256Gcm, KeyInit, Nonce,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::types::NormalizedWebhookEvent;

const STORAGE_VERSION: u8 = 1;
const NONCE_SIZE: usize = 12;

pub fn derive_whatsapp_client_id(phone_number_id: &str, wa_id: &str) -> String {
    let digest = Sha256::digest(format!(
        "whatsapp:{}:{}",
        phone_number_id.trim(),
        wa_id.trim()
    ));
    let hex = hex::encode(&digest[..16]);

    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

pub fn build_stored_event_payload(event: &NormalizedWebhookEvent) -> Result<Value, String> {
    match event.event_kind.as_str() {
        "message" => {
            let encrypted_text = match event.text_preview.as_deref() {
                Some(text) if !text.trim().is_empty() => Some(encrypt_text(text)?),
                _ => None,
            };

            Ok(json!({
                "v": STORAGE_VERSION,
                "kind": "message",
                "phone_number_id": event.phone_number_id,
                "wa_id": event.wa_id,
                "provider_message_id": event.provider_message_id,
                "message_type": event.message_type,
                "event_timestamp": event.event_timestamp,
                "text_len": event.text_preview.as_ref().map(|text| text.chars().count()),
                "encrypted_text": encrypted_text,
            }))
        }
        "status" => Ok(json!({
            "v": STORAGE_VERSION,
            "kind": "status",
            "phone_number_id": event.phone_number_id,
            "wa_id": event.wa_id,
            "provider_message_id": event.provider_message_id,
            "status": event.message_type,
            "event_timestamp": event.event_timestamp,
        })),
        _ => Ok(json!({
            "v": STORAGE_VERSION,
            "kind": event.event_kind,
            "phone_number_id": event.phone_number_id,
            "wa_id": event.wa_id,
            "provider_message_id": event.provider_message_id,
            "message_type": event.message_type,
            "event_timestamp": event.event_timestamp,
        })),
    }
}

pub fn extract_message_text_from_payload(payload: &Value) -> Result<Option<String>, String> {
    let Some(ciphertext) = payload.get("encrypted_text").and_then(Value::as_str) else {
        return Ok(None);
    };

    decrypt_text(ciphertext).map(Some)
}

pub fn redact_text_preview(text: &str) -> String {
    format!("[redacted:{} chars]", text.chars().count())
}

pub fn redact_outbound_body(body: &str) -> String {
    format!("[redacted outbound:{} chars]", body.chars().count())
}

fn encrypt_text(value: &str) -> Result<String, String> {
    let cipher = build_cipher();
    let mut nonce = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce);

    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), value.as_bytes())
        .map_err(|error| format!("failed to encrypt WhatsApp payload: {}", error))?;

    let mut combined = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    combined.extend_from_slice(&nonce);
    combined.extend_from_slice(&ciphertext);

    Ok(STANDARD.encode(combined))
}

fn decrypt_text(value: &str) -> Result<String, String> {
    let bytes = STANDARD
        .decode(value)
        .map_err(|error| format!("failed to decode encrypted WhatsApp payload: {}", error))?;

    if bytes.len() <= NONCE_SIZE {
        return Err("encrypted WhatsApp payload is truncated".to_string());
    }

    let (nonce, ciphertext) = bytes.split_at(NONCE_SIZE);
    let plaintext = build_cipher()
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|error| format!("failed to decrypt WhatsApp payload: {}", error))?;

    String::from_utf8(plaintext)
        .map_err(|error| format!("decrypted WhatsApp payload is not UTF-8: {}", error))
}

fn build_cipher() -> Aes256Gcm {
    Aes256Gcm::new_from_slice(&resolve_storage_key())
        .expect("WhatsApp storage key must resolve to 32 bytes")
}

fn resolve_storage_key() -> [u8; 32] {
    if let Ok(configured) = std::env::var("WHATSAPP_STORAGE_KEY") {
        if let Some(bytes) = try_decode_key_material(configured.trim()) {
            return bytes;
        }
    }

    let fallback = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "development-whatsapp-storage-key".to_string());
    let digest = Sha256::digest(fallback.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

fn try_decode_key_material(input: &str) -> Option<[u8; 32]> {
    if input.len() == 64 {
        if let Ok(decoded) = hex::decode(input) {
            if decoded.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(&decoded);
                return Some(key);
            }
        }
    }

    if let Ok(decoded) = STANDARD.decode(input) {
        if decoded.len() == 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&decoded);
            return Some(key);
        }
    }

    if input.as_bytes().len() == 32 {
        let mut key = [0u8; 32];
        key.copy_from_slice(input.as_bytes());
        return Some(key);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{
        build_stored_event_payload, derive_whatsapp_client_id, extract_message_text_from_payload,
    };
    use crate::services::whatsapp::NormalizedWebhookEvent;
    use serde_json::json;

    #[test]
    fn derives_stable_client_id() {
        let first = derive_whatsapp_client_id("123", "456");
        let second = derive_whatsapp_client_id("123", "456");
        let third = derive_whatsapp_client_id("123", "789");

        assert_eq!(first, second);
        assert_ne!(first, third);
        assert_eq!(first.len(), 36);
    }

    #[test]
    fn encrypted_payload_round_trip_works() {
        let event = NormalizedWebhookEvent {
            dedupe_key: "abc".to_string(),
            phone_number_id: "123".to_string(),
            wa_id: Some("456".to_string()),
            provider_message_id: Some("wamid.1".to_string()),
            event_kind: "message".to_string(),
            message_type: Some("text".to_string()),
            event_timestamp: Some("1711111111".to_string()),
            text_preview: Some("swap 100 usdc on stellar to bitcoin".to_string()),
            payload: json!({"ignored":"raw"}),
        };

        let stored = build_stored_event_payload(&event).expect("stored payload");
        let decrypted = extract_message_text_from_payload(&stored).expect("decrypt");

        assert_eq!(
            decrypted.as_deref(),
            Some("swap 100 usdc on stellar to bitcoin")
        );
    }
}
