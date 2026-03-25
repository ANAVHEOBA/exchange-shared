use crate::services::wallet::blockchains::encoding::{base32_encode_nopad, crc16_xmodem};
use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct StellarDerivation;

impl BlockchainDerivation for StellarDerivation {
    fn coin_type(&self) -> u32 {
        148
    }

    fn name(&self) -> &'static str {
        "Stellar"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use ed25519_dalek::{SigningKey, VerifyingKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();

        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&derived[0..32]);

        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key: VerifyingKey = signing_key.verifying_key();
        let public_key_bytes = verifying_key.to_bytes();

        // Stellar StrKey account IDs use version byte 6 << 3 = 48.
        let mut payload = vec![6u8 << 3];
        payload.extend_from_slice(&public_key_bytes);
        payload.extend_from_slice(&crc16_xmodem(&payload).to_le_bytes());

        Ok(base32_encode_nopad(&payload))
    }
}
