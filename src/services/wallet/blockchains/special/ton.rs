use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct TonDerivation;

impl BlockchainDerivation for TonDerivation {
    fn coin_type(&self) -> u32 {
        607
    }

    fn name(&self) -> &'static str {
        "TON"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use base64::{engine::general_purpose, Engine};
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

        // Simplified TON address (workchain 0, hash of public key)
        let mut hasher = Sha256::new();
        hasher.update(&public_key_bytes);
        let hash = hasher.finalize();

        // Build address: 1 byte flags + 1 byte workchain + 32 bytes hash + 2 bytes checksum
        let mut address_data = vec![0x11u8]; // Bounceable, not testnet
        address_data.push(0x00); // Workchain 0
        address_data.extend_from_slice(&hash);

        // Calculate CRC16 checksum
        let checksum = crc16(&address_data);
        address_data.extend_from_slice(&checksum.to_be_bytes());

        Ok(general_purpose::URL_SAFE.encode(&address_data))
    }
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}
