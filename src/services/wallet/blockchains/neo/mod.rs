use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct NeoDerivation;

impl BlockchainDerivation for NeoDerivation {
    fn coin_type(&self) -> u32 {
        888
    }

    fn name(&self) -> &'static str {
        "Neo"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use ripemd::Ripemd160;
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();

        let secret_key =
            SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();

        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let sha_hash = hasher.finalize();

        let mut hasher = Ripemd160::new();
        hasher.update(&sha_hash);
        let account_id = hasher.finalize();

        // Add version byte (0x35 for Neo N3)
        let mut payload = vec![0x35u8];
        payload.extend_from_slice(&account_id);

        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash1 = hasher.finalize();

        let mut hasher = Sha256::new();
        hasher.update(&hash1);
        let hash2 = hasher.finalize();

        payload.extend_from_slice(&hash2[0..4]);

        Ok(bs58::encode(&payload).into_string())
    }
}
