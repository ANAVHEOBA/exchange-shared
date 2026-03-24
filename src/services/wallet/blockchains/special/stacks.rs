use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct StacksDerivation;

impl BlockchainDerivation for StacksDerivation {
    fn coin_type(&self) -> u32 {
        5757
    }

    fn name(&self) -> &'static str {
        "Stacks"
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

        // Add version byte (22 for mainnet P2PKH)
        let mut payload = vec![22u8];
        payload.extend_from_slice(&account_id);

        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let hash1 = hasher.finalize();

        let mut hasher = Sha256::new();
        hasher.update(&hash1);
        let hash2 = hasher.finalize();

        payload.extend_from_slice(&hash2[0..4]);

        // c32check encoding (simplified as base58 for now)
        Ok(bs58::encode(&payload).into_string())
    }
}
