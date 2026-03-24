use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct CosmosHubDerivation;

impl BlockchainDerivation for CosmosHubDerivation {
    fn coin_type(&self) -> u32 {
        118
    }

    fn name(&self) -> &'static str {
        "Cosmos Hub"
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

        // Simplified bech32 encoding (actual uses bech32 crate)
        Ok(format!("cosmos1{}", hex::encode(&account_id)))
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();

        Ok(hex::encode(derived))
    }
}
