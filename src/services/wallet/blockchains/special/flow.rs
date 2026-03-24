use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct FlowDerivation;

impl BlockchainDerivation for FlowDerivation {
    fn coin_type(&self) -> u32 {
        539
    }

    fn name(&self) -> &'static str {
        "Flow"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};
        use sha3::Sha3_256;

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

        // Flow address = SHA3-256(public_key)[0..8]
        let mut hasher = Sha3_256::new();
        hasher.update(&pub_bytes);
        let hash = hasher.finalize();

        Ok(format!("0x{}", hex::encode(&hash[0..8])))
    }
}
