use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct ThetaDerivation;

impl BlockchainDerivation for ThetaDerivation {
    fn coin_type(&self) -> u32 {
        500
    }

    fn name(&self) -> &'static str {
        "Theta"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};
        use sha3::Keccak256;

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
        let pub_bytes = public_key.serialize_uncompressed();

        // Theta uses Ethereum-style address
        let mut hasher = Keccak256::new();
        hasher.update(&pub_bytes[1..]); // Skip 0x04 prefix
        let hash = hasher.finalize();
        let address_bytes = &hash[12..]; // Last 20 bytes

        Ok(format!("0x{}", hex::encode(address_bytes)))
    }
}
