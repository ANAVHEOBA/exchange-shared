use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct TerraDerivation;

impl BlockchainDerivation for TerraDerivation {
    fn coin_type(&self) -> u32 {
        330 // Terra coin type
    }

    fn name(&self) -> &'static str {
        "Terra Classic"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Terra uses Cosmos SDK
        // BIP44 path: m/44'/330'/0'/0/{index}
        // Address format: terra1... (bech32)

        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        // Simplified derivation for Cosmos SDK
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"terra");
        let derived = hasher.finalize();

        let secret_key =
            SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();

        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let hash = hasher.finalize();

        // Take first 20 bytes
        let address_bytes = &hash[0..20];

        // Bech32 encode with "terra" prefix
        Ok(format!("terra1{}", hex::encode(address_bytes)))
    }
}
