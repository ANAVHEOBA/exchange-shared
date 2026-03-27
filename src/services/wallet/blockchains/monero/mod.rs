use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct MoneroDerivation;

impl BlockchainDerivation for MoneroDerivation {
    fn coin_type(&self) -> u32 {
        128
    }

    fn name(&self) -> &'static str {
        "Monero"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use curve25519_dalek::scalar::Scalar;
        use monero::{Address, Network, PrivateKey, PublicKey};
        use sha2::{Digest, Sha256};
        use sha3::Keccak256;

        // Parse BIP39 mnemonic
        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed_bytes = mnemonic.to_seed("");

        // Derive spend key from BIP39 seed + index using SHA256
        let mut hasher = Sha256::new();
        hasher.update(&seed_bytes);
        hasher.update(&index.to_le_bytes());
        let spend_seed = hasher.finalize();

        // Convert to scalar and reduce modulo l (Ed25519 curve order)
        let spend_scalar = Scalar::from_bytes_mod_order(spend_seed.into());
        let spend_key_bytes: [u8; 32] = spend_scalar.to_bytes();

        // Create Monero private spend key
        let private_spend = PrivateKey::from_slice(&spend_key_bytes)
            .map_err(|e| format!("Invalid spend key: {}", e))?;

        // Derive view key from spend key using Keccak256 (Monero convention)
        let mut hasher = Keccak256::new();
        hasher.update(&spend_key_bytes);
        let view_key_hash = hasher.finalize();

        // Reduce view key modulo l
        let view_scalar = Scalar::from_bytes_mod_order(view_key_hash.into());
        let view_key_bytes: [u8; 32] = view_scalar.to_bytes();

        // Create Monero private view key
        let private_view = PrivateKey::from_slice(&view_key_bytes)
            .map_err(|e| format!("Invalid view key: {}", e))?;

        // Derive public keys from private keys
        let public_spend = PublicKey::from_private_key(&private_spend);
        let public_view = PublicKey::from_private_key(&private_view);

        // Create Monero address from public keys
        let address = Address::standard(Network::Mainnet, public_spend, public_view);

        Ok(address.to_string())
    }
}
