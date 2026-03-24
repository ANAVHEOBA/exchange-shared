use crate::services::wallet::blockchains::traits::{is_valid_seed_phrase, BlockchainDerivation};
use bip39::{Language, Mnemonic};
use coins_bip32::path::DerivationPath;
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use sha3::{Digest, Keccak256};
use std::str::FromStr;

/// Generic EVM-compatible chain (Ethereum, Polygon, Arbitrum, etc.)
/// All use coin_type 60 and same address derivation
pub struct EvmChain {
    name: &'static str,
}

impl EvmChain {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub fn ethereum() -> Self {
        Self::new("Ethereum")
    }

    pub fn polygon() -> Self {
        Self::new("Polygon")
    }

    pub fn arbitrum() -> Self {
        Self::new("Arbitrum")
    }

    pub fn optimism() -> Self {
        Self::new("Optimism")
    }

    pub fn base() -> Self {
        Self::new("Base")
    }
}

impl BlockchainDerivation for EvmChain {
    fn coin_type(&self) -> u32 {
        60 // All EVM chains use coin_type 60
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn derive_address(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let path_str = format!("m/44'/60'/0'/0/{}", index);
        let derivation_path = DerivationPath::from_str(&path_str)
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let key = coins_bip32::xkeys::XPriv::root_from_seed(&seed, None)
            .map_err(|e| format!("Failed to create root key: {}", e))?
            .derive_path(&derivation_path)
            .map_err(|e| format!("Failed to derive path: {}", e))?;

        let signing_key: &coins_bip32::prelude::SigningKey = key.as_ref();
        let priv_bytes = signing_key.to_bytes();
        let secret_key =
            SecretKey::from_slice(&priv_bytes).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes_uncompressed = public_key.serialize_uncompressed();

        // Ethereum address = last 20 bytes of Keccak256(public_key)
        let mut hasher = Keccak256::new();
        hasher.update(&pub_bytes_uncompressed[1..]); // Skip the 0x04 prefix
        let hash = hasher.finalize();
        let address_bytes = &hash[12..]; // Last 20 bytes

        Ok(format!("0x{}", hex::encode(address_bytes)))
    }

    fn derive_private_key(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let path_str = format!("m/44'/60'/0'/0/{}", index);
        let derivation_path = DerivationPath::from_str(&path_str)
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let key = coins_bip32::xkeys::XPriv::root_from_seed(&seed, None)
            .map_err(|e| format!("Failed to create root key: {}", e))?
            .derive_path(&derivation_path)
            .map_err(|e| format!("Failed to derive path: {}", e))?;

        let signing_key: &coins_bip32::prelude::SigningKey = key.as_ref();
        let priv_bytes = signing_key.to_bytes();

        Ok(format!("0x{}", hex::encode(priv_bytes)))
    }
}
