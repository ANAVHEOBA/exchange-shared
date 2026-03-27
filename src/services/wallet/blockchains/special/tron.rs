use crate::services::wallet::blockchains::encoding::base58check_encode;
use crate::services::wallet::blockchains::traits::BlockchainDerivation;
use bip39::{Language, Mnemonic};
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use sha2::{Digest, Sha256};
use sha3::Keccak256;

pub struct TronDerivation;

impl BlockchainDerivation for TronDerivation {
    fn coin_type(&self) -> u32 {
        195
    }

    fn name(&self) -> &'static str {
        "TRON"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        let secret_key = derive_secret_key(seed, index)?;
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize_uncompressed();

        // Hash public key with Keccak256
        let mut hasher = Keccak256::new();
        hasher.update(&pub_bytes[1..]); // Skip 0x04 prefix
        let hash = hasher.finalize();

        // Take last 20 bytes and add 0x41 prefix
        Ok(base58check_encode(&[0x41], &hash[12..]))
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        Ok(hex::encode(derive_secret_key(seed, index)?.secret_bytes()))
    }
}

fn derive_secret_key(seed: &str, index: u32) -> Result<SecretKey, String> {
    let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
        .map_err(|e| format!("Invalid mnemonic: {}", e))?;
    let seed = mnemonic.to_seed("");

    let mut hasher = Sha256::new();
    hasher.update(&seed);
    hasher.update(&index.to_le_bytes());
    let derived = hasher.finalize();

    SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))
}
