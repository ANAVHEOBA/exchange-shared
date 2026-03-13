use crate::services::wallet::blockchains::traits::{BlockchainDerivation, is_valid_seed_phrase};
use bip39::{Language, Mnemonic};
use coins_bip32::path::DerivationPath;
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use sha2::{Digest, Sha256};
use ripemd::Ripemd160;
use std::str::FromStr;

pub struct Litecoin;

impl BlockchainDerivation for Litecoin {
    fn coin_type(&self) -> u32 {
        2
    }
    
    fn name(&self) -> &'static str {
        "Litecoin"
    }
    
    fn derive_address(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let path_str = format!("m/44'/2'/0'/0/{}", index);
        let derivation_path = DerivationPath::from_str(&path_str)
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let key = coins_bip32::xkeys::XPriv::root_from_seed(&seed, None)
            .map_err(|e| format!("Failed to create root key: {}", e))?
            .derive_path(&derivation_path)
            .map_err(|e| format!("Failed to derive path: {}", e))?;

        let signing_key: &coins_bip32::prelude::SigningKey = key.as_ref();
        let priv_bytes = signing_key.to_bytes();
        let secret_key = SecretKey::from_slice(&priv_bytes)
            .map_err(|e| format!("Invalid secret key: {}", e))?;
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes_compressed = public_key.serialize();

        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes_compressed);
        let sha256_hash = hasher.finalize();

        let mut hasher = Ripemd160::new();
        hasher.update(&sha256_hash);
        let account_id = hasher.finalize();

        // Litecoin mainnet P2PKH prefix: 0x30
        let mut payload = vec![0x30u8];
        payload.extend_from_slice(&account_id);
        let checksum = ripple_checksum(&payload);
        payload.extend_from_slice(&checksum);

        Ok(bs58::encode(&payload).into_string())
    }
}

fn ripple_checksum(data: &[u8]) -> [u8; 4] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let hash1 = hasher.finalize();

    let mut hasher = Sha256::new();
    hasher.update(&hash1);
    let hash2 = hasher.finalize();

    let mut checksum = [0u8; 4];
    checksum.copy_from_slice(&hash2[0..4]);
    checksum
}
