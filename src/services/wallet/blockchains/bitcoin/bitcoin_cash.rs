use crate::services::wallet::blockchains::encoding::{cashaddr_encode, hash160};
use crate::services::wallet::blockchains::traits::{is_valid_seed_phrase, BlockchainDerivation};
use bip39::{Language, Mnemonic};
use coins_bip32::path::DerivationPath;
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use std::str::FromStr;

pub struct BitcoinCash;

impl BlockchainDerivation for BitcoinCash {
    fn coin_type(&self) -> u32 {
        145
    }

    fn name(&self) -> &'static str {
        "Bitcoin Cash"
    }

    fn derive_address(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let path_str = format!("m/44'/145'/0'/0/{}", index);
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
        let pub_bytes_compressed = public_key.serialize();

        let account_id = hash160(&pub_bytes_compressed);
        cashaddr_encode("bitcoincash", 0, &account_id)
    }

    fn derive_private_key(&self, seed_phrase: &str, index: u32) -> Result<String, String> {
        if !is_valid_seed_phrase(seed_phrase) {
            return Err("Invalid seed phrase".to_string());
        }

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed_phrase)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let path_str = format!("m/44'/145'/0'/0/{}", index);
        let derivation_path = DerivationPath::from_str(&path_str)
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let key = coins_bip32::xkeys::XPriv::root_from_seed(&seed, None)
            .map_err(|e| format!("Failed to create root key: {}", e))?
            .derive_path(&derivation_path)
            .map_err(|e| format!("Failed to derive path: {}", e))?;

        let signing_key: &coins_bip32::prelude::SigningKey = key.as_ref();
        let priv_bytes = signing_key.to_bytes();

        Ok(hex::encode(priv_bytes))
    }
}
