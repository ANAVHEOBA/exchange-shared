use crate::services::wallet::blockchains::encoding::{base58check_encode, hash160};
use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct BitcoinzDerivation;

impl BlockchainDerivation for BitcoinzDerivation {
    fn coin_type(&self) -> u32 {
        177 // BitcoinZ coin type
    }

    fn name(&self) -> &'static str {
        "BitcoinZ"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // BitcoinZ is a Zcash fork
        // BIP44 path: m/44'/177'/0'/0/{index}
        // Address format: t1-prefixed (transparent)

        use bip39::{Language, Mnemonic};
        use bitcoin::bip32::{DerivationPath, Xpriv};
        use bitcoin::secp256k1::Secp256k1;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;

        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();

        let root = Xpriv::new_master(bitcoin::Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;

        let path: DerivationPath = format!("m/44'/{}'/0'/0/{}", 177, index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let child = root
            .derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;

        let public_key = child.to_priv().public_key(&secp);
        let account_id = hash160(&public_key.to_bytes());

        Ok(base58check_encode(&[0x1c, 0xb8], &account_id))
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use bitcoin::bip32::{DerivationPath, Xpriv};
        use bitcoin::secp256k1::Secp256k1;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;

        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();

        let root = Xpriv::new_master(bitcoin::Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;

        let path: DerivationPath = format!("m/44'/177'/0'/0/{}", index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let child = root
            .derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;

        let priv_key = child.to_priv();
        Ok(hex::encode(priv_key.to_bytes()))
    }
}
