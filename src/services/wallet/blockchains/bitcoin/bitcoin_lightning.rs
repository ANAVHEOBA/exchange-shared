use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct BitcoinLightningDerivation;

impl BlockchainDerivation for BitcoinLightningDerivation {
    fn coin_type(&self) -> u32 {
        0 // Same as Bitcoin
    }

    fn name(&self) -> &'static str {
        "Bitcoin Lightning"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Lightning Network uses Bitcoin addresses
        // Lightning invoices are generated separately (BOLT11 format)
        // For wallet derivation, we use standard Bitcoin BIP44

        use bip39::{Language, Mnemonic};
        use bitcoin::bip32::{DerivationPath, Xpriv};
        use bitcoin::secp256k1::Secp256k1;
        use bitcoin::Network;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;

        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();

        let root = Xpriv::new_master(Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;

        let path: DerivationPath = format!("m/44'/0'/0'/0/{}", index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let child = root
            .derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;

        let public_key = child.to_priv().public_key(&secp);

        // p2wpkh requires CompressedPublicKey (wrapper around secp256k1::PublicKey)
        use bitcoin::key::CompressedPublicKey;
        let compressed_pubkey = CompressedPublicKey(public_key.inner);
        let address = bitcoin::Address::p2wpkh(&compressed_pubkey, Network::Bitcoin);

        Ok(address.to_string())
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use bitcoin::bip32::{DerivationPath, Xpriv};
        use bitcoin::secp256k1::Secp256k1;
        use bitcoin::Network;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;

        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();

        let root = Xpriv::new_master(Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;

        let path: DerivationPath = format!("m/44'/0'/0'/0/{}", index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let child = root
            .derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;

        let priv_key = child.to_priv();
        Ok(hex::encode(priv_key.to_bytes()))
    }
}
