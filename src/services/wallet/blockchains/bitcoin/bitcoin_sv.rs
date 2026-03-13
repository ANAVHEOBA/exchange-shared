use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct BitcoinSvDerivation;

impl BlockchainDerivation for BitcoinSvDerivation {
    fn coin_type(&self) -> u32 {
        236 // Bitcoin SV coin type
    }
    
    fn name(&self) -> &'static str {
        "Bitcoin SV"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Bitcoin SV uses same address format as Bitcoin
        // BIP44 path: m/44'/236'/0'/0/{index}
        
        use bip39::{Mnemonic, Language};
        use bitcoin::secp256k1::Secp256k1;
        use bitcoin::bip32::{Xpriv, DerivationPath};
        use bitcoin::Network;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        
        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();
        
        let root = Xpriv::new_master(Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;
        
        let path: DerivationPath = format!("m/44'/{}'/0'/0/{}", 236, index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;
        
        let child = root.derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;
        
        let public_key = child.to_priv().public_key(&secp);
        let address = bitcoin::Address::p2pkh(&public_key, Network::Bitcoin);
        
        Ok(address.to_string())
    }
        
    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Mnemonic, Language};
        use bitcoin::secp256k1::Secp256k1;
        use bitcoin::bip32::{Xpriv, DerivationPath};
        use bitcoin::Network;

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        
        let seed = mnemonic.to_seed("");
        let secp = Secp256k1::new();
        
        let root = Xpriv::new_master(Network::Bitcoin, &seed)
            .map_err(|e| format!("Failed to create master key: {}", e))?;
        
        let path: DerivationPath = format!("m/44'/236'/0'/0/{}", index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;
        
        let child = root.derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;
        
        let priv_key = child.to_priv();
        Ok(hex::encode(priv_key.to_bytes()))
    }
}