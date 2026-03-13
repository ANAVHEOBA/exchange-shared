use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct DashDerivation;

impl BlockchainDerivation for DashDerivation {
    fn coin_type(&self) -> u32 {
        5
    }
    
    fn name(&self) -> &'static str {
        "Dash"
    }
    
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Dash uses BIP44 path: m/44'/5'/0'/0/{index}
        // Coin type: 5
        // Address format: X-prefixed (P2PKH) or 7-prefixed (P2SH)
        
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
        
        let path: DerivationPath = format!("m/44'/{}'/0'/0/{}", 5, index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;
        
        let child = root.derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;
        
        let public_key = child.to_priv().public_key(&secp);
        let address = bitcoin::Address::p2pkh(&public_key, Network::Bitcoin);
        
        // Dash addresses start with 'X' instead of '1'
        let btc_addr = address.to_string();
        let dash_addr = format!("X{}", &btc_addr[1..]);
        
        Ok(dash_addr)
    }
}
