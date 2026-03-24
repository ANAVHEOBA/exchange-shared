use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct FluxDerivation;

impl BlockchainDerivation for FluxDerivation {
    fn coin_type(&self) -> u32 {
        19167
    }

    fn name(&self) -> &'static str {
        "Flux (ZelCash)"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Flux (formerly ZelCash) is a Bitcoin-based blockchain
        // Uses BIP44 path: m/44'/19167'/0'/0/{index}
        // Address format: t1-prefixed (similar to Zcash transparent)

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

        let path: DerivationPath = format!("m/44'/{}'/0'/0/{}", 19167, index)
            .parse()
            .map_err(|e| format!("Invalid derivation path: {}", e))?;

        let child = root
            .derive_priv(&secp, &path)
            .map_err(|e| format!("Failed to derive child key: {}", e))?;

        let public_key = child.to_priv().public_key(&secp);
        let address = bitcoin::Address::p2pkh(&public_key, Network::Bitcoin);

        // Flux addresses start with 't1' instead of '1'
        let btc_addr = address.to_string();
        let flux_addr = format!("t1{}", &btc_addr[1..]);

        Ok(flux_addr)
    }
}
