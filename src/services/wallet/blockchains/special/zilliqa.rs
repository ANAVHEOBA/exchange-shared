use crate::services::wallet::blockchains::encoding::bech32_encode;
use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct ZilliqaDerivation;

impl BlockchainDerivation for ZilliqaDerivation {
    fn coin_type(&self) -> u32 {
        313
    }

    fn name(&self) -> &'static str {
        "Zilliqa"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        let derived = hasher.finalize();

        let secret_key =
            SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();

        // Hash public key
        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let hash = hasher.finalize();

        // Take last 20 bytes for address
        let address_bytes = &hash[12..];

        bech32_encode("zil", address_bytes)
    }
}
