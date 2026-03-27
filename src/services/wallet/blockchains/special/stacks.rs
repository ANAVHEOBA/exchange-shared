use crate::services::wallet::blockchains::encoding::{c32check_encode, hash160};
use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct StacksDerivation;

impl BlockchainDerivation for StacksDerivation {
    fn coin_type(&self) -> u32 {
        5757
    }

    fn name(&self) -> &'static str {
        "Stacks"
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
        let account_id = hash160(&pub_bytes);

        // Mainnet single-sig address version is 22, which encodes to the SP prefix.
        c32check_encode(22, &account_id)
    }
}
