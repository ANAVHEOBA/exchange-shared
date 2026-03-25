use crate::services::wallet::blockchains::traits::BlockchainDerivation;
use crate::services::wallet::blockchains::{
    encoding::base58check_encode_with_alphabet, encoding::hash160,
};

pub struct XrpDerivation;

impl BlockchainDerivation for XrpDerivation {
    fn coin_type(&self) -> u32 {
        144
    }

    fn name(&self) -> &'static str {
        "XRP"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        // Simple derivation
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

        Ok(base58check_encode_with_alphabet(
            &[0x00],
            &account_id,
            bs58::Alphabet::RIPPLE,
        ))
    }
}
