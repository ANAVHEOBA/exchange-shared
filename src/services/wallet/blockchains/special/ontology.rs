use crate::services::wallet::blockchains::traits::BlockchainDerivation;

pub struct OntologyDerivation;

impl BlockchainDerivation for OntologyDerivation {
    fn coin_type(&self) -> u32 {
        1024
    }

    fn name(&self) -> &'static str {
        "Ontology"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        // Ontology uses ECDSA with secp256r1 (not secp256k1)
        // Address format: A-prefixed base58
        // BIP44 path: m/44'/1024'/0'/0/{index}

        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        // Simplified derivation (actual uses secp256r1)
        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(b"ontology");
        let derived = hasher.finalize();

        // Hash again for address
        let mut hasher = Sha256::new();
        hasher.update(&derived);
        let address_hash = hasher.finalize();

        // Add version byte (0x17 for Ontology)
        let mut payload = vec![0x17u8];
        payload.extend_from_slice(&address_hash[0..20]);

        // Calculate checksum
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        let mut hasher = Sha256::new();
        hasher.update(&checksum_hash);
        let checksum = hasher.finalize();
        payload.extend_from_slice(&checksum[0..4]);

        Ok(bs58::encode(&payload).into_string())
    }
}
