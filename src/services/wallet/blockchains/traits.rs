/// Core trait for blockchain address derivation
/// Each blockchain family implements this trait
pub trait BlockchainDerivation: Send + Sync {
    /// BIP44 coin type for this blockchain
    fn coin_type(&self) -> u32;
    
    /// Derive address from seed phrase and index
    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String>;
    
    /// Validate address format (optional, returns true by default)
    fn validate_address(&self, _address: &str) -> bool {
        true
    }
    
    /// Get blockchain name
    fn name(&self) -> &'static str;
}

/// Helper function to validate seed phrase format
pub fn is_valid_seed_phrase(seed_phrase: &str) -> bool {
    let words: Vec<&str> = seed_phrase.split_whitespace().collect();
    matches!(words.len(), 12 | 15 | 18 | 21 | 24)
}
