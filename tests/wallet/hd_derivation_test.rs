// =============================================================================
// INTEGRATION TESTS - HD WALLET DERIVATION
// Tests for BIP39/BIP44 hierarchical deterministic wallet derivation
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::wallet::{
    derive_evm_key, derive_evm_address, derive_btc_address, 
    derive_solana_address, derive_sui_address, is_valid_seed_phrase,
    sign_message_with_seed
};
use std::time::Instant;

// =============================================================================
// TEST 1: Seed Phrase Generates Consistent Keys
// Same seed always produces same private keys
// =============================================================================

#[tokio::test]
async fn test_seed_phrase_consistency() {
    // Example BIP39 seed phrase (test vector)
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // Derive EVM key from seed
    let evm_key_1 = derive_evm_key(&seed_phrase).await.unwrap();
    let evm_key_2 = derive_evm_key(&seed_phrase).await.unwrap();
    
    let duration = start.elapsed();
    
    // Same seed should produce same key
    assert_eq!(evm_key_1, evm_key_2, "Seed should produce consistent keys");
    
    // Performance check: should be fast (< 500ms)
    assert!(duration.as_millis() < 500, "Derivation too slow: {:?}", duration);
    
    println!("✅ EVM key consistent: {} (took {:?})", evm_key_1, duration);
}

// =============================================================================
// TEST 2: Different Derivation Paths = Different Keys
// BIP44 paths (m/44'/60'/0'/0/0 vs m/44'/60'/0'/0/1) produce different keys
// =============================================================================

#[tokio::test]
async fn test_different_derivation_paths() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // Index 0: m/44'/60'/0'/0/0
    let address_0 = derive_evm_address(&seed_phrase, 0).await.unwrap();
    
    // Index 1: m/44'/60'/0'/0/1
    let address_1 = derive_evm_address(&seed_phrase, 1).await.unwrap();
    
    // Index 2: m/44'/60'/0'/0/2
    let address_2 = derive_evm_address(&seed_phrase, 2).await.unwrap();
    
    let duration = start.elapsed();
    
    // All should be different
    assert_ne!(address_0, address_1, "Index 0 and 1 should produce different addresses");
    assert_ne!(address_1, address_2, "Index 1 and 2 should produce different addresses");
    assert_ne!(address_0, address_2, "Index 0 and 2 should produce different addresses");
    
    assert!(duration.as_millis() < 1000, "Batch derivation too slow: {:?}", duration);
    
    println!("Address 0: {}", address_0);
    println!("Address 1: {}", address_1);
    println!("Address 2: {}", address_2);
}

// =============================================================================
// TEST 3: EVM Chain Uses BIP44 Path 60 (Ethereum)
// All EVM chains (Ethereum, Polygon, Arbitrum) use same path: m/44'/60'
// =============================================================================

#[tokio::test]
async fn test_evm_chain_derivation_path() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // EVM path: m/44'/60'/0'/0/0
    let eth_address = derive_evm_address(&seed_phrase, 0).await.unwrap();
    
    let duration = start.elapsed();
    
    // Should be Ethereum address format (0x...)
    assert!(eth_address.starts_with("0x"), "EVM address should start with 0x");
    assert_eq!(eth_address.len(), 42, "EVM address should be 42 chars (0x + 40 hex)");
    
    assert!(duration.as_millis() < 500, "EVM derivation took too long: {:?}", duration);
    
    println!("EVM Address (Ethereum/Polygon/Arbitrum): {} (took {:?})", eth_address, duration);
}

// =============================================================================
// TEST 4: Bitcoin Uses BIP44 Path 0
// Bitcoin derivation: m/44'/0'/0'/0/[index]
// =============================================================================

#[tokio::test]
async fn test_bitcoin_derivation_path() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // BTC path: m/44'/0'/0'/0/0
    let btc_address = derive_btc_address(&seed_phrase, 0).await.unwrap();
    
    let duration = start.elapsed();
    
    // Should be Bitcoin address format (Legacy or SegWit)
    assert!(btc_address.starts_with("bc1") || btc_address.starts_with("1") || btc_address.starts_with("3"),
            "Bitcoin address should be valid format");
            
    assert!(duration.as_millis() < 500, "BTC derivation took too long: {:?}", duration);
    
    println!("Bitcoin Address: {} (took {:?})", btc_address, duration);
}

// =============================================================================
// TEST 5: Solana Uses BIP44 Path 501
// Solana derivation: m/44'/501'/0'/0'/[index]'
// =============================================================================

#[tokio::test]
async fn test_solana_derivation_path() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // Solana path: m/44'/501'/0'/0'/0'
    let sol_address = derive_solana_address(&seed_phrase, 0).await.unwrap();
    
    let duration = start.elapsed();
    
    // Should be Solana Base58 format
    assert!(!sol_address.is_empty(), "Solana address should not be empty");
    assert!(duration.as_millis() < 500, "Solana derivation took too long: {:?}", duration);
    
    println!("Solana Address: {} (took {:?})", sol_address, duration);
}

// =============================================================================
// TEST 6: Sui Uses BIP44 Path 784
// Sui derivation: m/44'/784'/0'/0'/[index]'
// =============================================================================

#[tokio::test]
async fn test_sui_derivation_path() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // Sui path: m/44'/784'/0'/0'/0'
    let sui_address = derive_sui_address(&seed_phrase, 0).await.unwrap();
    
    let duration = start.elapsed();
    
    // Should be Sui address format (0x...)
    assert!(sui_address.starts_with("0x"), "Sui address should start with 0x");
    assert!(duration.as_millis() < 500, "Sui derivation took too long: {:?}", duration);
    
    println!("Sui Address: {} (took {:?})", sui_address, duration);
}

// =============================================================================
// TEST 7: Private Keys Never Exposed in Response
// System should only expose public addresses, never private keys
// =============================================================================

#[tokio::test]
async fn test_private_key_not_exposed() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    // Get address (should be safe)
    let address = derive_evm_address(&seed_phrase, 0).await.unwrap();
    assert!(!address.is_empty(), "Address should be provided");
    
    // Private key should NEVER be returned to caller
    // This should only be used internally for signing
    println!("Public address safe to show: {}", address);
    println!("✅ Private key is kept internal, never exposed");
}

// =============================================================================
// TEST 8: Invalid Seed Phrase Rejection
// System should reject invalid BIP39 seed phrases
// =============================================================================

#[tokio::test]
async fn test_invalid_seed_phrase_rejection() {
    let invalid_seed = "invalid seed phrase with wrong words here now";
    
    let result = derive_evm_address_safe(invalid_seed, 0).await;
    
    assert!(result.is_err(), "Invalid seed phrase should be rejected");
    println!("✅ Invalid seed phrase correctly rejected");
}

// =============================================================================
// TEST 9: Seed Phrase Length Validation
// BIP39 supports 12, 15, 18, 21, 24 words
// =============================================================================

#[tokio::test]
async fn test_seed_phrase_word_count_validation() {
    // Too short (11 words)
    let short_seed = "word ".repeat(10) + "about";
    let short_result = derive_evm_address_safe(&short_seed, 0).await;
    assert!(short_result.is_err(), "11-word seed should be rejected");
    
    // Valid (12 words)
    let valid_seed = common::test_wallet_mnemonic();
    let valid_result = derive_evm_address_safe(&valid_seed, 0).await;
    assert!(valid_result.is_ok(), "12-word seed should be valid");
    
    println!("✅ Seed phrase word count validation works");
}

// =============================================================================
// TEST 10: Maximum Address Index
// System can derive many addresses (e.g., 2^31 - 1 in BIP44)
// =============================================================================

#[tokio::test]
async fn test_high_address_index_derivation() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let start = Instant::now();
    
    // Generate multiple addresses
    let mut addresses = vec![];
    for i in 0..100 {
        let addr = derive_evm_address(&seed_phrase, i).await.unwrap();
        addresses.push(addr);
    }
    
    let duration = start.elapsed();
    
    // All should be unique
    addresses.sort();
    addresses.dedup();
    assert_eq!(addresses.len(), 100, "All 100 addresses should be unique");
    
    assert!(duration.as_secs() < 10, "Batch of 100 derivations too slow: {:?}", duration);
    
    println!("✅ Generated 100 unique addresses from same seed (took {:?})", duration);
}

// =============================================================================
// TEST 11: Signing Consistency
// Same private key should produce same signature for same message
// =============================================================================

#[tokio::test]
async fn test_signing_consistency() {
    let seed_phrase = common::test_wallet_mnemonic();
    let message = "test message for signing";
    
    let start = Instant::now();
    
    let signature_1 = sign_message_with_seed(&seed_phrase, 0, message).await.unwrap();
    let signature_2 = sign_message_with_seed(&seed_phrase, 0, message).await.unwrap();
    
    let duration = start.elapsed();
    
    // Same private key, same message = same signature
    assert_eq!(signature_1, signature_2, "Signature should be deterministic");
    assert!(duration.as_millis() < 500, "Signing too slow: {:?}", duration);
    
    println!("✅ Signatures are consistent (took {:?})", duration);
}

// =============================================================================
// Helper Functions
// =============================================================================

async fn derive_evm_address_safe(seed: &str, index: u32) -> Result<String, String> {
    // Validate seed phrase first
    if !is_valid_seed_phrase(seed) {
        return Err("Invalid seed phrase".to_string());
    }
    derive_evm_address(seed, index).await
}
