// =============================================================================
// INTEGRATION TESTS - EVM SIGNING
// Tests for Ethereum, Polygon, Arbitrum transaction signing
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::modules::wallet::schema::EvmTransaction;
use exchange_shared::services::wallet::derivation::derive_evm_key;
use exchange_shared::services::wallet::signing::SigningService;

// =============================================================================
// TEST 1: Sign EVM Transaction
// Test signing a transaction for EVM chains (Ethereum, Polygon, Arbitrum)
// =============================================================================

#[tokio::test]
async fn test_sign_evm_transaction() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    // 1. Derive real key
    let priv_key = derive_evm_key(&seed_phrase, 0).await.unwrap();

    // 2. Real transaction data
    let tx = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 7.1993,
        token: "eth".to_string(),
        chain_id: 1, // Ethereum
        nonce: 42,
        gas_price: 50_000_000_000u64,
    };
    
    // 3. Sign transaction
    let signature = SigningService::sign_evm_transaction(&priv_key, &tx).unwrap();
    
    // Signature should be valid format (0x + hex)
    assert!(!signature.is_empty(), "Signature should not be empty");
    assert!(signature.starts_with("0x"), "Signature should be hex string");
    // Standard EVM signature is 65 bytes (130 hex chars) + 0x prefix = 132
    assert_eq!(signature.len(), 132);
    
    println!("✅ EVM transaction signed successfully");
    println!("  Signature: {}", signature);
}

// =============================================================================
// TEST 2: Different Signatures for Different Transactions
// Same key should produce different signatures for different data
// =============================================================================

#[tokio::test]
async fn test_different_txs_different_signatures() {
    let seed_phrase = common::test_wallet_mnemonic();
    let priv_key = derive_evm_key(&seed_phrase, 0).await.unwrap();

    let tx1 = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 1.0,
        token: "eth".to_string(),
        chain_id: 1,
        nonce: 1,
        gas_price: 50_000_000_000,
    };
    
    let tx2 = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 2.0,
        token: "eth".to_string(),
        chain_id: 1,
        nonce: 2,
        gas_price: 50_000_000_000,
    };
    
    let sig1 = SigningService::sign_evm_transaction(&priv_key, &tx1).unwrap();
    let sig2 = SigningService::sign_evm_transaction(&priv_key, &tx2).unwrap();
    
    assert_ne!(sig1, sig2, "Different transactions should have different signatures");
    println!("✅ Different transactions produce different signatures");
}

// =============================================================================
// TEST 3: Polygon Chain Signing
// EVM chains (Polygon, Arbitrum, Optimism) use same key as Ethereum
// =============================================================================

#[tokio::test]
async fn test_polygon_signing_same_key() {
    let seed_phrase = common::test_wallet_mnemonic();
    let priv_key = derive_evm_key(&seed_phrase, 0).await.unwrap();

    // Ethereum signature
    let eth_tx = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 5.0,
        token: "usdc".to_string(),
        chain_id: 1, // Ethereum
        nonce: 1,
        gas_price: 50_000_000_000,
    };
    
    // Polygon signature (same key, different chain_id)
    let poly_tx = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 5.0,
        token: "usdc".to_string(),
        chain_id: 137, // Polygon
        nonce: 1,
        gas_price: 50_000_000_000,
    };
    
    let eth_sig = SigningService::sign_evm_transaction(&priv_key, &eth_tx).unwrap();
    let poly_sig = SigningService::sign_evm_transaction(&priv_key, &poly_tx).unwrap();
    
    // Signatures should be different (different chain_id)
    assert_ne!(eth_sig, poly_sig, "Different chain IDs should produce different signatures");
    
    println!("✅ Polygon and Ethereum can both be signed with same key");
}

// =============================================================================
// TEST 4: Nonce Management in Signing
// Each transaction needs incrementing nonce to prevent replay
// =============================================================================

#[tokio::test]
async fn test_nonce_affects_signature() {
    let seed_phrase = common::test_wallet_mnemonic();
    let priv_key = derive_evm_key(&seed_phrase, 0).await.unwrap();

    let tx1 = EvmTransaction {
        to_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        amount: 1.0,
        token: "eth".to_string(),
        chain_id: 1,
        nonce: 1,
        gas_price: 50_000_000_000,
    };
    
    let mut tx2 = tx1.clone();
    tx2.nonce = 2; // Different nonce
    
    let sig1 = SigningService::sign_evm_transaction(&priv_key, &tx1).unwrap();
    let sig2 = SigningService::sign_evm_transaction(&priv_key, &tx2).unwrap();
    
    assert_ne!(sig1, sig2, "Different nonces should produce different signatures");
    println!("✅ Nonce properly affects signature");
}
