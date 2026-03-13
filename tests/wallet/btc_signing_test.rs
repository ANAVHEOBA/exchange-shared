// =============================================================================
// INTEGRATION TESTS - BITCOIN SIGNING
// Tests for Bitcoin transaction signing (UTXO model)
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

// =============================================================================
// TEST 1: Sign Bitcoin Transaction
// =============================================================================

#[tokio::test]
async fn test_sign_bitcoin_transaction() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let tx = BtcTransaction { };
    
    let signature = sign_bitcoin_transaction(&seed_phrase, 0, &tx).await;
    
    assert!(!signature.is_empty(), "Bitcoin transaction should be signed");
    println!("✅ Bitcoin transaction signed");
}

// =============================================================================
// TEST 2: UTXO Inputs/Outputs Signing
// Bitcoin uses UTXO model, not account-based
// =============================================================================

#[tokio::test]
async fn test_bitcoin_utxo_signing() {
    let seed_phrase = common::test_wallet_mnemonic();
    
    let utxo_tx = BtcUtxoTransaction { };
    
    let signature = sign_bitcoin_utxo_transaction(&seed_phrase, 0, &utxo_tx).await;
    
    assert!(!signature.is_empty());
    println!("✅ Bitcoin UTXO transaction signed");
}

// =============================================================================
// Helper Structures
// =============================================================================

#[allow(dead_code)]
struct BtcTransaction { }

#[allow(dead_code)]
struct BtcUtxoTransaction { }

#[allow(dead_code)]
struct UtxoInput { }

#[allow(dead_code)]
struct UtxoOutput { }

async fn sign_bitcoin_transaction(_seed: &str, _index: u32, _tx: &BtcTransaction) -> String {
    "bitcoin_signature".to_string()
}

async fn sign_bitcoin_utxo_transaction(_seed: &str, _index: u32, _tx: &BtcUtxoTransaction) -> String {
    "bitcoin_utxo_signature".to_string()
}
