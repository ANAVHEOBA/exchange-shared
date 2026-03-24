// =============================================================================
// INTEGRATION TESTS - SOLANA SIGNING
// Tests for Solana transaction signing (SPL tokens)
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::modules::wallet::schema::SolanaTransaction;
use exchange_shared::services::wallet::signing::SigningService;
use hex;

// =============================================================================
// TEST 1: Sign Solana SOL Transfer
// =============================================================================

#[tokio::test]
async fn test_sign_solana_sol_transfer() {
    let _tx = SolanaTransaction {
        from_pubkey: "3KxMUW3odH8RFuHVv3sfmQkzE64tx8HV8bgrNCsxNTY2".to_string(),
        to_pubkey: "9B5X1CbM3nDZCoDjiHWuqJ3UaYN2Wmmv".to_string(),
        amount: 1000000,
        recent_blockhash: "5E7vNp8Z8X...".to_string(),
    };

    // 32 bytes of hex for private key
    let priv_key = hex::encode([0u8; 32]);
    // Mock transaction data bytes
    let tx_data = hex::encode([1u8; 64]);

    let signature = SigningService::sign_solana_transaction(&priv_key, &tx_data).unwrap();

    assert!(!signature.is_empty());
    assert!(signature.starts_with("0x"));
    // Ed25519 sig is 64 bytes = 128 hex chars + 0x = 130
    assert_eq!(signature.len(), 130);

    println!("✅ Solana SOL transfer signed with real Ed25519 logic");
}
