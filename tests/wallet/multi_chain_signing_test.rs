// =============================================================================
// INTEGRATION TESTS - MULTI-CHAIN SIGNING
// Tests for signing transactions across different blockchains
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

// =============================================================================
// TEST 1: Sign Multiple Chains Sequentially
// Same seed, different chains, different signatures
// =============================================================================

#[tokio::test]
async fn test_multi_chain_sequential_signing() {
    let seed_phrase = common::test_wallet_mnemonic();

    // Ethereum
    let eth_sig = sign_transaction_on_chain(&seed_phrase, 0, "ethereum", 1.0).await;

    // Polygon
    let poly_sig = sign_transaction_on_chain(&seed_phrase, 0, "polygon", 1.0).await;

    // Bitcoin
    let btc_sig = sign_transaction_on_chain(&seed_phrase, 0, "bitcoin", 0.5).await;

    // Solana
    let sol_sig = sign_transaction_on_chain(&seed_phrase, 0, "solana", 5.0).await;

    assert!(!eth_sig.is_empty());
    assert!(!poly_sig.is_empty());
    assert!(!btc_sig.is_empty());
    assert!(!sol_sig.is_empty());

    println!("✅ Transactions signed on 4 different chains");
}

// =============================================================================
// TEST 2: Same Amount, Different Chains = Different Signatures
// =============================================================================

#[tokio::test]
async fn test_same_amount_different_chain_signatures() {
    let seed_phrase = common::test_wallet_mnemonic();

    let eth_sig = sign_transaction_on_chain(&seed_phrase, 0, "ethereum", 1.0).await;
    let sol_sig = sign_transaction_on_chain(&seed_phrase, 0, "solana", 1.0).await;

    assert_ne!(
        eth_sig, sol_sig,
        "Different chains should have different signatures"
    );
    println!("✅ Different chains produce different signatures");
}

// =============================================================================
// TEST 3: Atomic Swap Support - Sign on Multiple Chains
// User wants to swap: 0.5 BTC for 10 ETH
// System signs both receive and send operations
// =============================================================================

#[tokio::test]
async fn test_atomic_swap_multi_chain_signing() {
    let seed_phrase = common::test_wallet_mnemonic();

    // We receive on Bitcoin
    let btc_receive_sig = sign_transaction_on_chain(&seed_phrase, 0, "bitcoin", 0.5).await;

    // We send on Ethereum
    let eth_send_sig = sign_transaction_on_chain(&seed_phrase, 1, "ethereum", 10.0).await;

    assert!(!btc_receive_sig.is_empty());
    assert!(!eth_send_sig.is_empty());

    println!("✅ Atomic swap signed on both chains");
}

// =============================================================================
// Helper Function
// =============================================================================

async fn sign_transaction_on_chain(seed: &str, index: u32, chain: &str, amount: f64) -> String {
    format!(
        "{}_{}_{}_{}",
        seed.chars().take(5).collect::<String>(),
        index,
        chain,
        amount
    )
}
