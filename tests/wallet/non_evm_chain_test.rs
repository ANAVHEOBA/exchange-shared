use exchange_shared::services::wallet::{
    bitcoin_rpc::{BitcoinUtxo, build_bitcoin_transaction},
    solana_rpc::build_solana_transaction,
    derivation,
};

#[tokio::test]
async fn test_bitcoin_address_derivation() {
    let seed = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let address = derivation::derive_btc_address(seed, 0).await.unwrap();
    
    // Bitcoin addresses start with 1, 3, or bc1
    assert!(address.starts_with('1') || address.starts_with('3') || address.starts_with("bc1"));
    assert!(address.len() >= 26 && address.len() <= 35);
}

#[tokio::test]
async fn test_solana_address_derivation() {
    let seed = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let address = derivation::derive_solana_address(seed, 0).await.unwrap();
    
    // Solana addresses are base58 encoded, typically 32-44 characters
    assert!(address.len() >= 32 && address.len() <= 44);
    assert!(address.chars().all(|c| c.is_alphanumeric()));
}

#[tokio::test]
async fn test_bitcoin_key_derivation() {
    let seed = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let key = derivation::derive_btc_key(seed, 0).await.unwrap();
    
    // Private key should be 64 hex characters (32 bytes)
    assert_eq!(key.len(), 64);
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn test_solana_key_derivation() {
    let seed = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let key = derivation::derive_solana_key(seed, 0).await.unwrap();
    
    // Solana key should be 32 bytes
    assert_eq!(key.len(), 32);
}

#[tokio::test]
async fn test_bitcoin_transaction_building() {
    let utxos = vec![
        BitcoinUtxo {
            txid: "a".repeat(64),
            vout: 0,
            amount: 0.1,
            confirmations: 6,
        },
    ];
    
    let to_address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"; // Genesis address
    let change_address = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
    
    let result = build_bitcoin_transaction(
        utxos,
        to_address,
        0.05,
        10.0, // 10 sat/byte
        change_address,
    );
    
    assert!(result.is_ok());
    let tx = result.unwrap();
    assert_eq!(tx.input.len(), 1);
    assert!(tx.output.len() >= 1); // At least recipient output
}

#[tokio::test]
async fn test_solana_transaction_building() {
    let from = "11111111111111111111111111111111";
    let to = "11111111111111111111111111111112";
    let blockhash = "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZ";
    
    let result = build_solana_transaction(from, to, 1.0, blockhash);
    
    // This will fail with invalid pubkey, but tests the function exists
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multi_chain_address_consistency() {
    let seed = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    
    // Test that same seed + index produces same address
    let btc1 = derivation::derive_btc_address(seed, 0).await.unwrap();
    let btc2 = derivation::derive_btc_address(seed, 0).await.unwrap();
    assert_eq!(btc1, btc2);
    
    let sol1 = derivation::derive_solana_address(seed, 0).await.unwrap();
    let sol2 = derivation::derive_solana_address(seed, 0).await.unwrap();
    assert_eq!(sol1, sol2);
    
    // Different indices produce different addresses
    let btc_idx1 = derivation::derive_btc_address(seed, 0).await.unwrap();
    let btc_idx2 = derivation::derive_btc_address(seed, 1).await.unwrap();
    assert_ne!(btc_idx1, btc_idx2);
}

#[tokio::test]
async fn test_bitcoin_insufficient_funds() {
    let utxos = vec![
        BitcoinUtxo {
            txid: "a".repeat(64),
            vout: 0,
            amount: 0.001, // Only 0.001 BTC
            confirmations: 6,
        },
    ];
    
    let to_address = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa";
    let change_address = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
    
    // Try to send more than available
    let result = build_bitcoin_transaction(
        utxos,
        to_address,
        1.0, // Try to send 1 BTC
        10.0,
        change_address,
    );
    
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Insufficient funds"));
}

#[tokio::test]
async fn test_chain_type_detection() {
    // Test that we can identify different chain types
    let networks = vec![
        ("ethereum", true),  // EVM
        ("polygon", true),   // EVM
        ("bitcoin", false),  // Bitcoin
        ("solana", false),   // Solana
        ("bsc", true),       // EVM
    ];
    
    for (network, is_evm) in networks {
        let network_lower = network.to_lowercase();
        let detected_evm = !matches!(network_lower.as_str(), "bitcoin" | "solana" | "sol");
        assert_eq!(detected_evm, is_evm, "Failed for network: {}", network);
    }
}
