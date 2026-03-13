/// Tier 2 Phase 1 Blockchain Tests: Litecoin, Dogecoin, Bitcoin Cash
/// 
/// Strict production-ready tests with official specifications verification
/// Tests: Determinism, Uniqueness, Format Validation, Dispatcher Aliases, Invalid Input

use exchange_shared::services::wallet::derivation::derive_address;
use std::collections::HashSet;

// =========================================
// LITECOIN TESTS
// =========================================

#[tokio::test]
async fn test_litecoin_address_generation() {
    let result = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 0).await;
    assert!(result.is_ok(), "Failed to generate Litecoin address");
    
    let address = result.unwrap();
    
    // Verify format: Litecoin mainnet starts with 'L'
    assert!(
        address.starts_with('L'),
        "Litecoin address must start with 'L', got: {}",
        address
    );
    
    // Verify length: Base58Check produces 26-34 character addresses
    assert!(
        address.len() >= 26 && address.len() <= 34,
        "Litecoin address length must be 26-34 chars, got: {}",
        address.len()
    );
    
    // Verify Base58 characters (no 0, O, I, l)
    for c in address.chars() {
        assert!(
            !matches!(c, '0' | 'O' | 'I' | 'l'),
            "Litecoin address contains invalid Base58 character: {}",
            c
        );
    }
}

#[tokio::test]
async fn test_litecoin_determinism() {
    let addr1 = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 5)
        .await
        .expect("Failed to derive address 1");
    
    let addr2 = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 5)
        .await
        .expect("Failed to derive address 2");
    
    assert_eq!(
        addr1, addr2,
        "Litecoin addresses must be deterministic for same index"
    );
}

#[tokio::test]
async fn test_litecoin_uniqueness_per_index() {
    let mut addresses = HashSet::new();
    
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", i)
            .await
            .expect(&format!("Failed to derive Litecoin address at index {}", i));
        
        assert!(
            addresses.insert(addr.clone()),
            "Duplicate Litecoin address at index {}: {}",
            i,
            addr
        );
    }
    
    assert_eq!(
        addresses.len(),
        10,
        "All 10 Litecoin addresses must be unique"
    );
}

#[tokio::test]
async fn test_litecoin_dispatcher_aliases() {
    let addr_ltc = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 3)
        .await
        .expect("Failed with 'ltc' ticker");
    
    let addr_litecoin = derive_address(&crate::common::test_wallet_mnemonic(), "litecoin", "litecoin", 3)
        .await
        .expect("Failed with 'litecoin' ticker");
    
    assert_eq!(
        addr_ltc, addr_litecoin,
        "Dispatcher aliases 'ltc' and 'litecoin' must produce same address"
    );
}

// =========================================
// DOGECOIN TESTS
// =========================================

#[tokio::test]
async fn test_dogecoin_address_generation() {
    let result = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", 0).await;
    assert!(result.is_ok(), "Failed to generate Dogecoin address");
    
    let address = result.unwrap();
    
    // Verify format: Dogecoin mainnet starts with 'D'
    assert!(
        address.starts_with('D'),
        "Dogecoin address must start with 'D', got: {}",
        address
    );
    
    // Verify length: Base58Check produces 26-34 character addresses
    assert!(
        address.len() >= 26 && address.len() <= 34,
        "Dogecoin address length must be 26-34 chars, got: {}",
        address.len()
    );
    
    // Verify Base58 characters (no 0, O, I, l)
    for c in address.chars() {
        assert!(
            !matches!(c, '0' | 'O' | 'I' | 'l'),
            "Dogecoin address contains invalid Base58 character: {}",
            c
        );
    }
}

#[tokio::test]
async fn test_dogecoin_determinism() {
    let addr1 = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", 7)
        .await
        .expect("Failed to derive address 1");
    
    let addr2 = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", 7)
        .await
        .expect("Failed to derive address 2");
    
    assert_eq!(
        addr1, addr2,
        "Dogecoin addresses must be deterministic for same index"
    );
}

#[tokio::test]
async fn test_dogecoin_uniqueness_per_index() {
    let mut addresses = HashSet::new();
    
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", i)
            .await
            .expect(&format!("Failed to derive Dogecoin address at index {}", i));
        
        assert!(
            addresses.insert(addr.clone()),
            "Duplicate Dogecoin address at index {}: {}",
            i,
            addr
        );
    }
    
    assert_eq!(
        addresses.len(),
        10,
        "All 10 Dogecoin addresses must be unique"
    );
}

#[tokio::test]
async fn test_dogecoin_dispatcher_aliases() {
    let addr_doge = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", 2)
        .await
        .expect("Failed with 'doge' ticker");
    
    let addr_dogecoin = derive_address(&crate::common::test_wallet_mnemonic(), "dogecoin", "dogecoin", 2)
        .await
        .expect("Failed with 'dogecoin' ticker");
    
    assert_eq!(
        addr_doge, addr_dogecoin,
        "Dispatcher aliases 'doge' and 'dogecoin' must produce same address"
    );
}

// =========================================
// BITCOIN CASH TESTS
// =========================================

#[tokio::test]
async fn test_bitcoin_cash_address_generation() {
    let result = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", 0).await;
    assert!(result.is_ok(), "Failed to generate Bitcoin Cash address");
    
    let address = result.unwrap();
    
    // Verify CashAddr format: starts with 'bitcoincash:'
    assert!(
        address.starts_with("bitcoincash:"),
        "Bitcoin Cash address must use CashAddr format starting with 'bitcoincash:', got: {}",
        address
    );
    
    // Verify CashAddr alphanumeric content (only qpzry9x8gf2tvdw0s3jn54khce6mua7l)
    let cashaddr_part = address.strip_prefix("bitcoincash:").unwrap();
    for c in cashaddr_part.chars() {
        assert!(
            matches!(c, 'q' | 'p' | 'z' | 'r' | 'y' | '9' | 'x' | '8' | 'g' | 'f' | '2' | 't' | 'v' | 'd' | 'w' | '0' | 's' | '3' | 'j' | 'n' | '5' | '4' | 'k' | 'h' | 'c' | 'e' | '6' | 'm' | 'u' | 'a' | '7' | 'l'),
            "Bitcoin Cash address contains invalid CashAddr character: {}",
            c
        );
    }
    
    // Verify reasonable length (typically 42-54 chars including prefix)
    assert!(
        address.len() > 20,
        "Bitcoin Cash CashAddr must be longer than 20 chars, got: {}",
        address.len()
    );
}

#[tokio::test]
async fn test_bitcoin_cash_determinism() {
    let addr1 = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", 4)
        .await
        .expect("Failed to derive address 1");
    
    let addr2 = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", 4)
        .await
        .expect("Failed to derive address 2");
    
    assert_eq!(
        addr1, addr2,
        "Bitcoin Cash addresses must be deterministic for same index"
    );
}

#[tokio::test]
async fn test_bitcoin_cash_uniqueness_per_index() {
    let mut addresses = HashSet::new();
    
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", i)
            .await
            .expect(&format!("Failed to derive Bitcoin Cash address at index {}", i));
        
        assert!(
            addresses.insert(addr.clone()),
            "Duplicate Bitcoin Cash address at index {}: {}",
            i,
            addr
        );
    }
    
    assert_eq!(
        addresses.len(),
        10,
        "All 10 Bitcoin Cash addresses must be unique"
    );
}

#[tokio::test]
async fn test_bitcoin_cash_dispatcher_aliases() {
    let addr_bch = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", 1)
        .await
        .expect("Failed with 'bch' ticker");
    
    let addr_bitcoincash = derive_address(&crate::common::test_wallet_mnemonic(), "bitcoin_cash", "bitcoin_cash", 1)
        .await
        .expect("Failed with 'bitcoin_cash' ticker");
    
    assert_eq!(
        addr_bch, addr_bitcoincash,
        "Dispatcher aliases 'bch' and 'bitcoin_cash' must produce same address"
    );
}

// =========================================
// CROSS-BLOCKCHAIN VALIDATION
// =========================================

#[tokio::test]
async fn test_tier2_addresses_different_from_each_other() {
    let ltc = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 0)
        .await
        .expect("Failed to generate LTC address");
    
    let doge = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", 0)
        .await
        .expect("Failed to generate DOGE address");
    
    let bch = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", 0)
        .await
        .expect("Failed to generate BCH address");
    
    assert_ne!(
        ltc, doge,
        "Litecoin and Dogecoin addresses must differ"
    );
    assert_ne!(
        ltc, bch,
        "Litecoin and Bitcoin Cash addresses must differ"
    );
    assert_ne!(
        doge, bch,
        "Dogecoin and Bitcoin Cash addresses must differ"
    );
}

#[tokio::test]
async fn test_invalid_seed_phrase_rejected() {
    let invalid_seeds = vec![
        "",
        "invalid",
        "abandon abandon abandon",
        "12345 12345 12345",
    ];
    
    for seed in invalid_seeds {
        let result = derive_address(seed, "ltc", "litecoin", 0).await;
        assert!(
            result.is_err(),
            "Invalid seed '{}' should be rejected",
            seed
        );
    }
}

#[tokio::test]
async fn test_performance_100_addresses_under_7_seconds() {
    let start = std::time::Instant::now();
    
    for i in 0..25 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", i).await;
    }
    for i in 0..25 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "doge", "dogecoin", i).await;
    }
    for i in 0..25 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "bch", "bitcoin_cash", i).await;
    }
    for i in 0..25 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", i + 100).await;
    }
    
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_secs() < 7,
        "Generating 100 addresses took {:.2}s, must be under 7s",
        elapsed.as_secs_f64()
    );
}
