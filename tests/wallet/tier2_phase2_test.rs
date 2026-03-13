// =============================================================================
// TIER 2 PHASE 2 TESTS: Mid-Complexity Blockchains (7 Networks)
// Tezos, Algorand, Stellar, NEAR, Waves, Stacks, TON
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::wallet::derivation::derive_address;

fn test_seed() -> String {
    common::test_wallet_mnemonic()
}

// =============================================================================
// TEZOS (XTZ) TESTS
// =============================================================================

#[tokio::test]
async fn test_tezos_address_derivation() {
    let addr = derive_address(&test_seed(), "xtz", "tezos", 0)
        .await
        .expect("Failed to derive Tezos address");

    // Tezos addresses start with 'tz1' prefix
    assert!(addr.starts_with("tz1"), "Tezos address should start with tz1, got: {}", addr);
    assert!(addr.len() >= 34, "Tezos address too short: {}", addr);
    println!("✓ Tezos address (index 0): {}", addr);
}

#[tokio::test]
async fn test_tezos_deterministic() {
    let addr1 = derive_address(&test_seed(), "xtz", "tezos", 0)
        .await
        .expect("Failed to derive Tezos address");
    let addr2 = derive_address(&test_seed(), "xtz", "tezos", 0)
        .await
        .expect("Failed to derive Tezos address again");

    assert_eq!(addr1, addr2, "Tezos derivation not deterministic");
    println!("✓ Tezos deterministic: {}", addr1);
}

#[tokio::test]
async fn test_tezos_unique_indices() {
    let addr0 = derive_address(&test_seed(), "xtz", "tezos", 0)
        .await
        .expect("Failed to derive Tezos address at index 0");
    let addr1 = derive_address(&test_seed(), "xtz", "tezos", 1)
        .await
        .expect("Failed to derive Tezos address at index 1");

    assert_ne!(addr0, addr1, "Tezos addresses should differ for different indices");
    println!("✓ Tezos unique: index 0: {}", addr0);
    println!("✓ Tezos unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_tezos_invalid_seed() {
    let result = derive_address("invalid seed", "xtz", "tezos", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ Tezos invalid seed rejected");
}

// =============================================================================
// ALGORAND (ALGO) TESTS
// =============================================================================

#[tokio::test]
async fn test_algorand_address_derivation() {
    let addr = derive_address(TEST_SEED, "algo", "algorand", 0)
        .await
        .expect("Failed to derive Algorand address");

    // Algorand addresses are 58 characters, uppercase base32
    assert_eq!(addr.len(), 58, "Algorand address should be 58 chars, got: {}", addr.len());
    assert!(
        addr.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
        "Algorand address should be base32 encoded"
    );
    println!("✓ Algorand address (index 0): {}", addr);
}

#[tokio::test]
async fn test_algorand_deterministic() {
    let addr1 = derive_address(TEST_SEED, "algo", "algorand", 0)
        .await
        .expect("Failed to derive Algorand address");
    let addr2 = derive_address(TEST_SEED, "algo", "algorand", 0)
        .await
        .expect("Failed to derive Algorand address again");

    assert_eq!(addr1, addr2, "Algorand derivation not deterministic");
    println!("✓ Algorand deterministic: {}", addr1);
}

#[tokio::test]
async fn test_algorand_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "algo", "algorand", 0)
        .await
        .expect("Failed to derive Algorand address at index 0");
    let addr1 = derive_address(TEST_SEED, "algo", "algorand", 1)
        .await
        .expect("Failed to derive Algorand address at index 1");

    assert_ne!(addr0, addr1, "Algorand addresses should differ for different indices");
    println!("✓ Algorand unique: index 0: {}", addr0);
    println!("✓ Algorand unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_algorand_invalid_seed() {
    let result = derive_address("weak seed", "algo", "algorand", 0).await;
    assert!(result.is_err(), "Should reject weak seed");
    println!("✓ Algorand invalid seed rejected");
}

// =============================================================================
// STELLAR (XLM) TESTS
// =============================================================================

#[tokio::test]
async fn test_stellar_address_derivation() {
    let addr = derive_address(TEST_SEED, "xlm", "stellar", 0)
        .await
        .expect("Failed to derive Stellar address");

    // Stellar addresses start with 'G' prefix
    assert!(addr.starts_with("G"), "Stellar address should start with G, got: {}", addr);
    assert_eq!(addr.len(), 56, "Stellar address should be 56 chars, got: {}", addr.len());
    println!("✓ Stellar address (index 0): {}", addr);
}

#[tokio::test]
async fn test_stellar_deterministic() {
    let addr1 = derive_address(TEST_SEED, "xlm", "stellar", 0)
        .await
        .expect("Failed to derive Stellar address");
    let addr2 = derive_address(TEST_SEED, "xlm", "stellar", 0)
        .await
        .expect("Failed to derive Stellar address again");

    assert_eq!(addr1, addr2, "Stellar derivation not deterministic");
    println!("✓ Stellar deterministic: {}", addr1);
}

#[tokio::test]
async fn test_stellar_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "xlm", "stellar", 0)
        .await
        .expect("Failed to derive Stellar address at index 0");
    let addr1 = derive_address(TEST_SEED, "xlm", "stellar", 1)
        .await
        .expect("Failed to derive Stellar address at index 1");

    assert_ne!(addr0, addr1, "Stellar addresses should differ for different indices");
    println!("✓ Stellar unique: index 0: {}", addr0);
    println!("✓ Stellar unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_stellar_invalid_seed() {
    let result = derive_address("invalid", "xlm", "stellar", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ Stellar invalid seed rejected");
}

// =============================================================================
// NEAR PROTOCOL TESTS
// =============================================================================

#[tokio::test]
async fn test_near_address_derivation() {
    let addr = derive_address(TEST_SEED, "near", "near", 0)
        .await
        .expect("Failed to derive NEAR address");

    // NEAR implicit accounts are 64-char hex (32-byte public key)
    assert_eq!(addr.len(), 64, "NEAR address should be 64 hex chars, got: {}", addr.len());
    assert!(
        addr.chars().all(|c| c.is_ascii_hexdigit()),
        "NEAR address should be hexadecimal"
    );
    println!("✓ NEAR address (index 0): {}", addr);
}

#[tokio::test]
async fn test_near_deterministic() {
    let addr1 = derive_address(TEST_SEED, "near", "near", 0)
        .await
        .expect("Failed to derive NEAR address");
    let addr2 = derive_address(TEST_SEED, "near", "near", 0)
        .await
        .expect("Failed to derive NEAR address again");

    assert_eq!(addr1, addr2, "NEAR derivation not deterministic");
    println!("✓ NEAR deterministic: {}", addr1);
}

#[tokio::test]
async fn test_near_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "near", "near", 0)
        .await
        .expect("Failed to derive NEAR address at index 0");
    let addr1 = derive_address(TEST_SEED, "near", "near", 1)
        .await
        .expect("Failed to derive NEAR address at index 1");

    assert_ne!(addr0, addr1, "NEAR addresses should differ for different indices");
    println!("✓ NEAR unique: index 0: {}", addr0);
    println!("✓ NEAR unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_near_invalid_seed() {
    let result = derive_address("not a valid seed phrase", "near", "near", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ NEAR invalid seed rejected");
}

// =============================================================================
// WAVES TESTS
// =============================================================================

#[tokio::test]
async fn test_waves_address_derivation() {
    let addr = derive_address(TEST_SEED, "waves", "waves", 0)
        .await
        .expect("Failed to derive Waves address");

    // Waves addresses start with '3' (version byte 0x17)
    assert!(addr.starts_with("3"), "Waves address should start with 3, got: {}", addr);
    assert!(addr.len() >= 26, "Waves address too short: {}", addr);
    println!("✓ Waves address (index 0): {}", addr);
}

#[tokio::test]
async fn test_waves_deterministic() {
    let addr1 = derive_address(TEST_SEED, "waves", "waves", 0)
        .await
        .expect("Failed to derive Waves address");
    let addr2 = derive_address(TEST_SEED, "waves", "waves", 0)
        .await
        .expect("Failed to derive Waves address again");

    assert_eq!(addr1, addr2, "Waves derivation not deterministic");
    println!("✓ Waves deterministic: {}", addr1);
}

#[tokio::test]
async fn test_waves_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "waves", "waves", 0)
        .await
        .expect("Failed to derive Waves address at index 0");
    let addr1 = derive_address(TEST_SEED, "waves", "waves", 1)
        .await
        .expect("Failed to derive Waves address at index 1");

    assert_ne!(addr0, addr1, "Waves addresses should differ for different indices");
    println!("✓ Waves unique: index 0: {}", addr0);
    println!("✓ Waves unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_waves_invalid_seed() {
    let result = derive_address("bad seed", "waves", "waves", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ Waves invalid seed rejected");
}

// =============================================================================
// STACKS (STX) TESTS
// =============================================================================

#[tokio::test]
async fn test_stacks_address_derivation() {
    let addr = derive_address(TEST_SEED, "stx", "stacks", 0)
        .await
        .expect("Failed to derive Stacks address");

    // Stacks addresses are base58 encoded
    assert!(addr.len() >= 30, "Stacks address too short: {}", addr);
    println!("✓ Stacks address (index 0): {}", addr);
}

#[tokio::test]
async fn test_stacks_deterministic() {
    let addr1 = derive_address(TEST_SEED, "stx", "stacks", 0)
        .await
        .expect("Failed to derive Stacks address");
    let addr2 = derive_address(TEST_SEED, "stx", "stacks", 0)
        .await
        .expect("Failed to derive Stacks address again");

    assert_eq!(addr1, addr2, "Stacks derivation not deterministic");
    println!("✓ Stacks deterministic: {}", addr1);
}

#[tokio::test]
async fn test_stacks_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "stx", "stacks", 0)
        .await
        .expect("Failed to derive Stacks address at index 0");
    let addr1 = derive_address(TEST_SEED, "stx", "stacks", 1)
        .await
        .expect("Failed to derive Stacks address at index 1");

    assert_ne!(addr0, addr1, "Stacks addresses should differ for different indices");
    println!("✓ Stacks unique: index 0: {}", addr0);
    println!("✓ Stacks unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_stacks_invalid_seed() {
    let result = derive_address("short", "stx", "stacks", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ Stacks invalid seed rejected");
}

// =============================================================================
// TON TESTS
// =============================================================================

#[tokio::test]
async fn test_ton_address_derivation() {
    let addr = derive_address(TEST_SEED, "ton", "ton", 0)
        .await
        .expect("Failed to derive TON address");

    // TON addresses format: workchain:account
    assert!(addr.contains(":"), "TON address should contain colon separator");
    println!("✓ TON address (index 0): {}", addr);
}

#[tokio::test]
async fn test_ton_deterministic() {
    let addr1 = derive_address(TEST_SEED, "ton", "ton", 0)
        .await
        .expect("Failed to derive TON address");
    let addr2 = derive_address(TEST_SEED, "ton", "ton", 0)
        .await
        .expect("Failed to derive TON address again");

    assert_eq!(addr1, addr2, "TON derivation not deterministic");
    println!("✓ TON deterministic: {}", addr1);
}

#[tokio::test]
async fn test_ton_unique_indices() {
    let addr0 = derive_address(TEST_SEED, "ton", "ton", 0)
        .await
        .expect("Failed to derive TON address at index 0");
    let addr1 = derive_address(TEST_SEED, "ton", "ton", 1)
        .await
        .expect("Failed to derive TON address at index 1");

    assert_ne!(addr0, addr1, "TON addresses should differ for different indices");
    println!("✓ TON unique: index 0: {}", addr0);
    println!("✓ TON unique: index 1: {}", addr1);
}

#[tokio::test]
async fn test_ton_invalid_seed() {
    let result = derive_address("bad", "ton", "ton", 0).await;
    assert!(result.is_err(), "Should reject invalid seed");
    println!("✓ TON invalid seed rejected");
}

// =============================================================================
// CROSS-CHAIN VALIDATION TESTS
// =============================================================================

#[tokio::test]
async fn test_all_phase2_networks_derivable() {
    let networks = vec![
        ("xtz", "tezos"),
        ("algo", "algorand"),
        ("xlm", "stellar"),
        ("near", "near"),
        ("waves", "waves"),
        ("stx", "stacks"),
        ("ton", "ton"),
    ];

    for (ticker, network) in networks {
        let addr = derive_address(TEST_SEED, ticker, network, 0)
            .await
            .expect(&format!("Failed to derive {} address", network));
        assert!(!addr.is_empty(), "{} address is empty", network);
        println!("✓ {}: {}", ticker.to_uppercase(), addr);
    }
}

#[tokio::test]
async fn test_phase2_no_duplicate_addresses() {
    let mut addresses = std::collections::HashSet::new();

    let networks = vec![
        ("xtz", "tezos"),
        ("algo", "algorand"),
        ("xlm", "stellar"),
        ("near", "near"),
        ("waves", "waves"),
        ("stx", "stacks"),
        ("ton", "ton"),
    ];

    for (ticker, network) in networks {
        let addr = derive_address(TEST_SEED, ticker, network, 0)
            .await
            .expect(&format!("Failed to derive {} address", network));
        
        assert!(
            addresses.insert(addr.clone()),
            "Duplicate address found for {} using same seed/index",
            network
        );
    }

    println!("✓ All Phase 2 networks produce unique addresses");
}

#[tokio::test]
async fn test_phase2_batch_generation() {
    // Generate 100 addresses for each network to test performance
    let networks = vec![
        ("xtz", "tezos"),
        ("algo", "algorand"),
        ("xlm", "stellar"),
        ("near", "near"),
        ("waves", "waves"),
        ("stx", "stacks"),
        ("ton", "ton"),
    ];

    for (ticker, network) in networks {
        let start = std::time::Instant::now();
        for i in 0..100 {
            let _ = derive_address(TEST_SEED, ticker, network, i)
                .await
                .expect(&format!("Failed to generate {} batch address {}", network, i));
        }
        let elapsed = start.elapsed();
        println!("✓ {} - 100 addresses in {:.2}s", network.to_uppercase(), elapsed.as_secs_f64());
    }
}

#[tokio::test]
async fn test_phase2_index_boundaries() {
    // Test various index values
    let test_indices = vec![0, 1, 42, 1000, u32::MAX - 1];

    for index in test_indices {
        let addr = derive_address(TEST_SEED, "algo", "algorand", index)
            .await
            .expect(&format!("Failed to derive address at index {}", index));
        assert!(!addr.is_empty(), "Address empty for index {}", index);
    }

    println!("✓ Index boundary testing passed");
}
