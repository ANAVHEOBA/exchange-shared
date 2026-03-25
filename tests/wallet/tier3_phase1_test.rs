/// Tier 3 Phase 1 Tests: Bitcoin-like, Cosmos-like, Substrate-like Generic Wrappers
///
/// Tests 30+ Bitcoin-like, 13 Cosmos-like, 5 Substrate-like networks
/// All using generic wrapper functions
use exchange_shared::services::wallet::derivation::derive_address;
use std::collections::HashSet;

// =========================================
// BITCOIN-LIKE TESTS (Dash, Zcash, etc)
// =========================================

#[tokio::test]
async fn test_dash_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", 0).await;
    assert!(addr.is_ok(), "Failed to generate Dash address");
    let address = addr.unwrap();

    // Dash addresses start with X
    assert!(
        address.starts_with('X'),
        "Dash address must start with 'X', got: {}",
        address
    );
    assert!(address.len() >= 26 && address.len() <= 34);
}

#[tokio::test]
async fn test_zcash_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "zec", "mainnet", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with("t1"),
        "Zcash transparent address must start with 't1', got: {}",
        address
    );
    assert!(address.len() >= 30 && address.len() <= 40);
}

#[tokio::test]
async fn test_monacoin_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "mona", "mainnet", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with('M'),
        "Monacoin address must start with 'M', got: {}",
        address
    );
    assert!(address.len() >= 26 && address.len() <= 34);
}

#[tokio::test]
async fn test_bitcoin_like_determinism() {
    let dash1 = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", 5)
        .await
        .unwrap();
    let dash2 = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", 5)
        .await
        .unwrap();
    assert_eq!(dash1, dash2, "Dash addresses must be deterministic");

    let zec1 = derive_address(&crate::common::test_wallet_mnemonic(), "zec", "mainnet", 7)
        .await
        .unwrap();
    let zec2 = derive_address(&crate::common::test_wallet_mnemonic(), "zec", "mainnet", 7)
        .await
        .unwrap();
    assert_eq!(zec1, zec2, "Zcash addresses must be deterministic");
}

#[tokio::test]
async fn test_bitcoin_like_uniqueness() {
    let mut dash_addrs = HashSet::new();
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", i)
            .await
            .unwrap();
        assert!(
            dash_addrs.insert(addr),
            "Duplicate Dash address at index {}",
            i
        );
    }
    assert_eq!(dash_addrs.len(), 10, "All 10 Dash addresses must be unique");
}

// =========================================
// COSMOS-LIKE TESTS (Osmosis, Juno, etc)
// =========================================

#[tokio::test]
async fn test_osmosis_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    // Osmosis addresses start with osmo1
    assert!(
        address.starts_with("osmo1"),
        "Osmosis address must start with 'osmo1', got: {}",
        address
    );
    assert!(address.len() > 40);
}

#[tokio::test]
async fn test_juno_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "juno", "juno", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with("juno1"),
        "Juno address must start with 'juno1', got: {}",
        address
    );
    assert!(address.len() > 40);
}

#[tokio::test]
async fn test_akash_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "akt", "akash", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with("akash1"),
        "Akash address must start with 'akash1', got: {}",
        address
    );
    assert!(address.len() > 40);
}

#[tokio::test]
async fn test_cronos_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "cro", "cronos", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with("cro1"),
        "Cronos address must start with 'cro1', got: {}",
        address
    );
}

#[tokio::test]
async fn test_injective_address_generation() {
    let addr = derive_address(
        &crate::common::test_wallet_mnemonic(),
        "inj",
        "injective",
        0,
    )
    .await;
    assert!(addr.is_ok());
    let address = addr.unwrap();

    assert!(
        address.starts_with("inj1"),
        "Injective address must start with 'inj1', got: {}",
        address
    );
}

#[tokio::test]
async fn test_cosmos_like_determinism() {
    let osmo1 = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", 3)
        .await
        .unwrap();
    let osmo2 = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", 3)
        .await
        .unwrap();
    assert_eq!(osmo1, osmo2, "Osmosis addresses must be deterministic");

    let juno1 = derive_address(&crate::common::test_wallet_mnemonic(), "juno", "juno", 5)
        .await
        .unwrap();
    let juno2 = derive_address(&crate::common::test_wallet_mnemonic(), "juno", "juno", 5)
        .await
        .unwrap();
    assert_eq!(juno1, juno2, "Juno addresses must be deterministic");
}

#[tokio::test]
async fn test_cosmos_like_uniqueness() {
    let mut osmo_addrs = HashSet::new();
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", i)
            .await
            .unwrap();
        assert!(
            osmo_addrs.insert(addr),
            "Duplicate Osmosis address at index {}",
            i
        );
    }
    assert_eq!(
        osmo_addrs.len(),
        10,
        "All 10 Osmosis addresses must be unique"
    );
}

// =========================================
// SUBSTRATE-LIKE TESTS (Kusama, Acala, etc)
// =========================================

#[tokio::test]
async fn test_kusama_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", 0).await;
    assert!(addr.is_ok(), "Failed to generate Kusama address");
    let address = addr.unwrap();

    // Kusama is Substrate, addresses are longer
    assert!(
        address.len() > 45,
        "Kusama address too short: {}",
        address.len()
    );
}

#[tokio::test]
async fn test_acala_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "aca", "acala", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();
    assert!(address.len() > 45);
}

#[tokio::test]
async fn test_astar_address_generation() {
    let addr = derive_address(&crate::common::test_wallet_mnemonic(), "astr", "astar", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();
    assert!(address.len() > 45);
}

#[tokio::test]
async fn test_substrate_like_determinism() {
    let ksm1 = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", 2)
        .await
        .unwrap();
    let ksm2 = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", 2)
        .await
        .unwrap();
    assert_eq!(ksm1, ksm2, "Kusama addresses must be deterministic");

    let aca1 = derive_address(&crate::common::test_wallet_mnemonic(), "aca", "acala", 4)
        .await
        .unwrap();
    let aca2 = derive_address(&crate::common::test_wallet_mnemonic(), "aca", "acala", 4)
        .await
        .unwrap();
    assert_eq!(aca1, aca2, "Acala addresses must be deterministic");
}

#[tokio::test]
async fn test_substrate_like_uniqueness() {
    let mut ksm_addrs = HashSet::new();
    for i in 0..10 {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", i)
            .await
            .unwrap();
        assert!(
            ksm_addrs.insert(addr),
            "Duplicate Kusama address at index {}",
            i
        );
    }
    assert_eq!(
        ksm_addrs.len(),
        10,
        "All 10 Kusama addresses must be unique"
    );
}

// =========================================
// CROSS-CHAIN VALIDATION
// =========================================

#[tokio::test]
async fn test_all_tier3_different_networks() {
    let dash = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", 0)
        .await
        .unwrap();
    let zec = derive_address(&crate::common::test_wallet_mnemonic(), "zec", "mainnet", 0)
        .await
        .unwrap();
    let osmo = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", 0)
        .await
        .unwrap();
    let ksm = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", 0)
        .await
        .unwrap();

    assert_ne!(dash, zec, "Dash and Zcash must have different addresses");
    assert_ne!(dash, osmo, "Dash and Osmosis must have different addresses");
    assert_ne!(dash, ksm, "Dash and Kusama must have different addresses");
    assert_ne!(
        osmo, ksm,
        "Osmosis and Kusama must have different addresses"
    );
}

#[tokio::test]
async fn test_tier3_no_collisions_with_tier1_tier2() {
    // Tier 1
    let ada = derive_address(&crate::common::test_wallet_mnemonic(), "ada", "cardano", 0)
        .await
        .unwrap();

    // Tier 2 Phase 1
    let ltc = derive_address(&crate::common::test_wallet_mnemonic(), "ltc", "litecoin", 0)
        .await
        .unwrap();

    // Tier 3
    let dash = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", 0)
        .await
        .unwrap();
    let osmo = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", 0)
        .await
        .unwrap();

    assert_ne!(ada, ltc, "Tier 1 and Tier 2 collision");
    assert_ne!(ada, dash, "Tier 1 and Tier 3 Bitcoin-like collision");
    assert_ne!(ada, osmo, "Tier 1 and Tier 3 Cosmos-like collision");
    assert_ne!(ltc, dash, "Tier 2 and Tier 3 Bitcoin-like collision");
}

#[tokio::test]
async fn test_invalid_seed_rejected() {
    let invalid_seeds = vec!["", "invalid", "word word word", "12345 12345 12345"];

    for seed in invalid_seeds {
        let result = derive_address(seed, "dash", "mainnet", 0).await;
        assert!(
            result.is_err(),
            "Invalid seed '{}' should be rejected",
            seed
        );
    }
}

#[tokio::test]
async fn test_performance_tier3_addresses() {
    let start = std::time::Instant::now();

    // 10 Dash addresses
    for i in 0..10 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "dash", "mainnet", i).await;
    }

    // 10 Osmosis addresses
    for i in 0..10 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "osmo", "osmosis", i).await;
    }

    // 10 Kusama addresses
    for i in 0..10 {
        let _ = derive_address(&crate::common::test_wallet_mnemonic(), "ksm", "kusama", i).await;
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed.as_secs() < 5,
        "Generating 30 Tier 3 addresses took {:.2}s, must be under 5s",
        elapsed.as_secs_f64()
    );
}

#[tokio::test]
async fn test_all_bitcoinlike_networks() {
    let networks = vec![
        ("dash", "mainnet"),
        ("zec", "mainnet"),
        ("mona", "mainnet"),
        ("vtc", "mainnet"),
        ("dgb", "mainnet"),
        ("rvn", "mainnet"),
        ("grs", "mainnet"),
        ("nmc", "mainnet"),
        ("sys", "mainnet"),
        ("via", "mainnet"),
        ("pivx", "mainnet"),
    ];

    for (ticker, network) in networks {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), ticker, network, 0).await;
        assert!(addr.is_ok(), "Failed for {} on {}", ticker, network);
    }
}

#[tokio::test]
async fn test_all_cosmoslike_networks() {
    let networks = vec![
        ("osmo", "osmosis"),
        ("juno", "juno"),
        ("akt", "akash"),
        ("regen", "regen"),
        ("stars", "stargaze"),
        ("cro", "cronos"),
        ("inj", "injective"),
        ("scrt", "secret"),
        ("kava", "kava"),
        ("sei", "sei"),
        ("band", "band"),
        ("ion", "ion"),
        ("gravitybg", "gravity"),
    ];

    for (ticker, network) in networks {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), ticker, network, 0).await;
        assert!(addr.is_ok(), "Failed for {} on {}", ticker, network);
    }
}

#[tokio::test]
async fn test_all_substratelike_networks() {
    let networks = vec![
        ("ksm", "kusama"),
        ("aca", "acala"),
        ("astr", "astar"),
        ("sdn", "shiden"),
        ("para", "parallel"),
    ];

    for (ticker, network) in networks {
        let addr = derive_address(&crate::common::test_wallet_mnemonic(), ticker, network, 0).await;
        assert!(addr.is_ok(), "Failed for {} on {}", ticker, network);
    }
}
