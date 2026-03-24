// =============================================================================
// INTEGRATION TESTS - ADDRESS GENERATION & ROTATION
// Tests for generating unique addresses per swap to avoid flagging
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::GenerateAddressRequest;
use exchange_shared::services::wallet::manager::WalletManager;
use std::sync::Arc;
use uuid::Uuid;

// Helper to create a dummy swap in DB with unique IDs
async fn create_dummy_swap(db: &sqlx::Pool<sqlx::MySql>, swap_id: &str) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', 'ETH', 'ethereum', 0.1, 1.5, 15.0, 'dep_addr', 'rec_addr', 'waiting')
        "#
    )
    .bind(swap_id)
    .execute(db)
    .await
    .expect("Failed to create dummy swap");
}

// =============================================================================
// TEST 1: Generate Unique Address Per Swap
// Each swap should get a different receiving address
// =============================================================================

#[tokio::test]
async fn test_unique_address_per_swap() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let manager = WalletManager::new(crud, seed_phrase, Arc::new(common::NoOpProvider));

    let mut addresses = vec![];

    for _ in 0..5 {
        let swap_id = Uuid::new_v4().to_string();
        create_dummy_swap(&ctx.db, &swap_id).await;

        let req = GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
            user_recipient_extra_id: None,
        };

        let res = manager.get_or_generate_address(req).await.unwrap();
        addresses.push(res.address);
    }

    // All addresses should be different
    let original_len = addresses.len();
    addresses.sort();
    addresses.dedup();

    assert_eq!(
        addresses.len(),
        original_len,
        "All swap addresses should be unique"
    );

    println!("✅ Generated {} unique addresses", original_len);
    ctx.cleanup().await;
}

// =============================================================================
// TEST 2: Address Sequence Can Be Predicted
// Given swap_index, we can predict the exact address (BIP44)
// =============================================================================

#[tokio::test]
async fn test_address_sequence_predictable() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let manager = WalletManager::new(crud, seed_phrase, Arc::new(common::NoOpProvider));

    let swap_id_0 = Uuid::new_v4().to_string();
    let swap_id_1 = Uuid::new_v4().to_string();

    // Swap 0
    create_dummy_swap(&ctx.db, &swap_id_0).await;
    let res_0 = manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id_0,
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    // Swap 1
    create_dummy_swap(&ctx.db, &swap_id_1).await;
    let res_1 = manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id_1,
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    assert_ne!(res_0.address, res_1.address);
    // Indices should be consecutive regardless of starting point
    assert!(res_1.address_index > res_0.address_index);

    println!(
        "✅ Address sequence is sequential ({} -> {})",
        res_0.address_index, res_1.address_index
    );
    ctx.cleanup().await;
}

// =============================================================================
// TEST 3: Address Idempotency
// Requesting an address for the same swap_id twice should return the same address
// =============================================================================

#[tokio::test]
async fn test_address_idempotency() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let manager = WalletManager::new(crud, seed_phrase, Arc::new(common::NoOpProvider));

    let swap_id = Uuid::new_v4().to_string();
    create_dummy_swap(&ctx.db, &swap_id).await;

    let req = GenerateAddressRequest {
        swap_id: swap_id.to_string(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
        user_recipient_extra_id: None,
    };

    let res_1 = manager.get_or_generate_address(req.clone()).await.unwrap();
    let res_2 = manager.get_or_generate_address(req).await.unwrap();

    assert_eq!(
        res_1.address, res_2.address,
        "Should return same address for same swap_id"
    );
    assert_eq!(res_1.address_index, res_2.address_index);

    println!("✅ Address generation is idempotent");
    ctx.cleanup().await;
}

// =============================================================================
// TEST 4: Cross-Chain Generation
// BTC and ETH should generate addresses correctly from same manager
// =============================================================================

#[tokio::test]
async fn test_cross_chain_generation() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let manager = WalletManager::new(crud, seed_phrase, Arc::new(common::NoOpProvider));

    let btc_swap = Uuid::new_v4().to_string();
    let eth_swap = Uuid::new_v4().to_string();

    // BTC Swap
    create_dummy_swap(&ctx.db, &btc_swap).await;
    let btc_res = manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: btc_swap,
            ticker: "BTC".to_string(),
            network: "bitcoin".to_string(),
            user_recipient_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh".to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    // ETH Swap
    create_dummy_swap(&ctx.db, &eth_swap).await;
    let eth_res = manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: eth_swap,
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12".to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    assert!(btc_res.address.starts_with("1") || btc_res.address.starts_with("bc1"));
    assert!(eth_res.address.starts_with("0x"));

    println!("✅ Cross-chain address generation works");
    ctx.cleanup().await;
}
