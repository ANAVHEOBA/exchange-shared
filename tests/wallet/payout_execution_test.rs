// =============================================================================
// INTEGRATION TESTS - PAYOUT EXECUTION
// Tests for transferring converted crypto to user's recipient address
// Flow: Trocador sends to us → We deduct commission → We send to user
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use async_trait::async_trait;
use common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::model::PayoutStatus;
use exchange_shared::modules::wallet::schema::{GenerateAddressRequest, PayoutRequest};
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, RpcError};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// =============================================================================
// MOCK PROVIDER
// =============================================================================

#[derive(Clone)]
struct MockProvider {
    nonce: u64,
    gas_price: u64,
    broadcast_hash: String,
    broadcasted_txs: Arc<Mutex<Vec<String>>>,
}

impl MockProvider {
    fn new() -> Self {
        Self {
            nonce: 5,
            gas_price: 20_000_000_000,
            broadcast_hash: "0xrealhash123".to_string(),
            broadcasted_txs: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockchainProvider for MockProvider {
    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Ok(self.nonce)
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Ok(self.gas_price)
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        self.broadcasted_txs
            .lock()
            .unwrap()
            .push(signed_hex.to_string());
        Ok(self.broadcast_hash.clone())
    }

    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(1.0) // Return 1.0 to match test expectations
    }
}

// Helper to create a dummy swap in DB
async fn create_payout_ready_swap(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
    amount: f64,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address,
            platform_fee, total_fee, status
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', 'ETH', 'ethereum', 0.1, ?, 15.0, 'dep_addr', ?, 0.01, 0.01, 'completed')
        "#
    )
    .bind(swap_id)
    .bind(amount)
    .bind(recipient)
    .execute(db)
    .await
    .expect("Failed to create payout ready swap");
}

// =============================================================================
// TEST 1: Payout Deduction During Payout
// The wallet payout flow should subtract both the quoted service fee and the network fee.
// =============================================================================

#[tokio::test]
async fn test_commission_deduction_on_payout() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new());
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    let amount_from_trocador = 1.0;

    create_payout_ready_swap(&ctx.db, &swap_id, recipient, amount_from_trocador).await;

    // 1. Generate our receiving address first
    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    // 3. Execute payout (will use blockchain balance from mock: 1.0)
    let res = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(res.status, PayoutStatus::Success);

    let expected_network_fee = 21_000.0 * 20_000_000_000.0 / 1_000_000_000_000_000_000.0;
    let expected_payout = 1.0 - 0.01 - expected_network_fee;

    assert!(
        (res.amount - expected_payout).abs() < 0.0000001,
        "Expected {} payout, got {}",
        expected_payout,
        res.amount
    );

    println!(
        "✅ Payout deduction verified: {:.6} ETH to user",
        res.amount
    );
    ctx.cleanup().await;
}

// =============================================================================
// TEST 2: Payout Tracking Audit Trail
// =============================================================================

#[tokio::test]
async fn test_payout_audit_trail() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new());
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider);

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    create_payout_ready_swap(&ctx.db, &swap_id, recipient, 0.5).await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    // Execute payout (will use blockchain balance from mock: 1.0)
    manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    // Verify status in DB
    let info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.status, "success");
    assert_eq!(info.payout_tx_hash, Some("0xrealhash123".to_string()));
    assert_eq!(info.actual_received, Some(1.0));
    assert_eq!(info.commission_taken, Some(0.01));
    assert_eq!(info.network_fee_paid, Some(0.00042));
    assert!(info.payout_amount.unwrap_or_default() < 0.99);

    println!("✅ Payout audit trail maintained in DB");
    ctx.cleanup().await;
}
