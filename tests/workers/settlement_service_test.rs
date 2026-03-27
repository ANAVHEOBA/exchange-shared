// =============================================================================
// INTEGRATION TESTS - SETTLEMENT SERVICE
// Verifies that both worker paths can rely on a single payout entrypoint
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use async_trait::async_trait;
use common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::GenerateAddressRequest;
use exchange_shared::services::settlement::{SettlementOutcome, SettlementService};
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, RpcError};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
struct SuccessfulBlockchainProvider;

#[async_trait]
impl BlockchainProvider for SuccessfulBlockchainProvider {
    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(1.0)
    }

    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Ok(5)
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Ok(20_000_000_000)
    }

    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> {
        Ok("0xsettlementsuccess".to_string())
    }
}

#[derive(Clone)]
struct FailingBlockchainProvider;

#[async_trait]
impl BlockchainProvider for FailingBlockchainProvider {
    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(1.0)
    }

    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Ok(5)
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Ok(20_000_000_000)
    }

    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> {
        Err(RpcError::Network("simulated broadcast failure".to_string()))
    }
}

async fn create_swap_for_settlement(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
    status: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, provider_swap_id, from_currency, from_network,
            to_currency, to_network, amount, estimated_receive, platform_fee,
            rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'test_trade_123', 'BTC', 'bitcoin',
                'ETH', 'ethereum', 0.1, 1.0, 0.012, 15.0, 'dep_addr', ?, ?)
        "#,
    )
    .bind(swap_id)
    .bind(recipient)
    .bind(status)
    .execute(db)
    .await
    .expect("Failed to create swap");
}

async fn attach_internal_address(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    mnemonic: &str,
    provider: Arc<dyn BlockchainProvider>,
) {
    let wallet_manager =
        WalletManager::new(WalletCrud::new(db.clone()), mnemonic.to_string(), provider);

    wallet_manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.to_string(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: "0x742d35Cc6634C0532925a3b844Bc454e4438f44e".to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .expect("Failed to create internal address");
}

#[tokio::test]
async fn test_settlement_service_completes_payout() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let mnemonic = crate::common::test_wallet_mnemonic();
    let provider = Arc::new(SuccessfulBlockchainProvider);

    create_swap_for_settlement(
        &ctx.db,
        &swap_id,
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "sending",
    )
    .await;
    attach_internal_address(&ctx.db, &swap_id, &mnemonic, provider.clone()).await;

    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));
    let outcome = settlement_service
        .settle_swap(&swap_id, provider, Some(1.0))
        .await
        .expect("Settlement should succeed");

    match outcome {
        SettlementOutcome::Completed(response) => {
            assert_eq!(response.tx_hash, "0xsettlementsuccess");
        }
        other => panic!("Expected completed settlement outcome, got {:?}", other),
    }

    let (swap_status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(&swap_id)
        .fetch_one(&ctx.db)
        .await
        .unwrap();
    assert_eq!(swap_status, "completed");

    let address_info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .expect("Expected address info");
    assert_eq!(address_info.status, "success");
    assert_eq!(
        address_info.payout_tx_hash.as_deref(),
        Some("0xsettlementsuccess")
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_settlement_service_keeps_failed_payout_retryable() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let mnemonic = crate::common::test_wallet_mnemonic();
    let provider = Arc::new(FailingBlockchainProvider);

    create_swap_for_settlement(
        &ctx.db,
        &swap_id,
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "sending",
    )
    .await;
    attach_internal_address(&ctx.db, &swap_id, &mnemonic, provider.clone()).await;

    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));
    let outcome = settlement_service
        .settle_swap(&swap_id, provider, Some(1.0))
        .await
        .expect("Settlement path should return a retryable outcome");

    match outcome {
        SettlementOutcome::PendingRetry { reason } => {
            assert!(reason.contains("Payout failed after 3 attempts"));
        }
        other => panic!("Expected retryable settlement outcome, got {:?}", other),
    }

    let (swap_status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(&swap_id)
        .fetch_one(&ctx.db)
        .await
        .unwrap();
    assert_eq!(swap_status, "funds_received");

    let address_info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .expect("Expected address info");
    assert_eq!(address_info.status, "failed");
    assert!(address_info.payout_tx_hash.is_none());

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_settlement_service_treats_processing_payout_as_in_progress() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let mnemonic = crate::common::test_wallet_mnemonic();
    let provider = Arc::new(SuccessfulBlockchainProvider);

    create_swap_for_settlement(
        &ctx.db,
        &swap_id,
        "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "sending",
    )
    .await;
    attach_internal_address(&ctx.db, &swap_id, &mnemonic, provider.clone()).await;

    sqlx::query("UPDATE swap_address_info SET status = 'processing' WHERE swap_id = ?")
        .bind(&swap_id)
        .execute(&ctx.db)
        .await
        .unwrap();

    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));
    let outcome = settlement_service
        .settle_swap(&swap_id, provider, Some(1.0))
        .await
        .expect("Settlement should recognize in-progress payout");

    match outcome {
        SettlementOutcome::PayoutInProgress => {}
        other => panic!("Expected in-progress settlement outcome, got {:?}", other),
    }

    let (swap_status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(&swap_id)
        .fetch_one(&ctx.db)
        .await
        .unwrap();
    assert_eq!(swap_status, "funds_received");

    ctx.cleanup().await;
}
