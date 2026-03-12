// =============================================================================
// INTEGRATION TESTS - PAYOUT FAILURE RECOVERY
// Tests retry logic, error handling, and failure status updates
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use common::TestContext;
use uuid::Uuid;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::{GenerateAddressRequest, PayoutRequest};
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, RpcError};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

// =============================================================================
// MOCK PROVIDER THAT FAILS
// =============================================================================

#[derive(Clone)]
struct FailingBlockchainProvider {
    fail_count: Arc<Mutex<usize>>,
    max_failures: usize,
}

impl FailingBlockchainProvider {
    fn new(max_failures: usize) -> Self {
        Self {
            fail_count: Arc::new(Mutex::new(0)),
            max_failures,
        }
    }
    
    fn get_attempt_count(&self) -> usize {
        *self.fail_count.lock().unwrap()
    }
}

#[async_trait]
impl BlockchainProvider for FailingBlockchainProvider {
    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(1.0) // Always return balance
    }

    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Ok(5)
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Ok(20_000_000_000)
    }

    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> {
        let mut count = self.fail_count.lock().unwrap();
        *count += 1;
        
        if *count <= self.max_failures {
            // Fail for the first N attempts
            Err(RpcError::Network(format!(
                "Simulated network error (attempt {})",
                *count
            )))
        } else {
            // Succeed after max_failures
            Ok("0xsuccess_after_retry".to_string())
        }
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

async fn create_swap_for_payout(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', 'ETH', 'ethereum',
                0.1, 1.0, 15.0, 'dep_addr', ?, 'funds_received')
        "#
    )
    .bind(swap_id)
    .bind(recipient)
    .execute(db)
    .await
    .expect("Failed to create swap");
}

// =============================================================================
// TEST 1: Payout Fails Without Retry (Current Behavior)
// =============================================================================

#[tokio::test]
async fn test_payout_fails_without_retry() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    
    create_swap_for_payout(&ctx.db, &swap_id, recipient).await;
    
    let seed_phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let failing_provider = Arc::new(FailingBlockchainProvider::new(999)); // Always fail
    let crud = WalletCrud::new(ctx.db.clone());
    let wallet_manager = WalletManager::new(
        crud.clone(),
        seed_phrase.to_string(),
        failing_provider.clone(),
    );
    
    // Generate address
    wallet_manager.get_or_generate_address(GenerateAddressRequest {
        swap_id: swap_id.clone(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: recipient.to_string(),
        user_recipient_extra_id: None,
    }).await.unwrap();
    
    // Attempt payout - should fail
    let result = wallet_manager.process_payout(PayoutRequest {
        swap_id: swap_id.clone(),
    }).await;
    
    assert!(result.is_err(), "Payout should fail with failing provider");
    println!("✅ Payout correctly fails when broadcast fails");
    println!("   Error: {}", result.unwrap_err());
    
    // Verify only 1 attempt was made (no retry)
    assert_eq!(
        failing_provider.get_attempt_count(),
        1,
        "Should only attempt once without retry logic"
    );
    
    ctx.cleanup().await;
}

// =============================================================================
// TEST 2: Payout Succeeds After Retry (To Be Implemented)
// =============================================================================

#[tokio::test]
async fn test_payout_succeeds_after_retry() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    
    create_swap_for_payout(&ctx.db, &swap_id, recipient).await;
    
    let seed_phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    // Fail 2 times, then succeed on 3rd attempt
    let failing_provider = Arc::new(FailingBlockchainProvider::new(2));
    let crud = WalletCrud::new(ctx.db.clone());
    let wallet_manager = WalletManager::new(
        crud.clone(),
        seed_phrase.to_string(),
        failing_provider.clone(),
    );
    
    // Generate address
    wallet_manager.get_or_generate_address(GenerateAddressRequest {
        swap_id: swap_id.clone(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: recipient.to_string(),
        user_recipient_extra_id: None,
    }).await.unwrap();
    
    // Attempt payout with retry - should succeed on 3rd attempt
    let result = wallet_manager.process_payout_with_retry(PayoutRequest {
        swap_id: swap_id.clone(),
    }, 3).await;
    
    assert!(result.is_ok(), "Payout should succeed after retries");
    println!("✅ Payout succeeded after {} attempts", failing_provider.get_attempt_count());
    
    let response = result.unwrap();
    assert_eq!(response.tx_hash, "0xsuccess_after_retry");
    
    // Verify 3 attempts were made
    assert_eq!(
        failing_provider.get_attempt_count(),
        3,
        "Should attempt 3 times before succeeding"
    );
    
    ctx.cleanup().await;
}

// =============================================================================
// TEST 3: Payout Fails After Max Retries (To Be Implemented)
// =============================================================================

#[tokio::test]
async fn test_payout_fails_after_max_retries() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    
    create_swap_for_payout(&ctx.db, &swap_id, recipient).await;
    
    let seed_phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let failing_provider = Arc::new(FailingBlockchainProvider::new(999)); // Always fail
    let crud = WalletCrud::new(ctx.db.clone());
    let wallet_manager = WalletManager::new(
        crud.clone(),
        seed_phrase.to_string(),
        failing_provider.clone(),
    );
    
    // Generate address
    wallet_manager.get_or_generate_address(GenerateAddressRequest {
        swap_id: swap_id.clone(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: recipient.to_string(),
        user_recipient_extra_id: None,
    }).await.unwrap();
    
    // Attempt payout with retry - should fail after 3 attempts
    let result = wallet_manager.process_payout_with_retry(PayoutRequest {
        swap_id: swap_id.clone(),
    }, 3).await;
    
    assert!(result.is_err(), "Payout should fail after max retries");
    println!("✅ Payout correctly fails after max retries");
    println!("   Attempts made: {}", failing_provider.get_attempt_count());
    
    // Verify 3 attempts were made
    assert_eq!(
        failing_provider.get_attempt_count(),
        3,
        "Should attempt exactly 3 times"
    );
    
    // Note: Swap status update to 'failed' happens in BlockchainListener, not in WalletManager
    // WalletManager just returns an error, and the caller (BlockchainListener) updates the status
    println!("✅ Payout correctly returns error after max retries");
    println!("   (Status update to 'failed' is handled by BlockchainListener)");
    
    ctx.cleanup().await;
}

// =============================================================================
// TEST 4: Exponential Backoff Timing (To Be Implemented)
// =============================================================================

#[tokio::test]
async fn test_exponential_backoff() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    
    create_swap_for_payout(&ctx.db, &swap_id, recipient).await;
    
    let seed_phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let failing_provider = Arc::new(FailingBlockchainProvider::new(2));
    let crud = WalletCrud::new(ctx.db.clone());
    let wallet_manager = WalletManager::new(
        crud,
        seed_phrase.to_string(),
        failing_provider.clone(),
    );
    
    // Generate address
    wallet_manager.get_or_generate_address(GenerateAddressRequest {
        swap_id: swap_id.clone(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: recipient.to_string(),
        user_recipient_extra_id: None,
    }).await.unwrap();
    
    let start = std::time::Instant::now();
    
    // Attempt payout with retry
    let result = wallet_manager.process_payout_with_retry(PayoutRequest {
        swap_id: swap_id.clone(),
    }, 3).await;
    
    let duration = start.elapsed();
    
    assert!(result.is_ok(), "Payout should succeed");
    
    // With exponential backoff: 1s + 2s = 3s minimum
    // (First attempt immediate, 2nd after 1s, 3rd after 2s)
    assert!(
        duration.as_secs() >= 3,
        "Should take at least 3 seconds with exponential backoff, took {}s",
        duration.as_secs()
    );
    
    println!("✅ Exponential backoff working: took {}s for 3 attempts", duration.as_secs());
    
    ctx.cleanup().await;
}

// =============================================================================
// TEST 5: Error Logging and Tracking (To Be Implemented)
// =============================================================================

#[tokio::test]
#[ignore] // Remove this once error tracking is implemented
async fn test_error_logging() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12";
    
    create_swap_for_payout(&ctx.db, &swap_id, recipient).await;
    
    let seed_phrase = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
    let failing_provider = Arc::new(FailingBlockchainProvider::new(999));
    let crud = WalletCrud::new(ctx.db.clone());
    let wallet_manager = WalletManager::new(
        crud,
        seed_phrase.to_string(),
        failing_provider,
    );
    
    // Generate address
    wallet_manager.get_or_generate_address(GenerateAddressRequest {
        swap_id: swap_id.clone(),
        ticker: "ETH".to_string(),
        network: "ethereum".to_string(),
        user_recipient_address: recipient.to_string(),
        user_recipient_extra_id: None,
    }).await.unwrap();
    
    // Attempt payout - will fail
    let _result = wallet_manager.process_payout_with_retry(PayoutRequest {
        swap_id: swap_id.clone(),
    }, 2).await;
    
    // Check if error was logged to payout_audit table
    let error_logs: Vec<(String, String)> = sqlx::query_as(
        "SELECT status, message FROM payout_audit WHERE swap_id = ? ORDER BY created_at"
    )
    .bind(&swap_id)
    .fetch_all(&ctx.db)
    .await
    .unwrap();
    
    assert!(!error_logs.is_empty(), "Should have error logs");
    assert!(
        error_logs.iter().any(|(status, _)| status == "failed"),
        "Should have 'failed' status in logs"
    );
    
    println!("✅ Error logging working: {} log entries", error_logs.len());
    for (status, msg) in error_logs {
        println!("   - {}: {}", status, msg);
    }
    
    ctx.cleanup().await;
}
