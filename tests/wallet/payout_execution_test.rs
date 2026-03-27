// =============================================================================
// INTEGRATION TESTS - PAYOUT EXECUTION
// Tests for transferring converted crypto to user's recipient address
// Flow: Trocador sends to us → We deduct commission → We send to user
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use alloy::{
    consensus::TxEnvelope,
    eips::eip2718::Decodable2718,
    primitives::{Address as AlloyAddress, TxKind, U256},
};
use async_trait::async_trait;
use common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::model::PayoutStatus;
use exchange_shared::modules::wallet::schema::{GenerateAddressRequest, PayoutRequest};
use exchange_shared::services::wallet::cosmos_rpc::supported_cosmos_chain;
use exchange_shared::services::wallet::derivation;
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{
    BlockchainProvider, CosmosAccountState, RpcError, TronContractCallResponse,
    TronContractTriggerResult, TronPreparedTransaction,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use uuid::Uuid;

// =============================================================================
// MOCK PROVIDER
// =============================================================================

#[derive(Clone)]
struct MockProvider {
    nonce: u64,
    gas_price: u64,
    broadcast_hash: String,
    native_balance: f64,
    broadcasted_txs: Arc<Mutex<Vec<String>>>,
    contract_calls: Arc<Mutex<Vec<(String, String)>>>,
    token_balance_hex: String,
    tron_constant_calls: Arc<Mutex<Vec<(String, String, String, String)>>>,
    tron_trigger_calls: Arc<Mutex<Vec<(String, String, String, String, u64)>>>,
    tron_broadcasts: Arc<Mutex<Vec<TronPreparedTransaction>>>,
    tron_token_balance_hex: String,
    cosmos_account_state: CosmosAccountState,
    cosmos_account_requests: Arc<Mutex<Vec<String>>>,
}

impl MockProvider {
    fn new() -> Self {
        Self {
            nonce: 5,
            gas_price: 20_000_000_000,
            broadcast_hash: "0xrealhash123".to_string(),
            native_balance: 1.0,
            broadcasted_txs: Arc::new(Mutex::new(Vec::new())),
            contract_calls: Arc::new(Mutex::new(Vec::new())),
            token_balance_hex: "0x0".to_string(),
            tron_constant_calls: Arc::new(Mutex::new(Vec::new())),
            tron_trigger_calls: Arc::new(Mutex::new(Vec::new())),
            tron_broadcasts: Arc::new(Mutex::new(Vec::new())),
            tron_token_balance_hex: "0".to_string(),
            cosmos_account_state: CosmosAccountState {
                account_number: 7,
                sequence: 11,
                chain_id: "neutron-1".to_string(),
            },
            cosmos_account_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_token_balance_hex(mut self, token_balance_hex: &str) -> Self {
        self.token_balance_hex = token_balance_hex.to_string();
        self
    }

    fn with_native_balance(mut self, native_balance: f64) -> Self {
        self.native_balance = native_balance;
        self
    }

    fn with_tron_token_balance_hex(mut self, tron_token_balance_hex: &str) -> Self {
        self.tron_token_balance_hex = tron_token_balance_hex.to_string();
        self
    }

    fn with_cosmos_account_state(
        mut self,
        account_number: u64,
        sequence: u64,
        chain_id: &str,
    ) -> Self {
        self.cosmos_account_state = CosmosAccountState {
            account_number,
            sequence,
            chain_id: chain_id.to_string(),
        };
        self
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

    async fn evm_call(&self, to_address: &str, data: &str) -> Result<String, RpcError> {
        self.contract_calls
            .lock()
            .unwrap()
            .push((to_address.to_string(), data.to_string()));
        Ok(self.token_balance_hex.clone())
    }

    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(self.native_balance)
    }

    async fn tron_create_transaction(
        &self,
        owner_address_hex: &str,
        to_address_hex: &str,
        amount_sun: u64,
    ) -> Result<TronPreparedTransaction, RpcError> {
        Ok(TronPreparedTransaction {
            tx_id: "11".repeat(32),
            raw_data: json!({
                "owner_address": owner_address_hex,
                "to_address": to_address_hex,
                "amount": amount_sun
            }),
            raw_data_hex: Some("deadbeef".to_string()),
            signature: Vec::new(),
            visible: Some(false),
        })
    }

    async fn tron_trigger_constant_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
    ) -> Result<TronContractCallResponse, RpcError> {
        self.tron_constant_calls.lock().unwrap().push((
            owner_address_hex.to_string(),
            contract_address_hex.to_string(),
            function_selector.to_string(),
            parameter_hex.to_string(),
        ));

        Ok(TronContractCallResponse {
            result: Some(TronContractTriggerResult {
                result: true,
                code: None,
                message: None,
            }),
            constant_result: vec![self.tron_token_balance_hex.clone()],
            transaction: None,
            energy_used: Some(12_345),
            energy_penalty: None,
            message: None,
        })
    }

    async fn tron_trigger_smart_contract(
        &self,
        owner_address_hex: &str,
        contract_address_hex: &str,
        function_selector: &str,
        parameter_hex: &str,
        fee_limit_sun: u64,
    ) -> Result<TronContractCallResponse, RpcError> {
        self.tron_trigger_calls.lock().unwrap().push((
            owner_address_hex.to_string(),
            contract_address_hex.to_string(),
            function_selector.to_string(),
            parameter_hex.to_string(),
            fee_limit_sun,
        ));

        Ok(TronContractCallResponse {
            result: Some(TronContractTriggerResult {
                result: true,
                code: None,
                message: None,
            }),
            constant_result: Vec::new(),
            transaction: Some(TronPreparedTransaction {
                tx_id: "22".repeat(32),
                raw_data: json!({
                    "contract_address": contract_address_hex,
                    "function_selector": function_selector,
                    "parameter": parameter_hex,
                    "owner_address": owner_address_hex
                }),
                raw_data_hex: Some("beadfeed".to_string()),
                signature: Vec::new(),
                visible: Some(false),
            }),
            energy_used: Some(20_000),
            energy_penalty: None,
            message: None,
        })
    }

    async fn tron_broadcast_transaction(
        &self,
        transaction: &TronPreparedTransaction,
    ) -> Result<String, RpcError> {
        self.tron_broadcasts
            .lock()
            .unwrap()
            .push(transaction.clone());
        Ok(self.broadcast_hash.clone())
    }

    async fn cosmos_get_account_state(
        &self,
        address: &str,
    ) -> Result<CosmosAccountState, RpcError> {
        self.cosmos_account_requests
            .lock()
            .unwrap()
            .push(address.to_string());
        Ok(self.cosmos_account_state.clone())
    }
}

#[derive(Clone)]
struct BlockingBroadcastProvider {
    attempts: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl BlockingBroadcastProvider {
    fn new() -> Self {
        Self {
            attempts: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }

    async fn wait_for_broadcast_attempt(&self) {
        self.started.notified().await;
    }

    fn release_broadcast(&self) {
        self.release.notify_waiters();
    }

    fn attempt_count(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl BlockchainProvider for BlockingBroadcastProvider {
    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> {
        Ok(5)
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        Ok(20_000_000_000)
    }

    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(1.0)
    }

    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.started.notify_waiters();
        self.release.notified().await;
        Ok("0xlockedsuccess".to_string())
    }
}

// Helper to create a dummy swap in DB
async fn create_payout_ready_swap(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
    amount: f64,
) {
    create_payout_ready_swap_for_route(db, swap_id, recipient, amount, "ETH", "ethereum").await;
}

async fn create_payout_ready_swap_for_route(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
    amount: f64,
    to_currency: &str,
    to_network: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address,
            platform_fee, total_fee, status
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', ?, ?, 0.1, ?, 15.0, 'dep_addr', ?, 0.01, 0.01, 'completed')
        "#
    )
    .bind(swap_id)
    .bind(to_currency)
    .bind(to_network)
    .bind(amount)
    .bind(recipient)
    .execute(db)
    .await
    .expect("Failed to create payout ready swap");
}

async fn insert_token_metadata(
    db: &sqlx::Pool<sqlx::MySql>,
    symbol: &str,
    network: &str,
    contract_address: &str,
    decimals: i32,
    token_type: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO tokens (
            symbol, name, network, contract_address, decimals, token_type,
            is_active, is_verified, gas_multiplier
        )
        VALUES (?, ?, ?, ?, ?, ?, TRUE, TRUE, 1.0)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            decimals = VALUES(decimals),
            token_type = VALUES(token_type),
            is_active = TRUE,
            is_verified = TRUE,
            gas_multiplier = VALUES(gas_multiplier)
        "#,
    )
    .bind(symbol)
    .bind(format!("{} Test Token", symbol))
    .bind(network)
    .bind(contract_address)
    .bind(decimals)
    .bind(token_type)
    .execute(db)
    .await
    .expect("Failed to insert token metadata");
}

async fn assert_native_evm_payout_route(ticker: &str, network: &str, expected_chain_id: u64) {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new());
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";

    create_payout_ready_swap_for_route(&ctx.db, &swap_id, recipient, 1.0, ticker, network).await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: ticker.to_string(),
            network: network.to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(response.status, PayoutStatus::Success);

    let broadcasted = mock_provider.broadcasted_txs.lock().unwrap().clone();
    assert_eq!(
        broadcasted.len(),
        1,
        "Expected one broadcasted native EVM tx"
    );

    let raw_tx = hex::decode(broadcasted[0].trim_start_matches("0x")).unwrap();
    let envelope = TxEnvelope::decode_2718(&mut raw_tx.as_slice()).unwrap();
    let signed = envelope.as_legacy().expect("Expected legacy EVM envelope");

    assert_eq!(signed.tx().chain_id, Some(expected_chain_id));
    assert!(
        matches!(signed.tx().to, TxKind::Call(address) if address == recipient.parse::<AlloyAddress>().unwrap()),
        "Expected native transfer recipient to match user address"
    );

    let expected_network_fee: f64 = 21_000.0 * 20_000_000_000.0 / 1_000_000_000_000_000_000.0;
    let expected_payout: f64 = 1.0 - 0.01 - expected_network_fee;
    let expected_value =
        U256::from((expected_payout * 1_000_000_000_000_000_000.0f64).round() as u128);
    assert_eq!(signed.tx().value, expected_value);

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

    ctx.cleanup().await;
}

async fn assert_native_cosmos_payout_route(
    ticker: &str,
    network: &str,
    chain_key: &str,
    chain_id: &str,
) {
    assert_native_cosmos_payout_route_with_balance(ticker, network, chain_key, chain_id, 1.0).await;
}

async fn assert_native_cosmos_payout_route_with_balance(
    ticker: &str,
    network: &str,
    chain_key: &str,
    chain_id: &str,
    actual_balance: f64,
) {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(
        MockProvider::new()
            .with_native_balance(actual_balance)
            .with_cosmos_account_state(7, 11, chain_id),
    );
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = derivation::derive_address(&seed_phrase, ticker, network, 77)
        .await
        .expect("valid cosmos recipient");

    create_payout_ready_swap_for_route(
        &ctx.db,
        &swap_id,
        &recipient,
        actual_balance,
        ticker,
        network,
    )
    .await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: ticker.to_string(),
            network: network.to_string(),
            user_recipient_address: recipient.clone(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    let our_address = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap()
        .our_address;

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    let route = supported_cosmos_chain(chain_key).expect("cosmos route config");
    let expected_network_fee = route.network_fee_native();
    let expected_payout = actual_balance - 0.01 - expected_network_fee;

    assert_eq!(response.status, PayoutStatus::Success);
    assert!(
        (response.amount - expected_payout).abs() < 0.0000001,
        "Expected {} payout, got {}",
        expected_payout,
        response.amount
    );

    let cosmos_requests = mock_provider
        .cosmos_account_requests
        .lock()
        .unwrap()
        .clone();
    assert_eq!(cosmos_requests, vec![our_address.clone()]);

    let broadcasted = mock_provider.broadcasted_txs.lock().unwrap().clone();
    assert_eq!(broadcasted.len(), 1, "Expected one broadcasted Cosmos tx");
    assert!(
        broadcasted[0].starts_with("0x"),
        "Expected hex-encoded Cosmos tx bytes"
    );
    let raw_tx = hex::decode(broadcasted[0].trim_start_matches("0x")).unwrap();
    assert!(
        raw_tx.len() > 100,
        "Expected a non-trivial signed Cosmos tx payload"
    );

    let info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.status, "success");
    assert_eq!(info.payout_tx_hash, Some("0xrealhash123".to_string()));
    assert_eq!(info.actual_received, Some(actual_balance));
    assert_eq!(info.commission_taken, Some(0.01));
    assert_eq!(info.network_fee_paid, Some(expected_network_fee));
    assert!(
        (info.payout_amount.expect("payout amount recorded") - expected_payout).abs() < 0.0000001
    );

    ctx.cleanup().await;
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
    let recipient = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
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
    let recipient = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
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

#[tokio::test]
async fn test_concurrent_payout_attempts_use_processing_lock() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let provider = Arc::new(BlockingBroadcastProvider::new());
    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";

    create_payout_ready_swap(&ctx.db, &swap_id, recipient, 1.0).await;

    let manager_a = WalletManager::new(
        WalletCrud::new(ctx.db.clone()),
        seed_phrase.to_string(),
        provider.clone(),
    );
    let manager_b = WalletManager::new(
        WalletCrud::new(ctx.db.clone()),
        seed_phrase.to_string(),
        provider.clone(),
    );

    manager_a
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    let first_swap_id = swap_id.clone();
    let first_task = tokio::spawn(async move {
        manager_a
            .process_payout(PayoutRequest {
                swap_id: first_swap_id,
            })
            .await
    });

    provider.wait_for_broadcast_attempt().await;

    let locked_info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(locked_info.status, "processing");

    let second_result = manager_b
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await;

    let second_error = second_result.expect_err("Second payout attempt should be locked out");
    assert!(second_error.contains("Payout already in progress"));

    provider.release_broadcast();

    let first_response = first_task.await.unwrap().unwrap();
    assert_eq!(first_response.tx_hash, "0xlockedsuccess");
    assert_eq!(
        provider.attempt_count(),
        1,
        "Only one broadcast should occur"
    );

    let final_info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(final_info.status, "success");
    assert_eq!(
        final_info.payout_tx_hash.as_deref(),
        Some("0xlockedsuccess")
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_erc20_token_payout_uses_contract_transfer() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new().with_token_balance_hex("0x0f4240"));
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
    let token_contract = "0xdAC17F958D2ee523a2206206994597C13D831ec7";

    create_payout_ready_swap_for_route(&ctx.db, &swap_id, recipient, 1.0, "USDT", "ERC20").await;
    insert_token_metadata(&ctx.db, "USDT", "ethereum", token_contract, 6, "ERC20").await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "USDT".to_string(),
            network: "ERC20".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(response.status, PayoutStatus::Success);
    assert!((response.amount - 0.99).abs() < 0.0000001);

    let contract_calls = mock_provider.contract_calls.lock().unwrap().clone();
    assert_eq!(contract_calls.len(), 1, "Expected one balanceOf eth_call");
    assert_eq!(contract_calls[0].0, token_contract);
    assert!(
        contract_calls[0].1.starts_with("0x70a08231"),
        "Expected balanceOf selector in eth_call data"
    );

    let broadcasted = mock_provider.broadcasted_txs.lock().unwrap().clone();
    assert_eq!(broadcasted.len(), 1, "Expected one broadcasted token tx");

    let raw_tx = hex::decode(broadcasted[0].trim_start_matches("0x")).unwrap();
    let envelope = TxEnvelope::decode_2718(&mut raw_tx.as_slice()).unwrap();
    let signed = envelope.as_legacy().expect("Expected legacy EVM envelope");
    let contract_address: AlloyAddress = token_contract.parse().unwrap();

    assert!(
        matches!(signed.tx().to, TxKind::Call(address) if address == contract_address),
        "Expected token transfer to target the token contract"
    );
    assert_eq!(signed.tx().value, U256::ZERO);

    let calldata = signed.tx().input.as_ref();
    assert!(calldata.starts_with(&hex::decode("a9059cbb").unwrap()));
    assert_eq!(calldata.len(), 68);

    let encoded_amount = U256::from_be_slice(&calldata[36..68]);
    assert_eq!(encoded_amount, U256::from(990_000u64));

    let info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.status, "success");
    assert_eq!(info.payout_tx_hash, Some("0xrealhash123".to_string()));
    assert_eq!(info.actual_received, Some(1.0));
    assert_eq!(info.commission_taken, Some(0.01));
    assert!(info.network_fee_paid.unwrap_or_default() > 0.0);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_metis_mainnet_native_payout_uses_standard_evm_architecture() {
    assert_native_evm_payout_route("METIS", "Mainnet", 1_088).await;
}

#[tokio::test]
async fn test_scroll_native_eth_payout_uses_standard_evm_architecture() {
    assert_native_evm_payout_route("ETH", "SCROLL", 534_352).await;
}

#[tokio::test]
async fn test_supra_mainnet_native_payout_uses_standard_evm_architecture() {
    assert_native_evm_payout_route("SUPRA", "Mainnet", 523_994_005_626).await;
}

#[tokio::test]
async fn test_neutron_mainnet_native_payout_uses_standard_cosmos_architecture() {
    assert_native_cosmos_payout_route("NTRN", "MAINNET", "neutron", "neutron-1").await;
}

#[tokio::test]
async fn test_dymension_mainnet_native_payout_uses_standard_cosmos_architecture() {
    assert_native_cosmos_payout_route("DYM", "MAINNET", "dymension", "dymension_1100-1").await;
}

#[tokio::test]
async fn test_coreum_mainnet_native_payout_uses_standard_cosmos_architecture() {
    assert_native_cosmos_payout_route("COREUM", "MAINNET", "coreum", "coreum-mainnet-1").await;
}

#[tokio::test]
async fn test_initia_mainnet_native_payout_uses_standard_cosmos_architecture() {
    assert_native_cosmos_payout_route("INIT", "MAINNET", "initia", "interwoven-1").await;
}

#[tokio::test]
async fn test_kyve_mainnet_native_payout_uses_standard_cosmos_architecture() {
    assert_native_cosmos_payout_route_with_balance("KYVE", "MAINNET", "kyve", "kyve-1", 20.0).await;
}

#[tokio::test]
async fn test_trc20_token_payout_uses_tron_contract_transfer() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(
        MockProvider::new()
            .with_native_balance(50.0)
            .with_tron_token_balance_hex("0f4240"),
    );
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "TQn9Y2khEsLJW1ChVWFMSMeRDow5KcbLSE";
    let token_contract = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";

    create_payout_ready_swap_for_route(&ctx.db, &swap_id, recipient, 1.0, "USDT", "TRC20").await;
    insert_token_metadata(&ctx.db, "USDT", "tron", token_contract, 6, "TRC20").await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "USDT".to_string(),
            network: "TRC20".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .unwrap();

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(response.status, PayoutStatus::Success);
    assert!((response.amount - 0.99).abs() < 0.0000001);

    let constant_calls = mock_provider.tron_constant_calls.lock().unwrap().clone();
    assert_eq!(constant_calls.len(), 1, "Expected one TRC20 balanceOf call");
    assert_eq!(constant_calls[0].2, "balanceOf(address)");
    assert_eq!(
        constant_calls[0].1,
        "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
    );
    assert_eq!(constant_calls[0].3.len(), 64);

    let trigger_calls = mock_provider.tron_trigger_calls.lock().unwrap().clone();
    assert_eq!(
        trigger_calls.len(),
        1,
        "Expected one TRC20 transfer trigger"
    );
    assert_eq!(trigger_calls[0].2, "transfer(address,uint256)");
    assert_eq!(
        trigger_calls[0].1,
        "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
    );
    assert_eq!(trigger_calls[0].3.len(), 128);
    assert!(trigger_calls[0].4 > 0);
    assert_eq!(&trigger_calls[0].3[64..], &format!("{:064x}", 990_000u64));

    let broadcasts = mock_provider.tron_broadcasts.lock().unwrap().clone();
    assert_eq!(broadcasts.len(), 1, "Expected one broadcasted TRC20 tx");
    assert_eq!(broadcasts[0].signature.len(), 1);

    let info = WalletCrud::new(ctx.db.clone())
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.status, "success");
    assert_eq!(info.payout_tx_hash, Some("0xrealhash123".to_string()));
    assert_eq!(info.actual_received, Some(1.0));
    assert_eq!(info.commission_taken, Some(0.01));
    assert!(info.network_fee_paid.unwrap_or_default() > 0.0);

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_xrp_payout_includes_destination_tag() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new().with_native_balance(25.0));
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "rEb8TK3gBgk5auZkwc6sHnwrGVJH8DuaLh";

    create_payout_ready_swap_for_route(&ctx.db, &swap_id, recipient, 25.0, "XRP", "Mainnet").await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "XRP".to_string(),
            network: "Mainnet".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: Some("123456789".to_string()),
        })
        .await
        .unwrap();

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(response.status, PayoutStatus::Success);

    let broadcasted = mock_provider.broadcasted_txs.lock().unwrap().clone();
    assert_eq!(broadcasted.len(), 1, "Expected one XRP broadcast");

    let payload: Value = serde_json::from_str(&broadcasted[0]).expect("XRP payload should be JSON");
    assert_eq!(
        payload["DestinationTag"].as_u64(),
        Some(123_456_789),
        "XRP destination tag should be included in broadcast payload"
    );
    assert_eq!(payload["Destination"].as_str(), Some(recipient));

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_stellar_payout_includes_memo() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let crud = WalletCrud::new(ctx.db.clone());
    let mock_provider = Arc::new(MockProvider::new().with_native_balance(5.0));
    let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());

    let swap_id = Uuid::new_v4().to_string();
    let recipient = "GBRPYHIL2C7GQ4AKN6K4R2JDTJXQ3Y2B6A3JJ2X6JHPV6SKV7H6TNNVH";
    let memo = "customer-42";

    create_payout_ready_swap_for_route(&ctx.db, &swap_id, recipient, 5.0, "XLM", "Mainnet").await;

    manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: swap_id.clone(),
            ticker: "XLM".to_string(),
            network: "Mainnet".to_string(),
            user_recipient_address: recipient.to_string(),
            user_recipient_extra_id: Some(memo.to_string()),
        })
        .await
        .unwrap();

    let response = manager
        .process_payout(PayoutRequest {
            swap_id: swap_id.clone(),
        })
        .await
        .unwrap();

    assert_eq!(response.status, PayoutStatus::Success);

    let broadcasted = mock_provider.broadcasted_txs.lock().unwrap().clone();
    assert_eq!(broadcasted.len(), 1, "Expected one Stellar broadcast");

    let payload: Value =
        serde_json::from_str(&broadcasted[0]).expect("Stellar payload should be JSON");
    assert_eq!(
        payload["transaction"]["memo"].as_str(),
        Some(memo),
        "Stellar memo should be included in broadcast payload"
    );
    assert_eq!(
        payload["transaction"]["operations"][0]["destination"].as_str(),
        Some(recipient)
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn test_placeholder_payout_routes_fail_closed_without_broadcast() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();

    let cases = vec![
        ("ADA", "cardano", "Cardano"),
        ("XTZ", "tezos", "Tezos"),
        ("ATOM", "cosmos", "Cosmos"),
        ("DOT", "polkadot", "Substrate"),
        ("STX", "stacks", "Stacks"),
        ("TON", "ton", "TON"),
        ("WAVES", "waves", "Waves"),
    ];

    for (index, (ticker, network, route_name)) in cases.into_iter().enumerate() {
        let crud = WalletCrud::new(ctx.db.clone());
        let mock_provider = Arc::new(MockProvider::new().with_native_balance(10.0));
        let manager = WalletManager::new(crud, seed_phrase.to_string(), mock_provider.clone());
        let swap_id = Uuid::new_v4().to_string();
        let recipient =
            derivation::derive_address(&seed_phrase, ticker, network, 10_000 + index as u32)
                .await
                .expect("recipient derivation should succeed");

        create_payout_ready_swap_for_route(&ctx.db, &swap_id, &recipient, 10.0, ticker, network)
            .await;

        manager
            .get_or_generate_address(GenerateAddressRequest {
                swap_id: swap_id.clone(),
                ticker: ticker.to_string(),
                network: network.to_string(),
                user_recipient_address: recipient,
                user_recipient_extra_id: None,
            })
            .await
            .unwrap();

        let error = manager
            .process_payout(PayoutRequest {
                swap_id: swap_id.clone(),
            })
            .await
            .expect_err("placeholder route should fail closed");

        assert!(
            error.contains("chain-native transaction builder"),
            "Expected fail-closed builder error for {ticker}/{network}, got: {error}"
        );
        assert!(
            error.contains(route_name),
            "Expected route name {route_name} in error, got: {error}"
        );
        assert!(
            mock_provider.broadcasted_txs.lock().unwrap().is_empty(),
            "No raw transaction should be broadcast for {ticker}/{network}"
        );
        assert!(
            mock_provider.tron_broadcasts.lock().unwrap().is_empty(),
            "No Tron transaction should be broadcast for {ticker}/{network}"
        );

        let info = WalletCrud::new(ctx.db.clone())
            .get_address_info(&swap_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(info.status, "failed");
        assert!(info.payout_tx_hash.is_none());
    }

    ctx.cleanup().await;
}
