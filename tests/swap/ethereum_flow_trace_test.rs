use crate::common::{test_wallet_mnemonic, timed_get, timed_post, TestContext};
use alloy::{
    consensus::TxEnvelope,
    eips::eip2718::Decodable2718,
    primitives::{Address as AlloyAddress, TxKind, U256},
};
use async_trait::async_trait;
use exchange_shared::modules::monitor::model::PollingState;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::GenerateAddressRequest;
use exchange_shared::services::blockchain::BlockchainListener;
use exchange_shared::services::monitor::MonitorEngine;
use exchange_shared::services::rpc::{
    build_default_rpc_configs, build_provider_for_asset, RpcManager,
};
use exchange_shared::services::settlement::{SettlementOutcome, SettlementService};
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, HttpRpcClient, RpcError};
use serde_json::{json, Value};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

const ETHEREUM_RECIPIENT: &str = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
const BTC_REFUND_ADDRESS: &str = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";
const EXPECTED_CREATE_REJECTION: &str = "Recipient address is invalid for eth on ERC20";
const LISTENER_PROBE_ADDRESS: &str = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
const SYNTHETIC_PROVIDER_ID: &str = "changenow";
const SYNTHETIC_SETTLEMENT_BALANCE: f64 = 1.0;
const CREATE_RATE_LIMIT_RETRIES: usize = 3;

struct CreateAttemptTrace {
    trade_id: String,
    rate_provider: String,
    quoted_receive: f64,
    create_status: u16,
    error_message: Option<String>,
    swap_id: Option<String>,
    provider_swap_id: Option<String>,
    deposit_address: Option<String>,
    internal_payout_address: Option<String>,
}

#[derive(Clone)]
struct LiveSettlementEthereumProvider {
    inner: Arc<HttpRpcClient>,
    forced_balance: f64,
    broadcasted_txs: Arc<Mutex<Vec<String>>>,
}

impl LiveSettlementEthereumProvider {
    fn new(inner: Arc<HttpRpcClient>, forced_balance: f64) -> Self {
        Self {
            inner,
            forced_balance,
            broadcasted_txs: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockchainProvider for LiveSettlementEthereumProvider {
    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> {
        Ok(self.forced_balance)
    }

    async fn get_transaction_count(&self, address: &str) -> Result<u64, RpcError> {
        self.inner.get_transaction_count(address).await
    }

    async fn get_gas_price(&self) -> Result<u64, RpcError> {
        self.inner.get_gas_price().await
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        self.broadcasted_txs
            .lock()
            .unwrap()
            .push(signed_hex.to_string());
        self.inner.send_raw_transaction(signed_hex).await
    }
}

async fn execute_live_ethereum_create_attempt(
    ctx: &TestContext,
    wallet_crud: &WalletCrud,
) -> CreateAttemptTrace {
    println!();
    println!("================ ETHEREUM FULL FLOW TRACE ================");
    println!("USER");
    println!("  wants to swap 0.005 btc on Mainnet into eth on ERC20");
    println!("  enters recipient address {}", ETHEREUM_RECIPIENT);
    println!("  enters refund address {}", BTC_REFUND_ADDRESS);

    let rates_path =
        "/swap/rates?from=btc&to=eth&amount=0.005&network_from=Mainnet&network_to=ERC20";

    println!("PLATFORM");
    println!("  asks Trocador for a live quote via {}", rates_path);
    let rate_response = timed_get(&ctx.server, rates_path).await;
    rate_response.assert_status_ok();

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"]
        .as_str()
        .expect("rate response should include trade_id")
        .to_string();
    let rate_provider = rate_json["rates"][0]["provider"]
        .as_str()
        .expect("rate response should include provider")
        .to_string();
    let quoted_receive = rate_json["rates"][0]["estimated_amount"]
        .as_f64()
        .expect("rate response should include estimated_amount");

    println!("TROCADOR");
    println!("  returned trade_id {}", trade_id);
    println!("  best provider for this quote is {}", rate_provider);
    println!("  estimated payout is {}", quoted_receive);

    let create_payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "eth",
        "network_to": "ERC20",
        "amount": 0.005,
        "provider": rate_provider,
        "recipient_address": ETHEREUM_RECIPIENT,
        "refund_address": BTC_REFUND_ADDRESS,
        "rate_type": "floating"
    });

    println!("USER");
    println!("  confirms the swap and submits /swap/create");
    println!("PLATFORM");
    println!("  validates the recipient locally and with Trocador");
    println!("  decides whether payout will be direct-settlement or provider-managed");
    let mut attempt = 1usize;
    let (create_status, create_json): (u16, Value) = loop {
        let create_response = timed_post(&ctx.server, "/swap/create", &create_payload).await;
        let status = create_response.status_code().as_u16();

        if (200..300).contains(&status) {
            break (status, create_response.json());
        }

        let error_json: Value = create_response.json();
        let error_message = error_json["error"]
            .as_str()
            .unwrap_or("unknown create error")
            .to_string();

        if error_message.contains("Rate limit exceeded") && attempt < CREATE_RATE_LIMIT_RETRIES {
            println!("PLATFORM");
            println!(
                "  Trocador rate-limited /swap/create on attempt {}. Waiting 5s before retrying.",
                attempt
            );
            attempt += 1;
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        if status == 400 {
            println!("PLATFORM");
            println!("  rejected swap creation before persisting a live ETH swap");
            println!("  error: {}", error_message);
            println!("TROCADOR");
            println!("  did not receive a /new_trade request in this branch");

            return CreateAttemptTrace {
                trade_id,
                rate_provider,
                quoted_receive,
                create_status: status,
                error_message: Some(error_message),
                swap_id: None,
                provider_swap_id: None,
                deposit_address: None,
                internal_payout_address: None,
            };
        }

        panic!(
            "unexpected live ETH create failure: status={} error={}",
            status, error_message
        );
    };

    let swap_id = create_json["swap_id"]
        .as_str()
        .expect("create response should include swap_id")
        .to_string();
    let deposit_address = create_json["deposit_address"]
        .as_str()
        .expect("create response should include deposit_address")
        .to_string();

    let status_path = format!("/swap/{}", swap_id);
    println!("PLATFORM");
    println!("  fetches live swap status via {}", status_path);
    let status_response = timed_get(&ctx.server, &status_path).await;
    status_response.assert_status_ok();

    let status_json: Value = status_response.json();
    let provider_swap_id = status_json["provider_swap_id"]
        .as_str()
        .expect("status response should include provider_swap_id")
        .to_string();

    let address_info = wallet_crud
        .get_address_info(&swap_id)
        .await
        .expect("wallet lookup should succeed")
        .expect("direct settlement should create swap_address_info");

    let trade = fetch_trade_status_payload(&provider_swap_id).await;
    let address_provider = trade_string_field(&trade, "address_provider");
    let address_user = trade_string_field(&trade, "address_user");

    println!("PLATFORM");
    println!("  created swap {}", swap_id);
    println!("  returned deposit address {}", deposit_address);
    println!(
        "  stored internal Ethereum payout address {}",
        address_info.our_address
    );
    println!("TROCADOR");
    println!("  provider deposit address is {}", address_provider);
    println!("  payout target currently configured is {}", address_user);

    assert_eq!(address_provider, deposit_address);
    assert_eq!(address_user, address_info.our_address);
    assert_eq!(address_info.recipient_address, ETHEREUM_RECIPIENT);

    CreateAttemptTrace {
        trade_id,
        rate_provider,
        quoted_receive,
        create_status,
        error_message: None,
        swap_id: Some(swap_id),
        provider_swap_id: Some(provider_swap_id),
        deposit_address: Some(deposit_address),
        internal_payout_address: Some(address_info.our_address),
    }
}

async fn fetch_trade_status_payload(trade_id: &str) -> Value {
    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY must be set");
    let response = reqwest::Client::new()
        .get("https://api.trocador.app/trade")
        .header("API-Key", api_key)
        .query(&[("id", trade_id)])
        .send()
        .await
        .expect("trocador trade request should succeed");

    let status = response.status();
    let response_text = response
        .text()
        .await
        .expect("trocador trade response body should be readable");

    println!("TROCADOR raw /trade payload: {}", response_text);

    assert!(
        status.is_success(),
        "expected successful Trocador /trade response, got {} with body {}",
        status,
        response_text
    );

    serde_json::from_str(&response_text).expect("trocador trade payload should be JSON")
}

fn trade_string_field<'a>(payload: &'a Value, key: &str) -> &'a str {
    let payload = match payload {
        Value::Array(items) => items
            .first()
            .unwrap_or_else(|| panic!("trade payload array was empty: {}", payload)),
        other => other,
    };

    payload
        .get(key)
        .or_else(|| payload.get("trade").and_then(|trade| trade.get(key)))
        .or_else(|| payload.get("result").and_then(|trade| trade.get(key)))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing string field '{}' in trade payload: {}",
                key, payload
            )
        })
}

async fn create_seeded_ethereum_settlement_swap(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    recipient: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, provider_swap_id, from_currency, from_network,
            to_currency, to_network, amount, estimated_receive, platform_fee,
            network_fee, total_fee, rate, deposit_address, recipient_address, status
        )
        VALUES (?, ?, 'eth-flow-trace-trade', 'BTC', 'Mainnet',
                'ETH', 'ethereum', 0.005, 0.98958, 0.01, 0.00042, 0.01042, 15.0,
                'trocador_deposit_addr', ?, 'sending')
        "#,
    )
    .bind(swap_id)
    .bind(SYNTHETIC_PROVIDER_ID)
    .bind(recipient)
    .execute(db)
    .await
    .expect("failed to seed ethereum settlement swap");
}

async fn create_listener_probe_swap(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    funded_address: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, provider_swap_id, from_currency, from_network,
            to_currency, to_network, amount, estimated_receive, platform_fee,
            network_fee, total_fee, rate, deposit_address, recipient_address, status
        )
        VALUES (?, ?, 'eth-listener-probe', 'BTC', 'Mainnet',
                'ETH', 'ethereum', 0.005, 0.01, 0.0, 0.0, 0.0, 15.0,
                'trocador_deposit_addr', ?, 'sending')
        "#,
    )
    .bind(swap_id)
    .bind(SYNTHETIC_PROVIDER_ID)
    .bind(ETHEREUM_RECIPIENT)
    .execute(db)
    .await
    .expect("failed to seed listener probe swap");

    sqlx::query(
        r#"
        INSERT INTO swap_address_info (
            swap_id, our_address, address_index, blockchain_id, coin_type, recipient_address, status
        )
        VALUES (?, ?, 0, 1, 60, ?, 'pending')
        "#,
    )
    .bind(swap_id)
    .bind(funded_address)
    .bind(ETHEREUM_RECIPIENT)
    .execute(db)
    .await
    .expect("failed to seed listener probe address info");
}

async fn fetch_swap_status(db: &sqlx::Pool<sqlx::MySql>, swap_id: &str) -> String {
    let (status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(swap_id)
        .fetch_one(db)
        .await
        .expect("swap status should be queryable");
    status
}

async fn fetch_polling_state(db: &sqlx::Pool<sqlx::MySql>, swap_id: &str) -> PollingState {
    sqlx::query_as::<_, PollingState>("SELECT * FROM polling_states WHERE swap_id = ?")
        .bind(swap_id)
        .fetch_one(db)
        .await
        .expect("polling state should be persisted")
}

async fn clear_monitor_lock(ctx: &TestContext, swap_id: &str) {
    let lock_key = format!("lock:monitor:{}", swap_id);
    let mut conn = ctx
        .redis
        .get_client()
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection should be available for monitor lock cleanup");
    let _: i32 = redis::cmd("DEL")
        .arg(&lock_key)
        .query_async(&mut conn)
        .await
        .expect("monitor lock cleanup should succeed");
}

fn assert_insufficient_funds_message(reason: &str) {
    assert!(
        reason.contains("insufficient funds")
            || reason.contains("Insufficient balance")
            || reason.contains("Failed to broadcast"),
        "expected a safe no-funds rejection, got '{}'",
        reason
    );
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY, WALLET_MNEMONIC, database, Redis, and live Ethereum RPC access; traces the full ETH flow until the money-required boundary"]
async fn ethereum_full_flow_trace_live_until_money_boundary() {
    dotenvy::dotenv().ok();

    let ctx = TestContext::new().await;
    let wallet_crud = WalletCrud::new(ctx.db.clone());
    let mnemonic = test_wallet_mnemonic();
    let rpc_manager = Arc::new(RpcManager::new(build_default_rpc_configs()));
    let live_provider = build_provider_for_asset(rpc_manager.clone(), "ETH", "ethereum")
        .await
        .expect("Ethereum RPC provider should be configured");
    let live_endpoint = rpc_manager
        .select_endpoint("ethereum")
        .await
        .expect("Ethereum endpoint selection should succeed");
    let live_http = Arc::new(HttpRpcClient::new(live_endpoint.clone()));

    let create_trace = execute_live_ethereum_create_attempt(&ctx, &wallet_crud).await;

    println!("ASSERTIONS");
    println!("  trade id: {}", create_trace.trade_id);
    println!("  quoted receive: {}", create_trace.quoted_receive);
    println!("  quote provider: {}", create_trace.rate_provider);
    println!("  create status: {}", create_trace.create_status);

    let payout_swap_id = if let Some(swap_id) = create_trace.swap_id.clone() {
        println!("  live swap id: {}", swap_id);
        println!(
            "  live provider swap id: {}",
            create_trace
                .provider_swap_id
                .as_deref()
                .unwrap_or("<missing>")
        );
        println!(
            "  live deposit address: {}",
            create_trace
                .deposit_address
                .as_deref()
                .unwrap_or("<missing>")
        );
        println!(
            "  live internal payout address: {}",
            create_trace
                .internal_payout_address
                .as_deref()
                .unwrap_or("<missing>")
        );
        swap_id
    } else {
        let error_message = create_trace
            .error_message
            .as_deref()
            .expect("rejected create must include an error");
        println!("  live create rejection: {}", error_message);
        assert!(
            error_message.contains(EXPECTED_CREATE_REJECTION),
            "expected ETH live create rejection to contain '{}', got '{}'",
            EXPECTED_CREATE_REJECTION,
            error_message
        );

        let synthetic_swap_id = Uuid::new_v4().to_string();
        println!("PLATFORM");
        println!(
            "  seeds synthetic swap {} so the local ETH path can continue after the current live create bug",
            synthetic_swap_id
        );
        create_seeded_ethereum_settlement_swap(&ctx.db, &synthetic_swap_id, ETHEREUM_RECIPIENT)
            .await;
        synthetic_swap_id
    };

    let wallet_manager =
        WalletManager::new(wallet_crud.clone(), mnemonic.clone(), live_provider.clone());
    println!("PLATFORM");
    println!("  ensures an internal Ethereum address exists for local settlement");
    let generated = wallet_manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: payout_swap_id.clone(),
            ticker: "ETH".to_string(),
            network: "ethereum".to_string(),
            user_recipient_address: ETHEREUM_RECIPIENT.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .expect("ethereum internal address generation should succeed");

    let address_info = wallet_crud
        .get_address_info(&payout_swap_id)
        .await
        .unwrap()
        .expect("address info should exist after generation");

    println!("PLATFORM");
    println!("  internal settlement address is {}", generated.address);
    println!(
        "  original user recipient stays {}",
        address_info.recipient_address
    );
    assert_eq!(generated.address, address_info.our_address);
    assert_eq!(address_info.recipient_address, ETHEREUM_RECIPIENT);

    println!("PLATFORM");
    println!(
        "  connects to live Ethereum RPC {} and reads the generated address state",
        live_endpoint
    );
    let live_balance = live_provider
        .get_balance(&address_info.our_address)
        .await
        .expect("live ethereum balance lookup should succeed");
    let live_nonce = live_provider
        .get_transaction_count(&address_info.our_address)
        .await
        .expect("live ethereum nonce lookup should succeed");
    let live_gas_price = live_provider
        .get_gas_price()
        .await
        .expect("live ethereum gas lookup should succeed");

    println!("RPC");
    println!("  generated address balance: {} ETH", live_balance);
    println!("  generated address nonce: {}", live_nonce);
    println!(
        "  current gas price: {} gwei",
        live_gas_price as f64 / 1_000_000_000.0
    );
    assert!(live_gas_price > 0, "gas price should be positive");

    let listener_swap_id = Uuid::new_v4().to_string();
    println!("PLATFORM");
    println!(
        "  seeds listener probe swap {} against a known funded ETH address to prove monitoring",
        listener_swap_id
    );
    create_listener_probe_swap(&ctx.db, &listener_swap_id, LISTENER_PROBE_ADDRESS).await;

    let listener = BlockchainListener::new(ctx.db.clone(), rpc_manager.clone());
    println!("PLATFORM");
    println!("  runs BlockchainListener.check_pending_swaps() using live Ethereum RPC");
    listener
        .check_pending_swaps()
        .await
        .expect("blockchain listener should complete successfully");

    let (listener_status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(&listener_swap_id)
        .fetch_one(&ctx.db)
        .await
        .expect("listener probe swap should remain queryable");

    let listener_info = wallet_crud
        .get_address_info(&listener_swap_id)
        .await
        .unwrap()
        .expect("listener probe address info should exist");

    println!("MONITOR");
    println!(
        "  listener probe swap status after scan: {}",
        listener_status
    );
    println!(
        "  listener probe payout status remains {} because no mnemonic was configured for auto-payout",
        listener_info.status
    );
    assert_eq!(listener_status, "funds_received");
    assert_eq!(listener_info.status, "pending");

    let settlement_provider = Arc::new(LiveSettlementEthereumProvider::new(
        live_http.clone(),
        SYNTHETIC_SETTLEMENT_BALANCE,
    ));
    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));

    println!("PLATFORM");
    println!(
        "  starts local settlement for swap {} with synthetic receipt {}, but real nonce/gas/send RPC calls",
        payout_swap_id, SYNTHETIC_SETTLEMENT_BALANCE
    );
    let outcome = settlement_service
        .settle_swap(
            &payout_swap_id,
            settlement_provider.clone(),
            Some(SYNTHETIC_SETTLEMENT_BALANCE),
        )
        .await
        .expect("settlement service should return a retryable outcome");

    let failure_reason = match outcome {
        SettlementOutcome::PendingRetry { reason } => reason,
        SettlementOutcome::Completed(response) => {
            panic!(
                "unexpectedly completed a real Ethereum payout without funding: {:?}",
                response
            )
        }
        SettlementOutcome::AlreadyCompleted => {
            panic!("settlement should not already be completed for this test swap")
        }
        SettlementOutcome::AwaitingPayout => {
            panic!("wallet mnemonic is configured, settlement should not stay awaiting payout")
        }
        SettlementOutcome::PayoutInProgress => {
            panic!("settlement should not already be in progress for a fresh test swap")
        }
    };

    println!("PLATFORM");
    println!(
        "  settlement stopped safely with retryable reason: {}",
        failure_reason
    );
    assert_insufficient_funds_message(&failure_reason);

    let broadcasted = settlement_provider.broadcasted_txs.lock().unwrap().clone();
    assert!(
        !broadcasted.is_empty(),
        "expected at least one live RPC broadcast attempt"
    );

    let first_raw = broadcasted
        .first()
        .expect("captured raw transaction should exist");
    let raw_tx = hex::decode(first_raw.trim_start_matches("0x")).unwrap();
    let envelope = TxEnvelope::decode_2718(&mut raw_tx.as_slice()).unwrap();
    let signed = envelope
        .as_legacy()
        .expect("expected legacy EVM transaction");

    let service_fee = wallet_crud
        .get_payout_fee_quote(&payout_swap_id)
        .await
        .unwrap()
        .expect("payout fee quote should exist")
        .platform_fee;

    let signed_network_fee =
        signed.tx().gas_limit as f64 * signed.tx().gas_price as f64 / 1_000_000_000_000_000_000.0;
    let expected_payout = SYNTHETIC_SETTLEMENT_BALANCE - service_fee - signed_network_fee;
    let expected_value =
        U256::from((expected_payout * 1_000_000_000_000_000_000.0f64).round() as u128);

    println!("PLATFORM");
    println!("  signed tx gas limit: {}", signed.tx().gas_limit);
    println!(
        "  signed tx gas price: {} gwei",
        signed.tx().gas_price as f64 / 1_000_000_000.0
    );
    println!("  implied network fee: {}", signed_network_fee);
    println!("  service fee deducted: {}", service_fee);
    println!("  final payout amount encoded in tx: {}", expected_payout);

    assert!(
        matches!(
            signed.tx().to,
            TxKind::Call(address) if address == ETHEREUM_RECIPIENT.parse::<AlloyAddress>().unwrap()
        ),
        "final Ethereum payout must target the user's original address"
    );
    assert_eq!(signed.tx().value, expected_value);

    let final_info = wallet_crud
        .get_address_info(&payout_swap_id)
        .await
        .unwrap()
        .expect("final address info should exist");
    let (final_swap_status,): (String,) = sqlx::query_as("SELECT status FROM swaps WHERE id = ?")
        .bind(&payout_swap_id)
        .fetch_one(&ctx.db)
        .await
        .unwrap();

    println!("ASSERTIONS");
    println!(
        "  payout swap status after no-funds settlement attempt: {}",
        final_swap_status
    );
    println!("  payout address record status: {}", final_info.status);
    println!("  payout tx hash: {:?}", final_info.payout_tx_hash);
    println!(
        "  actual received recorded: {:?}",
        final_info.actual_received
    );
    println!("  commission recorded: {:?}", final_info.commission_taken);
    println!("  network fee recorded: {:?}", final_info.network_fee_paid);
    println!("  payout amount recorded: {:?}", final_info.payout_amount);

    assert_eq!(final_swap_status, "funds_received");
    assert_eq!(final_info.status, "failed");
    assert!(final_info.payout_tx_hash.is_none());
    assert!(final_info.actual_received.is_none());
    assert!(final_info.commission_taken.is_none());
    assert!(final_info.network_fee_paid.is_none());
    assert!(final_info.payout_amount.is_none());

    ctx.cleanup().await;
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY, WALLET_MNEMONIC, database, Redis, and live Ethereum RPC access; traces the ETH monitor poller against Trocador and into local settlement"]
async fn ethereum_monitor_engine_trace_live_until_money_boundary() {
    dotenvy::dotenv().ok();

    let ctx = TestContext::new().await;
    let wallet_crud = WalletCrud::new(ctx.db.clone());
    let mnemonic = test_wallet_mnemonic();
    let rpc_manager = Arc::new(RpcManager::new(build_default_rpc_configs()));
    let live_provider = build_provider_for_asset(rpc_manager.clone(), "ETH", "ethereum")
        .await
        .expect("Ethereum RPC provider should be configured");
    let live_endpoint = rpc_manager
        .select_endpoint("ethereum")
        .await
        .expect("Ethereum endpoint selection should succeed");
    let live_http = Arc::new(HttpRpcClient::new(live_endpoint.clone()));
    let engine = MonitorEngine::new(
        ctx.db.clone(),
        Some(ctx.redis.clone()),
        Some(mnemonic.clone()),
        rpc_manager.clone(),
    );

    let create_trace = execute_live_ethereum_create_attempt(&ctx, &wallet_crud).await;
    let swap_id = create_trace
        .swap_id
        .clone()
        .expect("live ETH create must succeed before monitor trace can run");
    let provider_swap_id = create_trace
        .provider_swap_id
        .clone()
        .expect("live ETH create should persist provider_swap_id");

    println!("ASSERTIONS");
    println!("  live swap id: {}", swap_id);
    println!("  live provider swap id: {}", provider_swap_id);
    println!(
        "  live internal payout address: {}",
        create_trace
            .internal_payout_address
            .as_deref()
            .unwrap_or("<missing>")
    );

    let address_info = wallet_crud
        .get_address_info(&swap_id)
        .await
        .expect("address info lookup should succeed")
        .expect("live ETH swap should keep direct-settlement address info");
    println!("PLATFORM");
    println!(
        "  connects to live Ethereum RPC {} and reads the internal payout address state",
        live_endpoint
    );
    let live_balance = live_provider
        .get_balance(&address_info.our_address)
        .await
        .expect("live ethereum balance lookup should succeed");
    let live_nonce = live_provider
        .get_transaction_count(&address_info.our_address)
        .await
        .expect("live ethereum nonce lookup should succeed");
    let live_gas_price = live_provider
        .get_gas_price()
        .await
        .expect("live ethereum gas lookup should succeed");

    println!("RPC");
    println!(
        "  internal payout address on-chain balance: {} ETH",
        live_balance
    );
    println!("  internal payout address nonce: {}", live_nonce);
    println!(
        "  current gas price: {} gwei",
        live_gas_price as f64 / 1_000_000_000.0
    );
    assert!(live_gas_price > 0, "gas price should be positive");

    let initial_swap_status = fetch_swap_status(&ctx.db, &swap_id).await;
    println!("PLATFORM");
    println!(
        "  runs MonitorEngine.process_poll() for the live ETH swap while Trocador status is still active"
    );
    let initial_poll = PollingState {
        swap_id: swap_id.clone(),
        last_polled_at: None,
        next_poll_at: chrono::Utc::now(),
        poll_count: 0,
        last_status: initial_swap_status.clone(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    engine
        .process_poll(initial_poll)
        .await
        .expect("monitor engine should process the live ETH poll");

    let first_poll_state = fetch_polling_state(&ctx.db, &swap_id).await;
    let post_live_poll_status = fetch_swap_status(&ctx.db, &swap_id).await;

    println!("MONITOR");
    println!(
        "  poller recorded last_status={} after checking Trocador trade {}",
        first_poll_state.last_status, provider_swap_id
    );
    println!("  poll count is now {}", first_poll_state.poll_count);
    println!("  swap status after live poll is {}", post_live_poll_status);

    assert!(
        first_poll_state.last_polled_at.is_some(),
        "poller should record last_polled_at after the live ETH poll"
    );
    assert!(
        first_poll_state.poll_count >= 1,
        "poller should persist at least one poll attempt"
    );
    assert_eq!(first_poll_state.last_status, post_live_poll_status);

    println!("PLATFORM");
    println!(
        "  marks the swap as funds_received to prove the poller can hand off into local settlement"
    );
    sqlx::query("UPDATE swaps SET status = 'funds_received', updated_at = NOW() WHERE id = ?")
        .bind(&swap_id)
        .execute(&ctx.db)
        .await
        .expect("should mark the ETH swap as funds_received for monitor settlement trace");
    clear_monitor_lock(&ctx, &swap_id).await;

    let funds_received_poll = PollingState {
        swap_id: swap_id.clone(),
        last_polled_at: first_poll_state.last_polled_at,
        next_poll_at: chrono::Utc::now(),
        poll_count: first_poll_state.poll_count,
        last_status: first_poll_state.last_status.clone(),
        created_at: first_poll_state.created_at,
        updated_at: chrono::Utc::now(),
    };
    engine
        .process_poll(funds_received_poll)
        .await
        .expect("monitor engine should safely re-enter settlement for funds_received ETH swap");

    let second_poll_state = fetch_polling_state(&ctx.db, &swap_id).await;
    let final_swap_status = fetch_swap_status(&ctx.db, &swap_id).await;
    let final_info = wallet_crud
        .get_address_info(&swap_id)
        .await
        .expect("address info lookup should succeed")
        .expect("live ETH swap should keep direct-settlement address info");

    println!("MONITOR");
    println!(
        "  after settlement handoff, poller recorded last_status={}",
        second_poll_state.last_status
    );
    println!("  poll count advanced to {}", second_poll_state.poll_count);
    println!("  swap status remains {}", final_swap_status);
    println!("  payout address record status is {}", final_info.status);
    println!("  payout tx hash is {:?}", final_info.payout_tx_hash);

    assert!(
        second_poll_state.poll_count > first_poll_state.poll_count,
        "second poll should increment the stored poll_count"
    );
    assert_eq!(second_poll_state.last_status, "payout_failed");
    assert_eq!(final_swap_status, "funds_received");
    assert_eq!(final_info.status, "failed");
    assert!(final_info.payout_tx_hash.is_none());

    let settlement_provider = Arc::new(LiveSettlementEthereumProvider::new(
        live_http.clone(),
        SYNTHETIC_SETTLEMENT_BALANCE,
    ));
    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));
    println!("PLATFORM");
    println!(
        "  replays settlement on the same ETH swap with synthetic receipt {}, but real nonce/gas/send RPC calls",
        SYNTHETIC_SETTLEMENT_BALANCE
    );
    let replay_outcome = settlement_service
        .settle_swap(
            &swap_id,
            settlement_provider.clone(),
            Some(SYNTHETIC_SETTLEMENT_BALANCE),
        )
        .await
        .expect("settlement replay should return a retryable outcome");

    let replay_failure_reason = match replay_outcome {
        SettlementOutcome::PendingRetry { reason } => reason,
        SettlementOutcome::Completed(response) => {
            panic!(
                "unexpectedly completed a real Ethereum payout without funding during monitor replay: {:?}",
                response
            )
        }
        SettlementOutcome::AlreadyCompleted => {
            panic!("monitor replay should not already be completed for this test swap")
        }
        SettlementOutcome::AwaitingPayout => {
            panic!("wallet mnemonic is configured, monitor replay should not stay awaiting payout")
        }
        SettlementOutcome::PayoutInProgress => {
            panic!("monitor replay should not already be in progress for this test swap")
        }
    };

    println!("PLATFORM");
    println!(
        "  settlement replay stopped safely with retryable reason: {}",
        replay_failure_reason
    );
    assert_insufficient_funds_message(&replay_failure_reason);

    let replay_broadcasted = settlement_provider.broadcasted_txs.lock().unwrap().clone();
    assert!(
        !replay_broadcasted.is_empty(),
        "expected at least one live RPC broadcast attempt during monitor replay"
    );

    let replay_first_raw = replay_broadcasted
        .first()
        .expect("captured replay raw transaction should exist");
    let replay_raw_tx = hex::decode(replay_first_raw.trim_start_matches("0x")).unwrap();
    let replay_envelope = TxEnvelope::decode_2718(&mut replay_raw_tx.as_slice()).unwrap();
    let replay_signed = replay_envelope
        .as_legacy()
        .expect("expected legacy EVM transaction");

    let service_fee = wallet_crud
        .get_payout_fee_quote(&swap_id)
        .await
        .unwrap()
        .expect("payout fee quote should exist")
        .platform_fee;
    let replay_network_fee = replay_signed.tx().gas_limit as f64
        * replay_signed.tx().gas_price as f64
        / 1_000_000_000_000_000_000.0;
    let replay_expected_payout = SYNTHETIC_SETTLEMENT_BALANCE - service_fee - replay_network_fee;
    let replay_expected_value =
        U256::from((replay_expected_payout * 1_000_000_000_000_000_000.0f64).round() as u128);

    println!("SIGNING");
    println!("  signed tx gas limit: {}", replay_signed.tx().gas_limit);
    println!(
        "  signed tx gas price: {} gwei",
        replay_signed.tx().gas_price as f64 / 1_000_000_000.0
    );
    println!("  implied network fee: {}", replay_network_fee);
    println!("  service fee deducted: {}", service_fee);
    println!(
        "  final payout amount encoded in tx: {}",
        replay_expected_payout
    );

    assert!(
        matches!(
            replay_signed.tx().to,
            TxKind::Call(address) if address == ETHEREUM_RECIPIENT.parse::<AlloyAddress>().unwrap()
        ),
        "monitor replay payout must target the user's original address"
    );
    assert_eq!(replay_signed.tx().value, replay_expected_value);

    ctx.cleanup().await;
}
