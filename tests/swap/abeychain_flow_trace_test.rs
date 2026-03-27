#[path = "../common/mod.rs"]
mod common;

use async_trait::async_trait;
use common::{test_wallet_mnemonic, timed_get, timed_post, TestContext};
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::GenerateAddressRequest;
use exchange_shared::services::rpc::{
    build_default_rpc_configs, build_provider_for_asset, RpcManager,
};
use exchange_shared::services::settlement::{SettlementOutcome, SettlementService};
use exchange_shared::services::wallet::manager::WalletManager;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, HttpRpcClient, RpcError};
use serde_json::{json, Value};
use serial_test::serial;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

const ABEYCHAIN_RECIPIENT: &str = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e";
const BTC_REFUND_ADDRESS: &str = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";
const SYNTHETIC_PROVIDER_ID: &str = "changenow";
const SYNTHETIC_SETTLEMENT_BALANCE: f64 = 1.0;

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
struct LiveSettlementAbeyChainProvider {
    inner: Arc<HttpRpcClient>,
    forced_balance: f64,
    broadcasted_txs: Arc<Mutex<Vec<String>>>,
}

impl LiveSettlementAbeyChainProvider {
    fn new(inner: Arc<HttpRpcClient>, forced_balance: f64) -> Self {
        Self {
            inner,
            forced_balance,
            broadcasted_txs: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl BlockchainProvider for LiveSettlementAbeyChainProvider {
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

async fn execute_live_abeychain_create_attempt(
    ctx: &TestContext,
    wallet_crud: &WalletCrud,
) -> CreateAttemptTrace {
    println!();
    println!("================ ABEYCHAIN FULL FLOW TRACE ================");
    println!("USER");
    println!("  wants to swap 0.005 btc on Mainnet into abey on MAINNET");
    println!("  enters recipient address {}", ABEYCHAIN_RECIPIENT);

    let rates_path =
        "/swap/rates?from=btc&to=abey&amount=0.005&network_from=Mainnet&network_to=MAINNET";

    println!("PLATFORM");
    println!("  asks Trocador for a live quote via {}", rates_path);
    let rate_response = timed_get(&ctx.server, rates_path).await;
    rate_response.assert_status_ok();

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"].as_str().unwrap().to_string();
    let rate_provider = rate_json["rates"][0]["provider"]
        .as_str()
        .unwrap()
        .to_string();
    let quoted_receive = rate_json["rates"][0]["estimated_amount"].as_f64().unwrap();

    println!("TROCADOR");
    println!("  returned trade_id {}", trade_id);
    println!("  best provider for this quote is {}", rate_provider);
    println!("  estimated payout is {}", quoted_receive);

    let create_payload = json!({
        "trade_id": trade_id,
        "from": "btc",
        "network_from": "Mainnet",
        "to": "abey",
        "network_to": "MAINNET",
        "amount": 0.005,
        "provider": rate_provider,
        "recipient_address": ABEYCHAIN_RECIPIENT,
        "refund_address": BTC_REFUND_ADDRESS,
        "rate_type": "floating"
    });

    println!("USER");
    println!("  confirms the swap and submits /swap/create");
    let create_response = timed_post(&ctx.server, "/swap/create", &create_payload).await;
    let status = create_response.status_code().as_u16();
    let create_json: Value = create_response.json();

    if status != 201 {
        let error_message = create_json["error"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        println!("PLATFORM");
        println!("  rejected AbeyChain create: {}", error_message);
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

    let swap_id = create_json["swap_id"].as_str().unwrap().to_string();
    let deposit_address = create_json["deposit_address"].as_str().unwrap().to_string();
    let address_info = wallet_crud
        .get_address_info(&swap_id)
        .await
        .unwrap()
        .expect("direct settlement should create address info");

    println!("PLATFORM");
    println!("  created swap {}", swap_id);
    println!("  returned deposit address {}", deposit_address);
    println!(
        "  stored internal AbeyChain payout address {}",
        address_info.our_address
    );

    CreateAttemptTrace {
        trade_id: trade_id.clone(),
        rate_provider,
        quoted_receive,
        create_status: status,
        error_message: None,
        swap_id: Some(swap_id),
        provider_swap_id: Some(trade_id),
        deposit_address: Some(deposit_address),
        internal_payout_address: Some(address_info.our_address),
    }
}

async fn create_seeded_abeychain_settlement_swap(
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
        VALUES (?, ?, 'abey-flow-trace-trade', 'BTC', 'Mainnet',
                'abey', 'MAINNET', 0.005, 0.98958, 0.01, 0.00042, 0.01042, 15.0,
                'trocador_deposit_addr', ?, 'sending')
        "#,
    )
    .bind(swap_id)
    .bind(SYNTHETIC_PROVIDER_ID)
    .bind(recipient)
    .execute(db)
    .await
    .expect("failed to seed abeychain swap");
}

#[serial]
#[tokio::test]
#[ignore = "Requires live AbeyChain RPC access; traces the full ABEY flow"]
async fn abeychain_full_flow_trace_live_until_money_boundary() {
    dotenvy::dotenv().ok();
    let ctx = TestContext::new().await;
    let wallet_crud = WalletCrud::new(ctx.db.clone());
    let mnemonic = test_wallet_mnemonic();
    let rpc_manager = Arc::new(RpcManager::new(build_default_rpc_configs()));

    // AbeyChain uses "abeychain" as canonical key in RpcManager
    let live_provider = build_provider_for_asset(rpc_manager.clone(), "abey", "MAINNET")
        .await
        .expect("AbeyChain RPC provider should be configured");
    let live_endpoint = rpc_manager
        .select_endpoint("abeychain")
        .await
        .expect("AbeyChain endpoint selection should succeed");
    let live_http = Arc::new(HttpRpcClient::new(live_endpoint.clone()));

    let create_trace = execute_live_abeychain_create_attempt(&ctx, &wallet_crud).await;
    let payout_swap_id = if let Some(swap_id) = create_trace.swap_id.clone() {
        swap_id
    } else {
        let synthetic_swap_id = Uuid::new_v4().to_string();
        create_seeded_abeychain_settlement_swap(&ctx.db, &synthetic_swap_id, ABEYCHAIN_RECIPIENT)
            .await;
        synthetic_swap_id
    };

    let wallet_manager =
        WalletManager::new(wallet_crud.clone(), mnemonic.clone(), live_provider.clone());
    let generated = wallet_manager
        .get_or_generate_address(GenerateAddressRequest {
            swap_id: payout_swap_id.clone(),
            ticker: "abey".to_string(),
            network: "MAINNET".to_string(),
            user_recipient_address: ABEYCHAIN_RECIPIENT.to_string(),
            user_recipient_extra_id: None,
        })
        .await
        .expect("AbeyChain internal address generation should succeed");

    println!("PLATFORM");
    println!("  internal settlement address is {}", generated.address);
    println!(
        "  connects to live AbeyChain RPC {} and reads state",
        live_endpoint
    );

    let live_gas_price = live_provider
        .get_gas_price()
        .await
        .expect("live abeychain gas lookup should succeed");
    println!("RPC");
    println!(
        "  current gas price: {} gwei",
        live_gas_price as f64 / 1_000_000_000.0
    );

    let settlement_provider = Arc::new(LiveSettlementAbeyChainProvider::new(
        live_http.clone(),
        SYNTHETIC_SETTLEMENT_BALANCE,
    ));
    let settlement_service = SettlementService::new(ctx.db.clone(), Some(mnemonic));

    let outcome = settlement_service
        .settle_swap(
            &payout_swap_id,
            settlement_provider.clone(),
            Some(SYNTHETIC_SETTLEMENT_BALANCE),
        )
        .await
        .expect("settlement service should return a retryable outcome");

    if let SettlementOutcome::PendingRetry { reason } = outcome {
        println!("PLATFORM");
        println!("  settlement stopped safely: {}", reason);
    }

    ctx.cleanup().await;
}
