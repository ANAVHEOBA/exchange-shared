// =============================================================================
// LIVE ETHEREUM RPC STRESS TEST
// Hammers the real Ethereum mainnet RPC stack without sending real funds.
//
// What this validates:
// 1. Endpoint-by-endpoint reads on every configured Ethereum RPC URL
// 2. Real tx signing from a derived wallet address
// 3. No-funds broadcast rejection on eth_sendRawTransaction
// 4. Wrong-chain-id rejection on eth_sendRawTransaction
// 5. RpcManager path under round-robin load across all real endpoints
//
// What this does NOT validate:
// - mempool acceptance of a funded transaction
// - final on-chain settlement
//
// Run with:
//   cargo test --test wallet_tests ethereum_live_ -- --ignored --nocapture
//
// Optional tuning:
//   ETHEREUM_LIVE_STRESS_ROUNDS=12
//   ETHEREUM_MANAGER_STRESS_ROUNDS=20
// =============================================================================

#[path = "../../common/mod.rs"]
mod common;

use exchange_shared::services::rpc::{
    build_default_rpc_configs, LoadBalancingStrategy, RpcConfig, RpcManager,
};
use exchange_shared::services::wallet::{
    derivation,
    rpc::{BlockchainProvider, HttpRpcClient, RpcError},
    signing::SigningService,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use serial_test::serial;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

const KNOWN_FROM_ADDRESS: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
const BURN_ADDRESS: &str = "0x000000000000000000000000000000000000dEaD";
const WETH_MAINNET: &str = "0xC02aaA39b223FE8D0A0E5C4F27eAD9083C756Cc2";
const HIGH_INDEX_START: u32 = 50_000;
const HIGH_INDEX_SEARCH: u32 = 128;
const DEFAULT_DIRECT_ROUNDS: usize = 6;
const DEFAULT_MANAGER_ROUNDS: usize = 10;
const DEFAULT_INTER_ROUND_DELAY_MS: u64 = 125;

#[derive(Debug)]
struct UnfundedEthereumAccount {
    index: u32,
    address: String,
    private_key_hex: String,
}

#[derive(Debug)]
struct EndpointSummary {
    label: String,
    url: String,
    chain_id: u64,
    first_block: u64,
    last_block: u64,
    min_gwei: f64,
    max_gwei: f64,
    estimate_gas: u64,
    no_funds_error: String,
    wrong_chain_mode: String,
    wrong_chain_error: String,
}

fn read_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn read_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn endpoint_label(url: &str) -> &'static str {
    if url.contains("alchemy.com") {
        "alchemy"
    } else if url.contains("infura.io") {
        "infura"
    } else if url.contains("ankr.com") {
        "ankr"
    } else {
        "public"
    }
}

fn parse_hex_quantity(hex: &str) -> Result<u64, String> {
    u64::from_str_radix(hex.trim_start_matches("0x"), 16)
        .map_err(|e| format!("Invalid hex quantity '{}': {}", hex, e))
}

fn sanitize_rpc_message(message: &str) -> String {
    message.replace('\n', " ").replace('\r', " ")
}

fn assert_expected_no_funds_error(error: &RpcError, label: &str) {
    let lower = error.to_string().to_ascii_lowercase();
    assert!(
        lower.contains("insufficient funds")
            || lower.contains("insufficient balance")
            || lower.contains("sender doesn't have enough funds")
            || lower.contains("funds for gas * price + value"),
        "Expected an insufficient-funds style rejection from {label}, got: {error}"
    );
}

fn classify_wrong_chain_rejection(error: &RpcError) -> Option<&'static str> {
    let lower = error.to_string().to_ascii_lowercase();
    if lower.contains("invalid sender") || lower.contains("chain id") {
        Some("chain-validated")
    } else if lower.contains("insufficient funds")
        || lower.contains("insufficient balance")
        || lower.contains("sender doesn't have enough funds")
        || lower.contains("funds for gas * price + value")
    {
        Some("funds-first")
    } else if lower.contains("rejected")
        || lower.contains("rlp")
        || lower.contains("transaction type not supported")
    {
        Some("other-safe-rejection")
    } else {
        None
    }
}

fn assert_wrong_chain_rejection(error: &RpcError, label: &str) -> &'static str {
    classify_wrong_chain_rejection(error)
        .unwrap_or_else(|| panic!("Expected a wrong-chain rejection from {label}, got: {error}"))
}

async fn rpc_call<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: Value,
) -> Result<T, RpcError> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    });

    let response = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| RpcError::Network(e.to_string()))?;
    let status = response.status();
    let body_text = response
        .text()
        .await
        .map_err(|e| RpcError::Parse(e.to_string()))?;
    let body: Value = serde_json::from_str(&body_text).map_err(|e| {
        RpcError::Parse(format!(
            "status {} body '{}': {}",
            status,
            body_text.chars().take(240).collect::<String>(),
            e
        ))
    })?;

    if let Some(message) = body
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
    {
        return Err(RpcError::Rpc(message.to_string()));
    }

    serde_json::from_value(body.get("result").cloned().unwrap_or(Value::Null))
        .map_err(|e| RpcError::Parse(e.to_string()))
}

async fn find_unfunded_ethereum_account(
    client: &HttpRpcClient,
    mnemonic: &str,
) -> Result<UnfundedEthereumAccount, String> {
    for index in HIGH_INDEX_START..(HIGH_INDEX_START + HIGH_INDEX_SEARCH) {
        let address = derivation::derive_evm_address(mnemonic, index).await?;
        let balance = client
            .get_balance(&address)
            .await
            .map_err(|e| format!("Failed to get balance for index {}: {}", index, e))?;
        let nonce = client
            .get_transaction_count(&address)
            .await
            .map_err(|e| format!("Failed to get nonce for index {}: {}", index, e))?;

        if balance == 0.0 && nonce == 0 {
            let private_key_hex = derivation::derive_evm_key(mnemonic, index).await?;
            return Ok(UnfundedEthereumAccount {
                index,
                address,
                private_key_hex,
            });
        }
    }

    Err(format!(
        "Failed to find an unfunded Ethereum address between indices {} and {}",
        HIGH_INDEX_START,
        HIGH_INDEX_START + HIGH_INDEX_SEARCH - 1
    ))
}

fn ethereum_endpoints() -> RpcConfig {
    dotenvy::dotenv().ok();

    let configs = build_default_rpc_configs();
    configs
        .get("ethereum")
        .cloned()
        .expect("ethereum must exist in RPC config")
}

fn unique_endpoint_urls(config: &RpcConfig) -> Vec<String> {
    let mut seen = BTreeSet::new();
    config
        .endpoints
        .iter()
        .filter_map(|endpoint| {
            if seen.insert(endpoint.url.clone()) {
                Some(endpoint.url.clone())
            } else {
                None
            }
        })
        .collect()
}

async fn probe_ethereum_endpoint(
    reqwest_client: &reqwest::Client,
    url: &str,
    rounds: usize,
    delay_ms: u64,
    unfunded: &UnfundedEthereumAccount,
    balance_of_call: &str,
) -> Result<EndpointSummary, String> {
    let label = endpoint_label(url).to_string();
    let client = HttpRpcClient::new(url.to_string());

    let chain_id_hex: String = rpc_call(reqwest_client, url, "eth_chainId", json!([]))
        .await
        .map_err(|e| format!("chain id failed: {}", e))?;
    let chain_id = parse_hex_quantity(&chain_id_hex)?;
    if chain_id != 1 {
        return Err(format!("expected Ethereum chain id 1, got {}", chain_id));
    }

    let mut block_numbers = Vec::with_capacity(rounds);
    let mut gas_prices = Vec::with_capacity(rounds);
    let mut latest_estimate_gas = 0u64;

    for round in 0..rounds {
        let block_hex: String = rpc_call(reqwest_client, url, "eth_blockNumber", json!([]))
            .await
            .map_err(|e| format!("round {} block number failed: {}", round + 1, e))?;
        let block_number = parse_hex_quantity(&block_hex)?;
        if block_number == 0 {
            return Err(format!("round {} returned block height 0", round + 1));
        }
        block_numbers.push(block_number);

        let gas_price = client
            .get_gas_price()
            .await
            .map_err(|e| format!("round {} gas price failed: {}", round + 1, e))?;
        if gas_price == 0 {
            return Err(format!("round {} returned gas price 0", round + 1));
        }
        gas_prices.push(gas_price);

        let balance = client
            .get_balance(&unfunded.address)
            .await
            .map_err(|e| format!("round {} balance lookup failed: {}", round + 1, e))?;
        if balance.abs() >= f64::EPSILON {
            return Err(format!(
                "round {} expected zero ETH on unfunded sender, got {}",
                round + 1,
                balance
            ));
        }

        let nonce = client
            .get_transaction_count(&unfunded.address)
            .await
            .map_err(|e| format!("round {} nonce lookup failed: {}", round + 1, e))?;
        if nonce != 0 {
            return Err(format!(
                "round {} expected nonce 0 on unfunded sender, got {}",
                round + 1,
                nonce
            ));
        }

        let token_balance = client
            .evm_call(WETH_MAINNET, balance_of_call)
            .await
            .map_err(|e| format!("round {} WETH balanceOf failed: {}", round + 1, e))?;
        if !token_balance.starts_with("0x") {
            return Err(format!(
                "round {} eth_call returned non-hex payload: {}",
                round + 1,
                token_balance
            ));
        }

        let estimate_hex: String = rpc_call(
            reqwest_client,
            url,
            "eth_estimateGas",
            json!([{
                "from": KNOWN_FROM_ADDRESS,
                "to": BURN_ADDRESS,
                "value": "0x1"
            }]),
        )
        .await
        .map_err(|e| format!("round {} eth_estimateGas failed: {}", round + 1, e))?;
        let estimate_gas = parse_hex_quantity(&estimate_hex)?;
        if !(21_000..=50_000).contains(&estimate_gas) {
            return Err(format!(
                "round {} returned suspicious gas estimate {}",
                round + 1,
                estimate_gas
            ));
        }
        latest_estimate_gas = estimate_gas;

        sleep(Duration::from_millis(delay_ms)).await;
    }

    let latest_gas_price = *gas_prices.last().expect("at least one gas price");
    let unsigned_raw = SigningService::sign_evm_raw_transaction(
        &unfunded.private_key_hex,
        1,
        0,
        latest_gas_price,
        21_000,
        BURN_ADDRESS,
        alloy::primitives::U256::from(1u64),
        &[],
    )
    .expect("live raw tx signing should succeed");
    let no_funds_error = client
        .send_raw_transaction(&unsigned_raw)
        .await
        .expect_err("unfunded broadcast must be rejected");
    assert_expected_no_funds_error(&no_funds_error, &label);

    let wrong_chain_raw = SigningService::sign_evm_raw_transaction(
        &unfunded.private_key_hex,
        137,
        0,
        latest_gas_price,
        21_000,
        BURN_ADDRESS,
        alloy::primitives::U256::from(1u64),
        &[],
    )
    .expect("wrong-chain raw tx signing should still produce bytes");
    let wrong_chain_error = client
        .send_raw_transaction(&wrong_chain_raw)
        .await
        .expect_err("wrong-chain broadcast must be rejected");
    let wrong_chain_mode = assert_wrong_chain_rejection(&wrong_chain_error, &label);

    let min_gwei = gas_prices.iter().copied().fold(u64::MAX, u64::min) as f64 / 1_000_000_000.0;
    let max_gwei = gas_prices.iter().copied().fold(0, u64::max) as f64 / 1_000_000_000.0;
    let first_block = *block_numbers.first().expect("at least one block");
    let last_block = *block_numbers.last().expect("at least one block");

    Ok(EndpointSummary {
        label,
        url: url.to_string(),
        chain_id,
        first_block,
        last_block,
        min_gwei,
        max_gwei,
        estimate_gas: latest_estimate_gas,
        no_funds_error: sanitize_rpc_message(&no_funds_error.to_string()),
        wrong_chain_mode: wrong_chain_mode.to_string(),
        wrong_chain_error: sanitize_rpc_message(&wrong_chain_error.to_string()),
    })
}

#[serial]
#[tokio::test]
#[ignore = "Requires network access and WALLET_MNEMONIC; hammers all configured Ethereum RPC endpoints without broadcasting funded transactions"]
async fn ethereum_live_endpoint_matrix_stress() {
    dotenvy::dotenv().ok();

    let config = ethereum_endpoints();
    let endpoint_urls = unique_endpoint_urls(&config);
    assert!(
        !endpoint_urls.is_empty(),
        "ethereum should have at least one configured endpoint"
    );

    let rounds = read_usize_env("ETHEREUM_LIVE_STRESS_ROUNDS", DEFAULT_DIRECT_ROUNDS);
    let delay_ms = read_u64_env(
        "ETHEREUM_LIVE_INTER_ROUND_DELAY_MS",
        DEFAULT_INTER_ROUND_DELAY_MS,
    );

    let mnemonic = common::test_wallet_mnemonic();
    let anchor_client = HttpRpcClient::new(endpoint_urls[0].clone());
    let unfunded = find_unfunded_ethereum_account(&anchor_client, &mnemonic)
        .await
        .expect("failed to derive a safe unfunded Ethereum sender");

    println!(
        "Using unfunded Ethereum sender index {} at {}",
        unfunded.index, unfunded.address
    );

    let balance_of_call = SigningService::encode_erc20_balance_of_call(&unfunded.address)
        .expect("balanceOf calldata must encode");
    let reqwest_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap_or_default();

    let mut summaries = Vec::new();
    let mut failures = Vec::new();

    for url in endpoint_urls {
        match probe_ethereum_endpoint(
            &reqwest_client,
            &url,
            rounds,
            delay_ms,
            &unfunded,
            &balance_of_call,
        )
        .await
        {
            Ok(summary) => summaries.push(summary),
            Err(error) => failures.push(format!(
                "[{}] {} => {}",
                endpoint_label(&url),
                url,
                sanitize_rpc_message(&error)
            )),
        }
    }

    println!("\n=== ETHEREUM LIVE ENDPOINT MATRIX ===");
    for summary in &summaries {
        println!(
            "[{}] chain_id={} blocks={}..{} gas={:.3}..{:.3} gwei estimate={} url={}",
            summary.label,
            summary.chain_id,
            summary.first_block,
            summary.last_block,
            summary.min_gwei,
            summary.max_gwei,
            summary.estimate_gas,
            summary.url
        );
        println!("  no-funds rejection: {}", summary.no_funds_error);
        println!("  wrong-chain mode: {}", summary.wrong_chain_mode);
        println!("  wrong-chain rejection: {}", summary.wrong_chain_error);
    }

    if !failures.is_empty() {
        println!("\n=== ETHEREUM LIVE ENDPOINT FAILURES ===");
        for failure in &failures {
            println!("{}", failure);
        }
    }

    assert!(
        failures.is_empty(),
        "Ethereum live endpoint stress failures:\n{}",
        failures.join("\n")
    );
}

#[serial]
#[tokio::test]
#[ignore = "Requires network access and WALLET_MNEMONIC; stresses RpcManager over all real Ethereum endpoints"]
async fn ethereum_live_rpc_manager_round_robin_stress() {
    dotenvy::dotenv().ok();

    let mut config = ethereum_endpoints();
    let endpoint_count = config.endpoints.len();
    assert!(
        endpoint_count >= 1,
        "ethereum should have configured endpoints"
    );

    for endpoint in &mut config.endpoints {
        endpoint.priority = 1;
    }
    config.strategy = LoadBalancingStrategy::RoundRobin;

    let rounds = read_usize_env("ETHEREUM_MANAGER_STRESS_ROUNDS", DEFAULT_MANAGER_ROUNDS);
    let delay_ms = read_u64_env(
        "ETHEREUM_LIVE_INTER_ROUND_DELAY_MS",
        DEFAULT_INTER_ROUND_DELAY_MS,
    );

    let mnemonic = common::test_wallet_mnemonic();
    let first_url = config.endpoints[0].url.clone();
    let anchor_client = HttpRpcClient::new(first_url);
    let unfunded = find_unfunded_ethereum_account(&anchor_client, &mnemonic)
        .await
        .expect("failed to derive a safe unfunded Ethereum sender");

    let mut configs = HashMap::new();
    configs.insert("ethereum".to_string(), config);
    let manager = Arc::new(RpcManager::new(configs));
    let adapter = exchange_shared::services::rpc::RpcManagerAdapter::new(
        manager.clone(),
        "ethereum".to_string(),
    );

    let total_calls = rounds * endpoint_count;
    for round in 0..total_calls {
        let chain_id_hex: String = manager
            .call("ethereum", "eth_chainId", json!([]))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "RpcManager eth_chainId failed on round {}: {}",
                    round + 1,
                    e
                )
            });
        let chain_id = parse_hex_quantity(&chain_id_hex).expect("valid chain id");
        assert_eq!(chain_id, 1, "RpcManager must stay on Ethereum mainnet");

        let block_hex: String = manager
            .call("ethereum", "eth_blockNumber", json!([]))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "RpcManager eth_blockNumber failed on round {}: {}",
                    round + 1,
                    e
                )
            });
        let block_number = parse_hex_quantity(&block_hex).expect("valid block number");
        assert!(block_number > 0, "RpcManager block number must be positive");

        let gas_price = adapter.get_gas_price().await.unwrap_or_else(|e| {
            panic!(
                "RpcManagerAdapter gas price failed on round {}: {}",
                round + 1,
                e
            )
        });
        assert!(
            gas_price > 0,
            "RpcManagerAdapter gas price must be positive"
        );

        let nonce = adapter
            .get_transaction_count(&unfunded.address)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "RpcManagerAdapter nonce failed on round {}: {}",
                    round + 1,
                    e
                )
            });
        assert_eq!(nonce, 0, "unfunded sender nonce must stay at zero");

        let balance = adapter
            .get_balance(&unfunded.address)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "RpcManagerAdapter balance failed on round {}: {}",
                    round + 1,
                    e
                )
            });
        assert!(
            balance.abs() < f64::EPSILON,
            "unfunded sender balance must stay zero, got {}",
            balance
        );

        sleep(Duration::from_millis(delay_ms)).await;
    }

    let health = manager.get_health_status("ethereum").await;
    let sampled: Vec<_> = health
        .into_iter()
        .filter(|status| status.total_requests > 0)
        .collect();
    let overview = manager.health_overview().await;

    assert_eq!(
        sampled.len(),
        endpoint_count,
        "round-robin stress should touch every configured Ethereum endpoint"
    );
    assert!(
        sampled.iter().all(|status| status.is_healthy),
        "all sampled Ethereum endpoints should remain healthy after live stress: {:?}",
        sampled
    );
    assert!(
        sampled
            .iter()
            .all(|status| status.success_rate >= 0.999 && status.total_requests > 0),
        "all sampled Ethereum endpoints should have perfect success in this stress run: {:?}",
        sampled
    );
    assert!(
        overview.sampled_endpoints >= endpoint_count,
        "health overview should report sampled Ethereum endpoints"
    );

    println!("\n=== ETHEREUM RPC MANAGER ROUND-ROBIN STRESS ===");
    println!(
        "sampled_endpoints={} healthy_endpoints={} total_endpoints={}",
        overview.sampled_endpoints, overview.healthy_endpoints, overview.total_endpoints
    );
    for status in sampled {
        println!(
            "url={} total_requests={} success_rate={:.3} avg_latency_ms={:.1} p95_latency_ms={:?}",
            status.url,
            status.total_requests,
            status.success_rate,
            status.average_latency_ms,
            status.p95_latency_ms
        );
    }
}
