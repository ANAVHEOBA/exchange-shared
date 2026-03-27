#[path = "../common/mod.rs"]
mod common;

use common::test_wallet_mnemonic;
use exchange_shared::services::payout_policy::PayoutPolicyConfig;
use exchange_shared::services::rpc::{
    build_default_rpc_configs, build_provider_for_asset, resolve_configured_send_chain_key,
    supports_direct_provider_chain, RpcManager,
};
use exchange_shared::services::trocador::{TrocadorClient, TrocadorError};
use exchange_shared::services::wallet::{
    derivation,
    validation::{self, AddressValidation},
};
use serde::Deserialize;
use serial_test::serial;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

const DEFAULT_DELAY_MS: u64 = 400;
const DEFAULT_MAX_LIVE_PAIRS: usize = 50;
const DEFAULT_MAX_SNAPSHOT_PAIRS: usize = 2_466;

const LOCAL_CHECK_BUDGET_MS: u128 = 50;
const ROUTE_LOOKUP_BUDGET_MS: u128 = 20;
const TROCADOR_VALIDATE_BUDGET_MS: u128 = 2_000;
const RPC_READ_BUDGET_MS: u128 = 800;
const EVM_GAS_BUDGET_MS: u128 = 1_000;
const TOTAL_PREFLIGHT_BUDGET_MS: u128 = 3_000;

const RATE_LIMIT_RETRY_ATTEMPTS: usize = 2;

#[derive(Debug, Clone, Deserialize)]
struct SnapshotCurrency {
    ticker: String,
    network: String,
}

#[derive(Default)]
struct LatencySummary {
    live_pairs_fetched: usize,
    checked_pairs: usize,
    derivation_errors: Vec<String>,
    local_invalid_pairs: Vec<String>,
    local_unsupported_pairs: Vec<String>,
    route_lookup_failures: Vec<String>,
    trocador_errors: Vec<String>,
    trocador_false_results: Vec<String>,
    rpc_provider_failures: Vec<String>,
    rpc_read_failures: Vec<String>,
    local_over_budget: Vec<String>,
    route_over_budget: Vec<String>,
    trocador_over_budget: Vec<String>,
    rpc_over_budget: Vec<String>,
    gas_over_budget: Vec<String>,
    total_over_budget: Vec<String>,
    local_certified_pairs: usize,
    trocador_only_pairs: usize,
}

impl LatencySummary {
    fn render(&self) -> String {
        let mut lines = vec![
            format!("live_pairs_fetched: {}", self.live_pairs_fetched),
            format!("checked_pairs: {}", self.checked_pairs),
            format!("local_certified_pairs: {}", self.local_certified_pairs),
            format!("trocador_only_pairs: {}", self.trocador_only_pairs),
            format!("derivation_errors: {}", self.derivation_errors.len()),
            format!("local_invalid_pairs: {}", self.local_invalid_pairs.len()),
            format!(
                "local_unsupported_pairs: {}",
                self.local_unsupported_pairs.len()
            ),
            format!(
                "route_lookup_failures: {}",
                self.route_lookup_failures.len()
            ),
            format!("trocador_errors: {}", self.trocador_errors.len()),
            format!(
                "trocador_false_results: {}",
                self.trocador_false_results.len()
            ),
            format!(
                "rpc_provider_failures: {}",
                self.rpc_provider_failures.len()
            ),
            format!("rpc_read_failures: {}", self.rpc_read_failures.len()),
            format!("local_over_budget: {}", self.local_over_budget.len()),
            format!("route_over_budget: {}", self.route_over_budget.len()),
            format!("trocador_over_budget: {}", self.trocador_over_budget.len()),
            format!("rpc_over_budget: {}", self.rpc_over_budget.len()),
            format!("gas_over_budget: {}", self.gas_over_budget.len()),
            format!("total_over_budget: {}", self.total_over_budget.len()),
        ];

        append_sample(
            &mut lines,
            "derivation_error_samples",
            &self.derivation_errors,
        );
        append_sample(
            &mut lines,
            "local_invalid_samples",
            &self.local_invalid_pairs,
        );
        append_sample(
            &mut lines,
            "local_unsupported_samples",
            &self.local_unsupported_pairs,
        );
        append_sample(
            &mut lines,
            "route_lookup_failure_samples",
            &self.route_lookup_failures,
        );
        append_sample(&mut lines, "trocador_error_samples", &self.trocador_errors);
        append_sample(
            &mut lines,
            "trocador_false_samples",
            &self.trocador_false_results,
        );
        append_sample(
            &mut lines,
            "rpc_provider_failure_samples",
            &self.rpc_provider_failures,
        );
        append_sample(
            &mut lines,
            "rpc_read_failure_samples",
            &self.rpc_read_failures,
        );
        append_sample(
            &mut lines,
            "local_over_budget_samples",
            &self.local_over_budget,
        );
        append_sample(
            &mut lines,
            "route_over_budget_samples",
            &self.route_over_budget,
        );
        append_sample(
            &mut lines,
            "trocador_over_budget_samples",
            &self.trocador_over_budget,
        );
        append_sample(&mut lines, "rpc_over_budget_samples", &self.rpc_over_budget);
        append_sample(&mut lines, "gas_over_budget_samples", &self.gas_over_budget);
        append_sample(
            &mut lines,
            "total_over_budget_samples",
            &self.total_over_budget,
        );

        lines.join("\n")
    }
}

fn append_sample(lines: &mut Vec<String>, label: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }

    lines.push(format!("{label}:"));
    for sample in items.iter().take(5) {
        lines.push(format!("  - {}", sample));
    }
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY and network access; measures preflight latency across the live Trocador /coins list"]
async fn test_live_preflight_latency_matrix_for_current_live_coin_list() {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY is required");
    let delay_ms = read_u64_env("TROCADOR_VALIDATE_DELAY_MS").unwrap_or(DEFAULT_DELAY_MS);
    let max_pairs = read_usize_env("TROCADOR_VALIDATE_MAX_PAIRS").unwrap_or(DEFAULT_MAX_LIVE_PAIRS);

    let client = TrocadorClient::new(api_key);
    let live_currencies = client
        .get_currencies()
        .await
        .expect("Failed to fetch live Trocador /coins list");
    let live_pairs_fetched = live_currencies.len();

    let seed = test_wallet_mnemonic();
    let rpc_manager = Arc::new(RpcManager::new(build_default_rpc_configs()));
    let payout_policy = PayoutPolicyConfig::from_env();
    let mut seen = BTreeSet::new();
    let deduped_pairs = live_currencies
        .into_iter()
        .filter(|currency| {
            seen.insert((
                currency.ticker.to_ascii_lowercase(),
                currency.network.to_ascii_lowercase(),
            ))
        })
        .collect::<Vec<_>>();

    let target_pairs = deduped_pairs.len().min(max_pairs);
    let pairs = deduped_pairs
        .into_iter()
        .take(target_pairs)
        .map(|currency| (currency.ticker, currency.network))
        .collect::<Vec<_>>();

    let summary = run_latency_matrix_for_pairs(
        &client,
        rpc_manager,
        &payout_policy,
        &seed,
        pairs,
        live_pairs_fetched,
        delay_ms,
    )
    .await;

    eprintln!("{}", summary.render());
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY and network access; measures preflight latency across all 2466 bundled snapshot pairs"]
async fn test_bundled_snapshot_preflight_latency_matrix_for_all_pairs() {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY is required");
    let delay_ms = read_u64_env("TROCADOR_VALIDATE_DELAY_MS").unwrap_or(DEFAULT_DELAY_MS);
    let max_pairs =
        read_usize_env("TROCADOR_SNAPSHOT_MAX_PAIRS").unwrap_or(DEFAULT_MAX_SNAPSHOT_PAIRS);

    let snapshot: Vec<SnapshotCurrency> =
        serde_json::from_str(include_str!("../../trocador_currencies_full.json"))
            .expect("Failed to parse bundled Trocador snapshot");

    let client = TrocadorClient::new(api_key);
    let seed = test_wallet_mnemonic();
    let rpc_manager = Arc::new(RpcManager::new(build_default_rpc_configs()));
    let payout_policy = PayoutPolicyConfig::from_env();
    let pairs = snapshot
        .into_iter()
        .take(max_pairs)
        .map(|currency| (currency.ticker, currency.network))
        .collect::<Vec<_>>();

    let summary = run_latency_matrix_for_pairs(
        &client,
        rpc_manager,
        &payout_policy,
        &seed,
        pairs,
        max_pairs,
        delay_ms,
    )
    .await;

    eprintln!("{}", summary.render());
}

async fn run_latency_matrix_for_pairs(
    client: &TrocadorClient,
    rpc_manager: Arc<RpcManager>,
    payout_policy: &PayoutPolicyConfig,
    seed: &str,
    pairs: Vec<(String, String)>,
    total_fetched: usize,
    delay_ms: u64,
) -> LatencySummary {
    let mut summary = LatencySummary {
        live_pairs_fetched: total_fetched,
        ..LatencySummary::default()
    };

    for (ticker, network) in pairs {
        summary.checked_pairs += 1;

        let pair_label = format!("{}/{}", ticker, network);

        let derive_started = Instant::now();
        let address = match derivation::derive_address(seed, &ticker, &network, 0).await {
            Ok(address) => address,
            Err(err) => {
                summary
                    .derivation_errors
                    .push(format!("{pair_label}: {err}"));
                continue;
            }
        };
        let derivation_ms = derive_started.elapsed().as_millis();

        let local_started = Instant::now();
        let local_validation =
            validation::validate_address_by_network_family(&ticker, &network, &address);
        let local_ms = local_started.elapsed().as_millis();

        if local_ms > LOCAL_CHECK_BUDGET_MS {
            summary.local_over_budget.push(format!(
                "{pair_label}: {local_ms}ms > {LOCAL_CHECK_BUDGET_MS}ms"
            ));
        }

        match local_validation {
            AddressValidation::Valid { .. } => {}
            AddressValidation::Invalid { family, reason } => {
                summary
                    .local_invalid_pairs
                    .push(format!("{pair_label} [{family}]: {reason}"));
                continue;
            }
            AddressValidation::Unsupported { family, reason } => {
                summary
                    .local_unsupported_pairs
                    .push(format!("{pair_label} [{family}]: {reason}"));
                continue;
            }
        }

        let route_started = Instant::now();
        let route_lookup =
            resolve_configured_send_chain_key(rpc_manager.as_ref(), &ticker, &network);
        let route_ms = route_started.elapsed().as_millis();

        if route_ms > ROUTE_LOOKUP_BUDGET_MS {
            summary.route_over_budget.push(format!(
                "{pair_label}: {route_ms}ms > {ROUTE_LOOKUP_BUDGET_MS}ms"
            ));
        }

        let (chain_key, family, direct_provider_supported) = match route_lookup {
            Ok(chain_key) => {
                let family = rpc_manager
                    .chain_family(&chain_key)
                    .unwrap_or("unknown")
                    .to_string();
                let direct = supports_direct_provider_chain(&chain_key, &family);
                (Some(chain_key), Some(family), direct)
            }
            Err(err) => {
                summary
                    .route_lookup_failures
                    .push(format!("{pair_label}: {err}"));
                (None, None, false)
            }
        };

        let trocador_started = Instant::now();
        match validate_with_rate_limit_retry(client, &ticker, &network, &address, delay_ms).await {
            Ok(true) => {}
            Ok(false) => {
                summary
                    .trocador_false_results
                    .push(format!("{pair_label}: Trocador returned false"));
                continue;
            }
            Err(err) => {
                summary.trocador_errors.push(format!("{pair_label}: {err}"));
                continue;
            }
        }
        let trocador_ms = trocador_started.elapsed().as_millis();

        if trocador_ms > TROCADOR_VALIDATE_BUDGET_MS {
            summary.trocador_over_budget.push(format!(
                "{pair_label}: {trocador_ms}ms > {TROCADOR_VALIDATE_BUDGET_MS}ms"
            ));
        }

        let mut rpc_read_ms = None;
        let mut gas_read_ms = None;
        let mut classification = "trocador_only";

        if let Some(chain_key) = chain_key.as_deref() {
            if payout_policy.is_local_certified(chain_key) && direct_provider_supported {
                match build_provider_for_asset(rpc_manager.clone(), &ticker, &network).await {
                    Ok(provider) => {
                        let rpc_started = Instant::now();
                        match provider.get_balance(&address).await {
                            Ok(_) => {
                                let elapsed = rpc_started.elapsed().as_millis();
                                rpc_read_ms = Some(elapsed);
                                if elapsed > RPC_READ_BUDGET_MS {
                                    summary.rpc_over_budget.push(format!(
                                        "{pair_label}: {elapsed}ms > {RPC_READ_BUDGET_MS}ms"
                                    ));
                                }
                                classification = "local_certified";
                                summary.local_certified_pairs += 1;
                            }
                            Err(err) => {
                                summary
                                    .rpc_read_failures
                                    .push(format!("{pair_label}: balance read failed: {err}"));
                            }
                        }

                        if family.as_deref() == Some("evm") {
                            let gas_started = Instant::now();
                            match provider.get_gas_price().await {
                                Ok(_) => {
                                    let elapsed = gas_started.elapsed().as_millis();
                                    gas_read_ms = Some(elapsed);
                                    if elapsed > EVM_GAS_BUDGET_MS {
                                        summary.gas_over_budget.push(format!(
                                            "{pair_label}: {elapsed}ms > {EVM_GAS_BUDGET_MS}ms"
                                        ));
                                    }
                                }
                                Err(err) => {
                                    summary
                                        .rpc_read_failures
                                        .push(format!("{pair_label}: gas read failed: {err}"));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        summary
                            .rpc_provider_failures
                            .push(format!("{pair_label}: {err}"));
                    }
                }
            }
        }
        if classification == "trocador_only" {
            summary.trocador_only_pairs += 1;
        }

        let total_ms = derivation_ms
            + local_ms
            + route_ms
            + trocador_ms
            + rpc_read_ms.unwrap_or(0)
            + gas_read_ms.unwrap_or(0);

        if total_ms > TOTAL_PREFLIGHT_BUDGET_MS {
            summary.total_over_budget.push(format!(
                "{pair_label}: {total_ms}ms > {TOTAL_PREFLIGHT_BUDGET_MS}ms"
            ));
        }

        eprintln!(
            "{} | class={} | chain_key={} | family={} | derive={}ms | local={}ms | route={}ms | trocador={}ms | rpc={} | gas={} | total={}ms",
            pair_label,
            classification,
            chain_key.as_deref().unwrap_or("n/a"),
            family.as_deref().unwrap_or("n/a"),
            derivation_ms,
            local_ms,
            route_ms,
            trocador_ms,
            rpc_read_ms
                .map(|value| format!("{value}ms"))
                .unwrap_or_else(|| "n/a".to_string()),
            gas_read_ms
                .map(|value| format!("{value}ms"))
                .unwrap_or_else(|| "n/a".to_string()),
            total_ms
        );

        sleep(Duration::from_millis(delay_ms)).await;
    }

    summary
}

fn read_u64_env(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn read_usize_env(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
}

async fn validate_with_rate_limit_retry(
    client: &TrocadorClient,
    ticker: &str,
    network: &str,
    address: &str,
    base_delay_ms: u64,
) -> Result<bool, TrocadorError> {
    let mut retry_delay_ms = base_delay_ms.saturating_mul(4).max(2_000);

    for attempt in 0..=RATE_LIMIT_RETRY_ATTEMPTS {
        match client.validate_address(ticker, network, address).await {
            Ok(valid) => return Ok(valid),
            Err(TrocadorError::ApiError(message))
                if message.to_ascii_lowercase().contains("rate limit exceeded")
                    && attempt < RATE_LIMIT_RETRY_ATTEMPTS =>
            {
                sleep(Duration::from_millis(retry_delay_ms)).await;
                retry_delay_ms = retry_delay_ms.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }

    unreachable!("rate limit retry loop should always return before exhausting attempts")
}
