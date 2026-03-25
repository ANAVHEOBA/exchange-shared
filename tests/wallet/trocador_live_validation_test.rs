#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::trocador::{TrocadorClient, TrocadorError};
use exchange_shared::services::wallet::{
    derivation,
    validation::{self, AddressValidation},
};
use serde::Deserialize;
use serial_test::serial;
use std::collections::BTreeSet;
use tokio::time::{sleep, Duration};

const DEFAULT_DELAY_MS: u64 = 500;
const DEFAULT_MAX_LIVE_PAIRS: usize = 50;
const DEFAULT_SAMPLE_LIMIT: usize = 25;
const RATE_LIMIT_RETRY_ATTEMPTS: usize = 2;

#[derive(Deserialize)]
struct SnapshotCurrency {
    ticker: String,
    network: String,
}

struct SuspiciousPair {
    ticker: String,
    network: String,
    address: String,
    local_family: &'static str,
    local_reason: String,
    local_status: &'static str,
}

#[derive(Default)]
struct ValidationSummary {
    suspicious_pairs_found: usize,
    smoke_pairs_used: usize,
    local_invalid_pairs: usize,
    local_unsupported_pairs: usize,
    checked_pairs: usize,
    valid_pairs: usize,
    derivation_errors: Vec<String>,
    rejected_pairs: Vec<String>,
    http_errors: Vec<String>,
    parse_errors: Vec<String>,
    rate_limit_errors: Vec<String>,
    unsupported_pairs: Vec<String>,
    api_errors: Vec<String>,
}

impl ValidationSummary {
    fn is_clean(&self) -> bool {
        self.derivation_errors.is_empty()
            && self.rejected_pairs.is_empty()
            && self.http_errors.is_empty()
            && self.parse_errors.is_empty()
            && self.rate_limit_errors.is_empty()
            && self.unsupported_pairs.is_empty()
            && self.api_errors.is_empty()
    }

    fn render(&self) -> String {
        let mut sections = vec![
            format!("suspicious_pairs_found: {}", self.suspicious_pairs_found),
            format!("smoke_pairs_used: {}", self.smoke_pairs_used),
            format!("local_invalid_pairs: {}", self.local_invalid_pairs),
            format!("local_unsupported_pairs: {}", self.local_unsupported_pairs),
            format!("checked_pairs: {}", self.checked_pairs),
            format!("valid_pairs: {}", self.valid_pairs),
            format!("derivation_errors: {}", self.derivation_errors.len()),
            format!("rejected_pairs: {}", self.rejected_pairs.len()),
            format!("http_errors: {}", self.http_errors.len()),
            format!("parse_errors: {}", self.parse_errors.len()),
            format!("rate_limit_errors: {}", self.rate_limit_errors.len()),
            format!("unsupported_pairs: {}", self.unsupported_pairs.len()),
            format!("api_errors: {}", self.api_errors.len()),
        ];

        append_sample(
            &mut sections,
            "derivation_error_samples",
            &self.derivation_errors,
        );
        append_sample(&mut sections, "rejected_pair_samples", &self.rejected_pairs);
        append_sample(&mut sections, "http_error_samples", &self.http_errors);
        append_sample(&mut sections, "parse_error_samples", &self.parse_errors);
        append_sample(
            &mut sections,
            "rate_limit_error_samples",
            &self.rate_limit_errors,
        );
        append_sample(
            &mut sections,
            "unsupported_pair_samples",
            &self.unsupported_pairs,
        );
        append_sample(&mut sections, "api_error_samples", &self.api_errors);

        sections.join("\n")
    }

    fn push_validation_error(&mut self, pair_label: &str, address: &str, err: TrocadorError) {
        let message = format!("{pair_label} -> {address}: {err}");
        match err {
            TrocadorError::HttpError(_) => self.http_errors.push(message),
            TrocadorError::ParseError(_) => self.parse_errors.push(message),
            TrocadorError::ApiError(api_message) => {
                let lowered = api_message.to_ascii_lowercase();
                if lowered.contains("rate limit exceeded") {
                    self.rate_limit_errors.push(message);
                } else if lowered.contains("coin not found") {
                    self.unsupported_pairs.push(message);
                } else {
                    self.api_errors.push(message);
                }
            }
        }
    }
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY and network access; validates only locally suspicious snapshot pairs against Trocador validateaddress"]
async fn test_live_trocador_validation_for_locally_suspicious_snapshot_pairs() {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY is required");
    let delay_ms = read_u64_env("TROCADOR_VALIDATE_DELAY_MS").unwrap_or(DEFAULT_DELAY_MS);
    let max_pairs = read_usize_env("TROCADOR_VALIDATE_MAX_PAIRS").unwrap_or(DEFAULT_MAX_LIVE_PAIRS);
    let include_unsupported = read_bool_env("TROCADOR_VALIDATE_INCLUDE_UNSUPPORTED");

    let snapshot: Vec<SnapshotCurrency> =
        serde_json::from_str(include_str!("../../trocador_currencies_full.json"))
            .expect("Failed to parse bundled Trocador snapshot");

    let seed = common::test_wallet_mnemonic();
    let mut seen = BTreeSet::new();
    let mut invalid_pairs = Vec::new();
    let mut unsupported_pairs = Vec::new();
    let mut summary = ValidationSummary::default();

    for currency in snapshot {
        let key = (
            currency.ticker.to_ascii_lowercase(),
            currency.network.to_ascii_lowercase(),
        );
        if !seen.insert(key) {
            continue;
        }

        let pair_label = format!("{}/{}", currency.ticker, currency.network);

        let address =
            match derivation::derive_address(&seed, &currency.ticker, &currency.network, 0).await {
                Ok(address) => address,
                Err(err) => {
                    summary
                        .derivation_errors
                        .push(format!("{pair_label}: {err}"));
                    continue;
                }
            };

        match validation::validate_address_by_network_family(
            &currency.ticker,
            &currency.network,
            &address,
        ) {
            AddressValidation::Valid { .. } => {}
            AddressValidation::Invalid { family, reason } => {
                summary.local_invalid_pairs += 1;
                invalid_pairs.push(SuspiciousPair {
                    ticker: currency.ticker,
                    network: currency.network,
                    address,
                    local_family: family,
                    local_reason: reason,
                    local_status: "invalid",
                });
            }
            AddressValidation::Unsupported { family, reason } => {
                summary.local_unsupported_pairs += 1;
                unsupported_pairs.push(SuspiciousPair {
                    ticker: currency.ticker,
                    network: currency.network,
                    address,
                    local_family: family,
                    local_reason: reason,
                    local_status: "unsupported",
                });
            }
        }
    }

    let mut suspicious_pairs = invalid_pairs;
    if include_unsupported {
        suspicious_pairs.extend(unsupported_pairs);
    }

    if suspicious_pairs.is_empty() {
        for (ticker, network) in live_smoke_pairs() {
            let address = derivation::derive_address(&seed, ticker, network, 0)
                .await
                .unwrap_or_else(|err| {
                    panic!("Failed to derive smoke-test address for {ticker}/{network}: {err}")
                });

            suspicious_pairs.push(SuspiciousPair {
                ticker: (*ticker).to_string(),
                network: (*network).to_string(),
                address,
                local_family: "smoke",
                local_reason: "historically broken route/encoder smoke case".to_string(),
                local_status: "smoke",
            });
        }
        summary.smoke_pairs_used = suspicious_pairs.len();
    }

    summary.suspicious_pairs_found = suspicious_pairs.len();
    if suspicious_pairs.is_empty() {
        eprintln!("No locally suspicious pairs found; skipping live Trocador validation.");
        return;
    }

    let target_pairs = suspicious_pairs.len().min(max_pairs);
    let client = TrocadorClient::new(api_key);

    for pair in suspicious_pairs.into_iter().take(target_pairs) {
        summary.checked_pairs += 1;
        let pair_label = format!(
            "{}/{} [{} {}]",
            pair.ticker, pair.network, pair.local_status, pair.local_family
        );

        match validate_with_rate_limit_retry(
            &client,
            &pair.ticker,
            &pair.network,
            &pair.address,
            delay_ms,
        )
        .await
        {
            Ok(true) => summary.valid_pairs += 1,
            Ok(false) => summary.rejected_pairs.push(format!(
                "{pair_label} -> {}: {}",
                pair.address, pair.local_reason
            )),
            Err(err) => summary.push_validation_error(&pair_label, &pair.address, err),
        }

        if summary.checked_pairs % 25 == 0 {
            eprintln!(
                "Validated {}/{} suspicious snapshot pairs so far",
                summary.checked_pairs, target_pairs
            );
        }
        sleep(Duration::from_millis(delay_ms)).await;
    }

    eprintln!("{}", summary.render());

    assert!(
        summary.is_clean(),
        "Live Trocador snapshot validation found rejects or inconclusive pairs:\n{}",
        summary.render()
    );
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

fn read_bool_env(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(false)
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

fn live_smoke_pairs() -> &'static [(&'static str, &'static str)] {
    &[
        ("xrp", "Mainnet"),
        ("xmr", "Mainnet"),
        ("bch", "Mainnet"),
        ("algo", "Mainnet"),
        ("xlm", "Mainnet"),
        ("stx", "Mainnet"),
        ("zec", "Mainnet"),
        ("band", "ERC20"),
        ("atom", "BEP20"),
        ("atom", "Mainnet"),
        ("bnb", "BEP2"),
        ("avax", "AVAXX"),
        ("egld", "Mainnet"),
        ("lunc", "Mainnet"),
        ("dot", "ERC20"),
        ("inj", "ERC20"),
    ]
}

fn append_sample(sections: &mut Vec<String>, label: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }

    let sample = items
        .iter()
        .take(DEFAULT_SAMPLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

    sections.push(format!("{label}:\n{sample}"));
}
