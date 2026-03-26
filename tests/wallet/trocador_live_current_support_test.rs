#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::trocador::{TrocadorClient, TrocadorError};
use exchange_shared::services::wallet::{
    derivation,
    validation::{self, AddressValidation},
};
use serial_test::serial;
use std::collections::BTreeSet;
use tokio::time::{sleep, Duration};

const DEFAULT_DELAY_MS: u64 = 500;
const DEFAULT_MAX_LIVE_PAIRS: usize = 50;
const DEFAULT_SAMPLE_LIMIT: usize = 25;
const RATE_LIMIT_RETRY_ATTEMPTS: usize = 2;

#[derive(Default)]
struct ValidationSummary {
    live_pairs_fetched: usize,
    checked_pairs: usize,
    valid_pairs: usize,
    local_invalid_pairs: Vec<String>,
    local_unsupported_pairs: Vec<String>,
    derivation_errors: Vec<String>,
    rejected_pairs: Vec<String>,
    live_catalog_mismatches: Vec<String>,
    http_errors: Vec<String>,
    parse_errors: Vec<String>,
    rate_limit_errors: Vec<String>,
    api_errors: Vec<String>,
}

impl ValidationSummary {
    fn is_clean(&self) -> bool {
        self.local_invalid_pairs.is_empty()
            && self.local_unsupported_pairs.is_empty()
            && self.derivation_errors.is_empty()
            && self.rejected_pairs.is_empty()
            && self.live_catalog_mismatches.is_empty()
            && self.http_errors.is_empty()
            && self.parse_errors.is_empty()
            && self.rate_limit_errors.is_empty()
            && self.api_errors.is_empty()
    }

    fn render(&self) -> String {
        let mut sections = vec![
            format!("live_pairs_fetched: {}", self.live_pairs_fetched),
            format!("checked_pairs: {}", self.checked_pairs),
            format!("valid_pairs: {}", self.valid_pairs),
            format!("local_invalid_pairs: {}", self.local_invalid_pairs.len()),
            format!(
                "local_unsupported_pairs: {}",
                self.local_unsupported_pairs.len()
            ),
            format!("derivation_errors: {}", self.derivation_errors.len()),
            format!("rejected_pairs: {}", self.rejected_pairs.len()),
            format!(
                "live_catalog_mismatches: {}",
                self.live_catalog_mismatches.len()
            ),
            format!("http_errors: {}", self.http_errors.len()),
            format!("parse_errors: {}", self.parse_errors.len()),
            format!("rate_limit_errors: {}", self.rate_limit_errors.len()),
            format!("api_errors: {}", self.api_errors.len()),
        ];

        append_sample(
            &mut sections,
            "local_invalid_pair_samples",
            &self.local_invalid_pairs,
        );
        append_sample(
            &mut sections,
            "local_unsupported_pair_samples",
            &self.local_unsupported_pairs,
        );
        append_sample(
            &mut sections,
            "derivation_error_samples",
            &self.derivation_errors,
        );
        append_sample(&mut sections, "rejected_pair_samples", &self.rejected_pairs);
        append_sample(
            &mut sections,
            "live_catalog_mismatch_samples",
            &self.live_catalog_mismatches,
        );
        append_sample(&mut sections, "http_error_samples", &self.http_errors);
        append_sample(&mut sections, "parse_error_samples", &self.parse_errors);
        append_sample(
            &mut sections,
            "rate_limit_error_samples",
            &self.rate_limit_errors,
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
                    self.live_catalog_mismatches.push(message);
                } else {
                    self.api_errors.push(message);
                }
            }
        }
    }
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY and network access; validates only the current live /coins list against Trocador validateaddress"]
async fn test_live_trocador_validation_for_current_live_coin_list() {
    dotenvy::dotenv().ok();

    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY is required");
    let delay_ms = read_u64_env("TROCADOR_VALIDATE_DELAY_MS").unwrap_or(DEFAULT_DELAY_MS);
    let max_pairs = read_usize_env("TROCADOR_VALIDATE_MAX_PAIRS").unwrap_or(DEFAULT_MAX_LIVE_PAIRS);

    let client = TrocadorClient::new(api_key);
    let live_currencies = client
        .get_currencies()
        .await
        .expect("Failed to fetch live Trocador /coins list");

    let seed = common::test_wallet_mnemonic();
    let mut seen = BTreeSet::new();
    let mut summary = ValidationSummary {
        live_pairs_fetched: live_currencies.len(),
        ..ValidationSummary::default()
    };

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

    for currency in deduped_pairs.into_iter().take(target_pairs) {
        summary.checked_pairs += 1;

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
                summary
                    .local_invalid_pairs
                    .push(format!("{pair_label} -> {address} [{family}]: {reason}"));
                continue;
            }
            AddressValidation::Unsupported { family, reason } => {
                summary
                    .local_unsupported_pairs
                    .push(format!("{pair_label} -> {address} [{family}]: {reason}"));
                continue;
            }
        }

        match validate_with_rate_limit_retry(
            &client,
            &currency.ticker,
            &currency.network,
            &address,
            delay_ms,
        )
        .await
        {
            Ok(true) => summary.valid_pairs += 1,
            Ok(false) => summary.rejected_pairs.push(format!(
                "{pair_label} -> {address}: Trocador returned false"
            )),
            Err(err) => summary.push_validation_error(&pair_label, &address, err),
        }

        if summary.checked_pairs % 25 == 0 {
            eprintln!(
                "Validated {}/{} live /coins pairs so far",
                summary.checked_pairs, target_pairs
            );
        }
        sleep(Duration::from_millis(delay_ms)).await;
    }

    eprintln!("{}", summary.render());

    assert!(
        summary.is_clean(),
        "Live Trocador /coins validation found rejects or mismatches:\n{}",
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

fn append_sample(sections: &mut Vec<String>, label: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }

    let sample_limit =
        read_usize_env("TROCADOR_VALIDATE_SAMPLE_LIMIT").unwrap_or(DEFAULT_SAMPLE_LIMIT);
    let sample = items
        .iter()
        .take(sample_limit)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");

    sections.push(format!("{label}:\n{sample}"));
}
