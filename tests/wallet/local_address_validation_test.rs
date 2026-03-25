#[path = "../common/mod.rs"]
mod common;

use exchange_shared::services::wallet::{derivation, validation::AddressValidation};
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct SnapshotCurrency {
    ticker: String,
    network: String,
}

#[derive(Default)]
struct ValidationSummary {
    checked_pairs: usize,
    valid_pairs: usize,
    invalid_pairs: Vec<String>,
    unsupported_pairs: Vec<String>,
}

#[tokio::test]
async fn test_bundled_snapshot_pairs_pass_local_family_validation() {
    let snapshot: Vec<SnapshotCurrency> =
        serde_json::from_str(include_str!("../../trocador_currencies_full.json"))
            .expect("Failed to parse bundled Trocador snapshot");

    let seed = common::test_wallet_mnemonic();
    let mut seen = BTreeSet::new();
    let mut summary = ValidationSummary::default();

    for currency in snapshot {
        let key = (
            currency.ticker.to_ascii_lowercase(),
            currency.network.to_ascii_lowercase(),
        );
        if !seen.insert(key) {
            continue;
        }

        summary.checked_pairs += 1;
        let address = derivation::derive_address(&seed, &currency.ticker, &currency.network, 0)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "Derivation failed for local validation {}/{}: {}",
                    currency.ticker, currency.network, err
                )
            });

        match exchange_shared::services::wallet::validation::validate_address_by_network_family(
            &currency.ticker,
            &currency.network,
            &address,
        ) {
            AddressValidation::Valid { .. } => summary.valid_pairs += 1,
            AddressValidation::Invalid { family, reason } => summary.invalid_pairs.push(format!(
                "{}/{} -> {} [{}]: {}",
                currency.ticker, currency.network, address, family, reason
            )),
            AddressValidation::Unsupported { family, reason } => {
                summary.unsupported_pairs.push(format!(
                    "{}/{} -> {} [{}]: {}",
                    currency.ticker, currency.network, address, family, reason
                ))
            }
        }
    }

    eprintln!(
        "checked_pairs: {}\nvalid_pairs: {}\ninvalid_pairs: {}\nunsupported_pairs: {}",
        summary.checked_pairs,
        summary.valid_pairs,
        summary.invalid_pairs.len(),
        summary.unsupported_pairs.len()
    );

    if !summary.unsupported_pairs.is_empty() {
        eprintln!(
            "unsupported_pair_samples:\n{}",
            summary
                .unsupported_pairs
                .iter()
                .take(25)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    assert!(
        summary.invalid_pairs.is_empty(),
        "Local family validation found invalid derived addresses:\n{}",
        summary
            .invalid_pairs
            .iter()
            .take(50)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
