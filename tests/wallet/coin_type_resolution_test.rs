#[path = "../common/mod.rs"]
mod common;

use common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use exchange_shared::modules::wallet::schema::GenerateAddressRequest;
use exchange_shared::services::wallet::{derivation, manager::WalletManager};
use std::sync::Arc;
use uuid::Uuid;

async fn create_dummy_swap(
    db: &sqlx::Pool<sqlx::MySql>,
    swap_id: &str,
    to_currency: &str,
    to_network: &str,
    recipient_address: &str,
) {
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address, status
        )
        VALUES (?, 'changenow', 'BTC', 'bitcoin', ?, ?, 0.1, 1.5, 15.0, 'dep_addr', ?, 'waiting')
        "#,
    )
    .bind(swap_id)
    .bind(to_currency)
    .bind(to_network)
    .bind(recipient_address)
    .execute(db)
    .await
    .expect("Failed to create dummy swap");
}

#[test]
fn test_coin_type_resolution_matches_derivation_families() {
    let cases = [
        ("ETH", "ethereum", 60),
        ("LTC", "litecoin", 2),
        ("SOL", "solana", 501),
        ("ATOM", "cosmos", 118),
        ("DOT", "polkadot", 354),
        ("TRX", "tron", 195),
        ("ALGO", "algorand", 283),
        ("ADA", "cardano", 1815),
        ("XRP", "ripple", 144),
        ("TON", "ton", 607),
    ];

    for (ticker, network, expected) in cases {
        let actual = derivation::resolve_coin_type(ticker, network)
            .unwrap_or_else(|e| panic!("Failed to resolve coin type for {ticker}/{network}: {e}"));
        assert_eq!(
            actual, expected,
            "Unexpected coin type for {ticker}/{network}"
        );
    }
}

#[tokio::test]
async fn test_generated_addresses_persist_non_evm_coin_types() {
    let ctx = TestContext::new().await;
    let seed_phrase = common::test_wallet_mnemonic();
    let crud = WalletCrud::new(ctx.db.clone());
    let manager = WalletManager::new(crud.clone(), seed_phrase, Arc::new(common::NoOpProvider));

    let cases = [
        ("LTC", "litecoin", "LZGiLz4G1P4nujY2B8WcN6iYG3PaY5E9s3", 2),
        (
            "SOL",
            "solana",
            "6pQK2Y6V3HgNsx3Rp3XIanFkFJxuxMxDPZWS9Vyuk3F7",
            501,
        ),
        (
            "ATOM",
            "cosmos",
            "cosmos1hsk6jryyqf4m6l6thm2s0w4a3l8w9x5v5a6r9n",
            118,
        ),
        (
            "DOT",
            "polkadot",
            "14DqjVnWmX6RyvCb4s46HoPazTA7kGEXLMaLLq5yRvCNr4ie",
            354,
        ),
        ("TRX", "tron", "TQn9Y2khEsLJW1ChVWFMSMeRDow5KcbLSE", 195),
        ("ALGO", "algorand", "RECIPIENT-ALGO-ADDRESS", 283),
    ];

    for (ticker, network, recipient_address, expected_coin_type) in cases {
        let swap_id = Uuid::new_v4().to_string();
        create_dummy_swap(&ctx.db, &swap_id, ticker, network, recipient_address).await;

        manager
            .get_or_generate_address(GenerateAddressRequest {
                swap_id: swap_id.clone(),
                ticker: ticker.to_string(),
                network: network.to_string(),
                user_recipient_address: recipient_address.to_string(),
                user_recipient_extra_id: None,
            })
            .await
            .unwrap_or_else(|e| panic!("Failed to generate address for {ticker}/{network}: {e}"));

        let info = crud
            .get_address_info(&swap_id)
            .await
            .unwrap()
            .expect("Expected address info to be persisted");

        assert_eq!(
            info.coin_type, expected_coin_type,
            "Persisted coin type drifted for {ticker}/{network}"
        );
    }

    ctx.cleanup().await;
}
