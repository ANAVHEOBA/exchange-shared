use axum::http::StatusCode;
use csv::ReaderBuilder;
use exchange_shared::modules::admin::model::{DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD};
use serde::Deserialize;
use serde_json::json;
use serial_test::serial;
use uuid::Uuid;

use crate::common::TestContext;

#[derive(Debug, Deserialize)]
struct ExportRow {
    swap_id: String,
    provider: String,
    provider_swap_id: Option<String>,
    client_id: Option<String>,
    tx_hash_in: Option<String>,
    tx_hash_out: Option<String>,
}

async fn admin_token(ctx: &TestContext) -> String {
    let response = ctx
        .server
        .post("/admin/login")
        .json(&json!({
            "email": DEFAULT_ADMIN_EMAIL,
            "password": DEFAULT_ADMIN_PASSWORD
        }))
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    body["access_token"]
        .as_str()
        .expect("access token")
        .to_string()
}

async fn insert_export_test_swap(ctx: &TestContext, provider_swap_id: &str) -> String {
    let swap_id = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4().to_string();

    sqlx::query(
        "INSERT INTO swaps (
            id, user_id, client_id, provider_id, provider_swap_id,
            from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, actual_receive, rate,
            network_fee, provider_fee, platform_fee, total_fee,
            deposit_address, deposit_extra_id, recipient_address, recipient_extra_id,
            refund_address, refund_extra_id, tx_hash_in, tx_hash_out,
            status, rate_type, is_sandbox, is_payment, error,
            completed_at, created_at, updated_at
        ) VALUES (
            ?, NULL, ?, 'changenow', ?,
            'BTC', 'Mainnet', 'ETH', 'Ethereum',
            0.10000000, 1.50000000, 1.49000000, 15.0,
            0.00100000, 0.00200000, 0.00300000, 0.00600000,
            'bc1testdeposit', 'memo-in', '0xtestrecipient', 'memo-out',
            'bc1refund', 'refund-memo', 'in-hash-123', 'out-hash-456',
            'completed', 'fixed', TRUE, FALSE, NULL,
            NOW(), NOW(), NOW()
        )",
    )
    .bind(&swap_id)
    .bind(&client_id)
    .bind(provider_swap_id)
    .execute(&ctx.db)
    .await
    .expect("Failed to insert export test swap");

    swap_id
}

#[tokio::test]
#[serial]
async fn admin_swap_export_requires_authentication() {
    let ctx = TestContext::new().await;

    let response = ctx.server.get("/admin/swaps/export").await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json();
    assert!(body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Missing authorization header"));

    ctx.cleanup().await;
}

#[tokio::test]
#[serial]
async fn admin_swap_export_returns_csv_attachment() {
    let ctx = TestContext::new().await;
    let token = admin_token(&ctx).await;
    let provider_swap_id = format!("export-trade-{}", Uuid::new_v4().simple());
    let swap_id = insert_export_test_swap(&ctx, &provider_swap_id).await;

    let response = ctx
        .server
        .get(&format!(
            "/admin/swaps/export?provider_swap_id={}",
            provider_swap_id
        ))
        .authorization_bearer(&token)
        .await;

    response.assert_status(StatusCode::OK);

    let content_type = response
        .maybe_header("content-type")
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_default()
        .to_string();
    assert!(content_type.contains("text/csv"));

    let content_disposition = response
        .maybe_header("content-disposition")
        .and_then(|value| value.to_str().ok().map(str::to_owned))
        .unwrap_or_default()
        .to_string();
    assert!(content_disposition.contains("attachment;"));
    assert!(content_disposition.contains(".csv"));

    let body = response.text();
    assert!(body.starts_with("swap_id,user_id,client_id,provider,provider_swap_id"));

    let mut reader = ReaderBuilder::new().from_reader(body.as_bytes());
    let rows: Vec<ExportRow> = reader
        .deserialize()
        .collect::<Result<Vec<_>, _>>()
        .expect("valid csv rows");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].swap_id, swap_id);
    assert_eq!(rows[0].provider, "changenow");
    assert_eq!(
        rows[0].provider_swap_id.as_deref(),
        Some(provider_swap_id.as_str())
    );
    assert!(rows[0].client_id.is_some());
    assert_eq!(rows[0].tx_hash_in.as_deref(), Some("in-hash-123"));
    assert_eq!(rows[0].tx_hash_out.as_deref(), Some("out-hash-456"));

    ctx.cleanup().await;
}
