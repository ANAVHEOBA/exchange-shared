use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", &uuid::Uuid::new_v4().to_string()[..8])
}

async fn create_and_login(ctx: &TestContext) -> (String, String, String, String) {
    let email = test_email();
    let username = test_username();

    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": &username,
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    sqlx::query("UPDATE users SET email_verified = TRUE WHERE email = ?")
        .bind(&email)
        .execute(&ctx.db)
        .await
        .unwrap();

    let user_id: String = sqlx::query_scalar("SELECT id FROM users WHERE email = ?")
        .bind(&email)
        .fetch_one(&ctx.db)
        .await
        .unwrap();

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    let access_token = body["access_token"].as_str().unwrap().to_string();

    (email, username, access_token, user_id)
}

async fn insert_completed_btc_swap(ctx: &TestContext, user_id: &str, amount: f64) {
    sqlx::query(
        r#"
        INSERT INTO providers (id, name, slug, is_active, kyc_rating, insurance_percentage, eta_minutes, markup_enabled)
        VALUES (?, ?, ?, TRUE, 'C', 0.015, 10, FALSE)
        ON DUPLICATE KEY UPDATE id = id
        "#,
    )
    .bind("test_provider")
    .bind("Test Provider")
    .bind("test_provider")
    .execute(&ctx.db)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, user_id, client_id, provider_id, provider_swap_id,
            from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, network_fee,
            deposit_address, deposit_extra_id,
            recipient_address, recipient_extra_id,
            refund_address, refund_extra_id,
            platform_fee, total_fee,
            status, rate_type, is_sandbox, is_payment,
            expires_at, completed_at, created_at, updated_at
        )
        VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, ?, ?, ?, ?, FALSE, FALSE, DATE_ADD(NOW(), INTERVAL 1 HOUR), NOW(), NOW(), NOW())
        "#,
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(user_id)
    .bind("test_provider")
    .bind(uuid::Uuid::new_v4().to_string())
    .bind("btc")
    .bind("Mainnet")
    .bind("eth")
    .bind("ERC20")
    .bind(amount)
    .bind(3.5)
    .bind(14.0)
    .bind(0.0001)
    .bind("bc1qtestdepositaddress")
    .bind("0xrecipientaddress")
    .bind(0.0)
    .bind(0.0)
    .bind("completed")
    .bind("floating")
    .execute(&ctx.db)
    .await
    .unwrap();
}

#[tokio::test]
async fn me_returns_minimal_profile_fields() {
    let ctx = TestContext::new().await;
    let (email, username, access_token, _) = create_and_login(&ctx).await;

    let response = ctx
        .server
        .get("/auth/me")
        .authorization_bearer(&access_token)
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert_eq!(body["email"], email);
    assert_eq!(body["username"], username);
    assert_eq!(body["total_trades"], 0);
    assert_eq!(body["traded_value_btc"], 0.0);
    assert!(body.get("id").is_none());
    assert!(body.get("dashboard").is_none());
    assert!(body.get("password_hash").is_none());

    ctx.cleanup().await;
}

#[tokio::test]
async fn me_includes_completed_btc_trade_value() {
    let ctx = TestContext::new().await;
    let (_, _, access_token, user_id) = create_and_login(&ctx).await;

    insert_completed_btc_swap(&ctx, &user_id, 0.25).await;

    let response = ctx
        .server
        .get("/auth/me")
        .authorization_bearer(&access_token)
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert_eq!(body["total_trades"], 1);
    assert_eq!(body["traded_value_btc"], 0.25);

    ctx.cleanup().await;
}

#[tokio::test]
async fn me_without_auth_header_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx.server.get("/auth/me").await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn me_with_invalid_token_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .get("/auth/me")
        .authorization_bearer("invalid-token")
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}
