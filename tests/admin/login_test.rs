use axum::http::StatusCode;
use exchange_shared::modules::admin::model::{DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD};
use serde_json::json;

use crate::common::TestContext;

#[tokio::test]
async fn admin_login_with_valid_credentials_returns_tokens() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/ops/login")
        .json(&json!({
            "email": DEFAULT_ADMIN_EMAIL,
            "password": DEFAULT_ADMIN_PASSWORD
        }))
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert!(body.get("access_token").is_some());
    assert!(body.get("refresh_token").is_some());
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["admin"]["email"], DEFAULT_ADMIN_EMAIL);

    ctx.cleanup().await;
}

#[tokio::test]
async fn admin_login_with_invalid_password_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/ops/login")
        .json(&json!({
            "email": DEFAULT_ADMIN_EMAIL,
            "password": "wrong-password"
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json();
    assert!(body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Invalid admin email or password"));

    ctx.cleanup().await;
}
