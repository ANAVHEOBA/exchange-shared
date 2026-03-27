use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", &uuid::Uuid::new_v4().to_string()[..8])
}

async fn create_and_login(ctx: &TestContext) -> (String, String, String) {
    let email = test_email();

    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": test_username(),
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

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    let body: serde_json::Value = response.json();
    let access_token = body["access_token"].as_str().unwrap().to_string();
    let refresh_token = body["refresh_token"].as_str().unwrap().to_string();

    (email, access_token, refresh_token)
}

#[tokio::test]
async fn logout_with_valid_token_returns_success() {
    let ctx = TestContext::new().await;
    let (_, access_token, refresh_token) = create_and_login(&ctx).await;

    let response = ctx
        .server
        .post("/auth/logout")
        .authorization_bearer(&access_token)
        .json(&json!({
            "refresh_token": &refresh_token
        }))
        .await;

    response.assert_status(StatusCode::OK);

    ctx.cleanup().await;
}

#[tokio::test]
async fn logout_invalidates_refresh_token() {
    let ctx = TestContext::new().await;
    let (_, access_token, refresh_token) = create_and_login(&ctx).await;

    // Logout
    ctx.server
        .post("/auth/logout")
        .authorization_bearer(&access_token)
        .json(&json!({
            "refresh_token": &refresh_token
        }))
        .await;

    // Try to use refresh token after logout
    let response = ctx
        .server
        .post("/auth/refresh")
        .json(&json!({
            "refresh_token": &refresh_token
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn logout_without_auth_header_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/logout")
        .json(&json!({
            "refresh_token": "some-token"
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn logout_with_invalid_token_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/logout")
        .authorization_bearer("invalid-token")
        .json(&json!({
            "refresh_token": "some-token"
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn logout_with_expired_token_returns_unauthorized() {
    let ctx = TestContext::new().await;

    // Using a properly formatted but expired JWT
    let expired_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxfQ.invalid";

    let response = ctx
        .server
        .post("/auth/logout")
        .authorization_bearer(expired_token)
        .json(&json!({
            "refresh_token": "some-token"
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}
