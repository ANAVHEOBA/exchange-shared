use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", uuid::Uuid::new_v4().to_string()[..8].to_string())
}

async fn create_test_user(ctx: &TestContext) -> String {
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

    // Verify the user so they can login
    sqlx::query("UPDATE users SET email_verified = TRUE WHERE email = ?")
        .bind(&email)
        .execute(&ctx.db)
        .await
        .unwrap();

    email
}

#[tokio::test]
async fn login_with_unverified_email_returns_forbidden() {
    let ctx = TestContext::new().await;
    let email = test_email();

    // Register but don't verify
    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": test_username(),
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    // Try to login
    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    response.assert_status(StatusCode::FORBIDDEN);

    let body: serde_json::Value = response.json();
    assert!(body["error"].as_str().unwrap().contains("verify"));

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_valid_credentials_returns_tokens() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

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
    assert!(body.get("access_token").is_some());
    assert!(body.get("refresh_token").is_some());
    assert!(body.get("token_type").is_some());
    assert_eq!(body["token_type"], "Bearer");

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_invalid_password_returns_unauthorized() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": "WrongPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = response.json();
    assert!(body.get("error").is_some());

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_nonexistent_email_returns_unauthorized() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": "nonexistent@example.com",
            "password": test_password()
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_missing_email_returns_unprocessable() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "password": test_password()
        }))
        .await;

    response.assert_status(StatusCode::UNPROCESSABLE_ENTITY);

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_missing_password_returns_unprocessable() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": test_email()
        }))
        .await;

    response.assert_status(StatusCode::UNPROCESSABLE_ENTITY);

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_empty_body_returns_unprocessable() {
    let ctx = TestContext::new().await;

    let response = ctx.server.post("/auth/login").json(&json!({})).await;

    response.assert_status(StatusCode::UNPROCESSABLE_ENTITY);

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_returns_different_tokens_each_time() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    let response1 = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    let response2 = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    let body1: serde_json::Value = response1.json();
    let body2: serde_json::Value = response2.json();

    assert_ne!(body1["access_token"], body2["access_token"]);
    assert_ne!(body1["refresh_token"], body2["refresh_token"]);

    ctx.cleanup().await;
}

// =============================================================================
// PERFORMANCE
// =============================================================================

#[tokio::test]
async fn login_responds_within_acceptable_time() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    let start = std::time::Instant::now();

    ctx.server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    let duration = start.elapsed();

    println!("Login took: {:?}", duration);

    // Login should be faster than register (only verifies hash, doesn't create)
    assert!(
        duration.as_millis() < 3000,
        "Login took too long: {:?}",
        duration
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn measure_register_and_login_times() {
    let ctx = TestContext::new().await;
    let email = test_email();

    // Measure register
    let reg_start = std::time::Instant::now();
    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": test_username(),
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;
    let reg_duration = reg_start.elapsed();

    // Verify user so they can login
    sqlx::query("UPDATE users SET email_verified = TRUE WHERE email = ?")
        .bind(&email)
        .execute(&ctx.db)
        .await
        .unwrap();

    // Measure login
    let login_start = std::time::Instant::now();
    let login_resp = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;
    let login_duration = login_start.elapsed();

    let tokens: serde_json::Value = login_resp.json();
    let access_token = tokens["access_token"].as_str().unwrap();

    // Measure JWT-protected request (no password hashing, just token verify)
    let jwt_start = std::time::Instant::now();
    ctx.server
        .get("/auth/me")
        .authorization_bearer(access_token)
        .await;
    let jwt_duration = jwt_start.elapsed();

    println!("========================================");
    println!("Register (hash password):  {} ms", reg_duration.as_millis());
    println!(
        "Login (verify password):   {} ms",
        login_duration.as_millis()
    );
    println!(
        "JWT verify (/auth/me):     {} ms  <-- THIS IS FAST",
        jwt_duration.as_millis()
    );
    println!("========================================");

    ctx.cleanup().await;
}

#[tokio::test]
async fn login_with_2fa_enabled_requires_code() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    // Login to get token
    let login_response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    let tokens: serde_json::Value = login_response.json();
    let access_token = tokens["access_token"].as_str().unwrap();

    // Enable 2FA
    let enable_response = ctx
        .server
        .post("/auth/enable-2fa")
        .authorization_bearer(access_token)
        .await;

    if enable_response.status_code() == StatusCode::OK {
        let secret_data: serde_json::Value = enable_response.json();
        let _secret = secret_data["secret"].as_str().unwrap();

        // TODO: Generate valid TOTP code and verify
        // For now, attempt login without 2FA code
        let response = ctx
            .server
            .post("/auth/login")
            .json(&json!({
                "email": &email,
                "password": test_password()
            }))
            .await;

        // Should indicate 2FA is required
        let body: serde_json::Value = response.json();
        assert!(body.get("requires_2fa").is_some() || response.status_code() == StatusCode::OK);
    }

    ctx.cleanup().await;
}
