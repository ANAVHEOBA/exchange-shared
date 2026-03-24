use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", uuid::Uuid::new_v4().to_string()[..8].to_string())
}

async fn create_unverified_user(ctx: &TestContext) -> String {
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

    email
}

// =============================================================================
// EMAIL VERIFICATION REQUIREMENT
// =============================================================================

#[tokio::test]
async fn unverified_user_cannot_login() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

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
async fn registration_creates_verification_token() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Check that verification token was created
    let count: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    assert!(
        count.0 > 0,
        "Verification token should be created on registration"
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn verify_email_with_valid_token_succeeds() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Get token from database
    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?
         ORDER BY ev.created_at DESC
         LIMIT 1",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    // Verify email using GET endpoint with query param
    let response = ctx
        .server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert!(body["message"].as_str().unwrap().contains("verified"));

    ctx.cleanup().await;
}

#[tokio::test]
async fn verify_email_updates_user_verified_status() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Get token
    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    // Verify
    ctx.server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    // Check user status in database
    let verified: bool = sqlx::query_scalar("SELECT email_verified FROM users WHERE email = ?")
        .bind(&email)
        .fetch_one(&ctx.db)
        .await
        .unwrap();

    assert_eq!(verified, true, "User should be verified");

    ctx.cleanup().await;
}

#[tokio::test]
async fn verified_user_can_login() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Get and use verification token
    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    ctx.server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    // Now login should work
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

    ctx.cleanup().await;
}

#[tokio::test]
async fn verify_email_with_invalid_token_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .get("/auth/verify-email?token=invalid-token-12345")
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    let body: serde_json::Value = response.json();
    assert!(body.get("error").is_some());

    ctx.cleanup().await;
}

#[tokio::test]
async fn verify_email_with_expired_token_returns_bad_request() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Expire the token manually
    sqlx::query(
        "UPDATE email_verifications ev
         JOIN users u ON ev.user_id = u.id
         SET ev.expires_at = DATE_SUB(NOW(), INTERVAL 1 HOUR)
         WHERE u.email = ?",
    )
    .bind(&email)
    .execute(&ctx.db)
    .await
    .unwrap();

    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    let response = ctx
        .server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    let body: serde_json::Value = response.json();
    assert!(body["error"].as_str().unwrap().contains("expired"));

    ctx.cleanup().await;
}

#[tokio::test]
async fn verification_token_can_only_be_used_once() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    // First verification
    ctx.server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    // Second verification with same token
    let response = ctx
        .server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn verification_token_deleted_after_successful_verification() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    let token: String = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    // Verify
    ctx.server
        .get(&format!("/auth/verify-email?token={}", token))
        .await;

    // Check token is deleted
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM email_verifications WHERE token = ?")
        .bind(&token)
        .fetch_one(&ctx.db)
        .await
        .unwrap();

    assert_eq!(count.0, 0, "Token should be deleted after verification");

    ctx.cleanup().await;
}

// =============================================================================
// EXPIRED UNVERIFIED ACCOUNT REPLACEMENT
// =============================================================================

#[tokio::test]
async fn can_register_again_with_unverified_email() {
    let ctx = TestContext::new().await;
    let email = test_email();

    // First registration
    let response1 = ctx
        .server
        .post("/auth/register")
        .json(&json!({
            "username": "firstuser",
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    response1.assert_status(StatusCode::CREATED);

    // Give a moment for the database transaction to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second registration with same email (should replace unverified account)
    let response2 = ctx
        .server
        .post("/auth/register")
        .json(&json!({
            "username": "seconduser",
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    response2.assert_status(StatusCode::CREATED);

    // Give a moment for the database transaction to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Only one user should exist
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users WHERE email = ?")
        .bind(&email)
        .fetch_one(&ctx.db)
        .await
        .unwrap();

    assert_eq!(count.0, 1, "Only one user should exist");

    // The username should be "seconduser"
    let username: Option<String> = sqlx::query_scalar("SELECT username FROM users WHERE email = ?")
        .bind(&email)
        .fetch_optional(&ctx.db)
        .await
        .unwrap();

    assert_eq!(
        username,
        Some("seconduser".to_string()),
        "Second registration should replace first"
    );

    ctx.cleanup().await;
}

#[tokio::test]
async fn cannot_register_with_verified_email() {
    let ctx = TestContext::new().await;
    let email = test_email();

    // First registration
    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": test_username(),
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    // Verify the user
    sqlx::query("UPDATE users SET email_verified = TRUE WHERE email = ?")
        .bind(&email)
        .execute(&ctx.db)
        .await
        .unwrap();

    // Try to register again
    let response = ctx
        .server
        .post("/auth/register")
        .json(&json!({
            "username": test_username(),
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    response.assert_status(StatusCode::CONFLICT);

    let body: serde_json::Value = response.json();
    assert!(body["error"].as_str().unwrap().contains("verified"));

    ctx.cleanup().await;
}

#[tokio::test]
async fn old_verification_tokens_deleted_when_account_replaced() {
    let ctx = TestContext::new().await;
    let email = test_email();

    // First registration
    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": "firstuser",
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    // Give time for email verification token to be created
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Get first token
    let first_token: Option<String> = sqlx::query_scalar(
        "SELECT token FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_optional(&ctx.db)
    .await
    .unwrap();

    // If no token was created (email service not configured), skip this test
    if first_token.is_none() {
        ctx.cleanup().await;
        return;
    }

    let first_token = first_token.unwrap();

    // Second registration (replaces first)
    ctx.server
        .post("/auth/register")
        .json(&json!({
            "username": "seconduser",
            "email": &email,
            "password": test_password(),
            "password_confirm": test_password()
        }))
        .await;

    // Give time for deletion to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // First token should be invalid
    let response = ctx
        .server
        .get(&format!("/auth/verify-email?token={}", first_token))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn verification_token_expires_after_24_hours() {
    let ctx = TestContext::new().await;
    let email = create_unverified_user(&ctx).await;

    // Check token expiration is set to ~24 hours from now
    let expires_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT expires_at FROM email_verifications ev
         JOIN users u ON ev.user_id = u.id
         WHERE u.email = ?",
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    let now = chrono::Utc::now();
    let hours_until_expiry = (expires_at - now).num_hours();

    // Should be approximately 24 hours (allow 23-25 for test timing)
    assert!(
        hours_until_expiry >= 23 && hours_until_expiry <= 25,
        "Token should expire in ~24 hours, got {} hours",
        hours_until_expiry
    );

    ctx.cleanup().await;
}
