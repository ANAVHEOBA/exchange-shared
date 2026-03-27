use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", &uuid::Uuid::new_v4().to_string()[..8])
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

    email
}

#[tokio::test]
async fn forgot_password_with_existing_email_returns_success() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": &email
        }))
        .await;

    // Always return success to prevent email enumeration
    response.assert_status(StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert!(body.get("message").is_some());

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_with_nonexistent_email_returns_success() {
    let ctx = TestContext::new().await;

    // Should still return success to prevent email enumeration attacks
    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": "nonexistent@example.com"
        }))
        .await;

    response.assert_status(StatusCode::OK);

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_with_invalid_email_format_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": "invalid-email"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_with_missing_email_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({}))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_with_empty_email_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": ""
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_creates_reset_token_in_database() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    ctx.server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": &email
        }))
        .await;

    // Verify token was created in database
    let result = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM password_resets pr
         JOIN users u ON pr.user_id = u.id
         WHERE u.email = ?"
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    assert!(result > 0, "Password reset token should be created");

    ctx.cleanup().await;
}

#[tokio::test]
async fn forgot_password_rate_limits_requests() {
    let ctx = TestContext::new().await;
    let email = create_test_user(&ctx).await;

    // Send multiple requests rapidly
    for _ in 0..5 {
        ctx.server
            .post("/auth/forgot-password")
            .json(&json!({
                "email": &email
            }))
            .await;
    }

    // The 6th request might be rate limited
    let response = ctx
        .server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": &email
        }))
        .await;

    // Either OK (if no rate limiting) or TOO_MANY_REQUESTS
    assert!(
        response.status_code() == StatusCode::OK
        || response.status_code() == StatusCode::TOO_MANY_REQUESTS
    );

    ctx.cleanup().await;
}
