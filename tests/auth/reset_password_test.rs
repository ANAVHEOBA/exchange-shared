use axum::http::StatusCode;
use serde_json::json;

use crate::common::{test_email, test_password, TestContext};

fn test_username() -> String {
    format!("user_{}", &uuid::Uuid::new_v4().to_string()[..8])
}

async fn create_user_and_get_reset_token(ctx: &TestContext) -> (String, String) {
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

    ctx.server
        .post("/auth/forgot-password")
        .json(&json!({
            "email": &email
        }))
        .await;

    // Get token from database (in real app, this would be sent via email)
    let token: String = sqlx::query_scalar(
        "SELECT token FROM password_resets pr
         JOIN users u ON pr.user_id = u.id
         WHERE u.email = ?
         ORDER BY pr.created_at DESC
         LIMIT 1"
    )
    .bind(&email)
    .fetch_one(&ctx.db)
    .await
    .unwrap();

    (email, token)
}

#[tokio::test]
async fn reset_password_with_valid_token_returns_success() {
    let ctx = TestContext::new().await;
    let (_, token) = create_user_and_get_reset_token(&ctx).await;

    let new_password = "NewPassword123!";

    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": new_password,
            "password_confirm": new_password
        }))
        .await;

    response.assert_status(StatusCode::OK);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_allows_login_with_new_password() {
    let ctx = TestContext::new().await;
    let (email, token) = create_user_and_get_reset_token(&ctx).await;

    let new_password = "NewPassword123!";

    ctx.server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": new_password,
            "password_confirm": new_password
        }))
        .await;

    // Login with new password
    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": new_password
        }))
        .await;

    response.assert_status(StatusCode::OK);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_invalidates_old_password() {
    let ctx = TestContext::new().await;
    let (email, token) = create_user_and_get_reset_token(&ctx).await;

    let new_password = "NewPassword123!";

    ctx.server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": new_password,
            "password_confirm": new_password
        }))
        .await;

    // Try login with old password
    let response = ctx
        .server
        .post("/auth/login")
        .json(&json!({
            "email": &email,
            "password": test_password()
        }))
        .await;

    response.assert_status(StatusCode::UNAUTHORIZED);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_with_invalid_token_returns_bad_request() {
    let ctx = TestContext::new().await;

    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": "invalid-token",
            "password": "NewPassword123!",
            "password_confirm": "NewPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_with_expired_token_returns_bad_request() {
    let ctx = TestContext::new().await;
    let (_, token) = create_user_and_get_reset_token(&ctx).await;

    // Manually expire the token
    sqlx::query("UPDATE password_resets SET expires_at = DATE_SUB(NOW(), INTERVAL 1 HOUR) WHERE token = ?")
        .bind(&token)
        .execute(&ctx.db)
        .await
        .unwrap();

    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": "NewPassword123!",
            "password_confirm": "NewPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_with_mismatched_passwords_returns_bad_request() {
    let ctx = TestContext::new().await;
    let (_, token) = create_user_and_get_reset_token(&ctx).await;

    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": "NewPassword123!",
            "password_confirm": "DifferentPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_with_weak_password_returns_bad_request() {
    let ctx = TestContext::new().await;
    let (_, token) = create_user_and_get_reset_token(&ctx).await;

    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": "weak",
            "password_confirm": "weak"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_token_can_only_be_used_once() {
    let ctx = TestContext::new().await;
    let (_, token) = create_user_and_get_reset_token(&ctx).await;

    let new_password = "NewPassword123!";

    // First reset
    ctx.server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": new_password,
            "password_confirm": new_password
        }))
        .await;

    // Second reset with same token
    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": &token,
            "password": "AnotherPassword123!",
            "password_confirm": "AnotherPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}

#[tokio::test]
async fn reset_password_with_missing_fields_returns_bad_request() {
    let ctx = TestContext::new().await;

    // Missing token
    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "password": "NewPassword123!",
            "password_confirm": "NewPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    // Missing password
    let response = ctx
        .server
        .post("/auth/reset-password")
        .json(&json!({
            "token": "some-token",
            "password_confirm": "NewPassword123!"
        }))
        .await;

    response.assert_status(StatusCode::BAD_REQUEST);

    ctx.cleanup().await;
}
