use serial_test::serial;
use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_app, create_test_user, create_test_swap};

use axum_test::TestServer;

// =============================================================================
// SWAP HISTORY TESTS - Keyset Pagination
// =============================================================================

#[tokio::test]
#[serial]
async fn test_history_requires_authentication() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Try to access history without authentication
    let response = server.get("/swap/history").await;
    
    assert_eq!(response.status_code(), 401, "Should require authentication");
}

#[tokio::test]
#[serial]
async fn test_history_first_page_empty() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (_user_id, token) = create_test_user(&server, "history1@test.com", "password123").await;

    // Get history (should be empty)
    let response = server
        .get("/swap/history")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    
    let json: Value = response.json();
    assert!(json["swaps"].is_array());
    assert_eq!(json["swaps"].as_array().unwrap().len(), 0);
    assert_eq!(json["pagination"]["has_more"].as_bool().unwrap(), false);
    assert!(json["pagination"]["next_cursor"].is_null());
}

#[tokio::test]
#[serial]
async fn test_history_first_page_with_swaps() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (user_id, token) = create_test_user(&server, "history2@test.com", "password123").await;

    // Create 5 test swaps
    for _i in 0..5 {
        create_test_swap(&server, &user_id, "BTC", "ETH").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Get history
    let response = server
        .get("/swap/history?limit=10")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    
    let json: Value = response.json();
    assert_eq!(json["swaps"].as_array().unwrap().len(), 5);
    assert_eq!(json["pagination"]["limit"].as_u64().unwrap(), 10);
    assert_eq!(json["pagination"]["has_more"].as_bool().unwrap(), false);
}

#[tokio::test]
#[serial]
async fn test_history_keyset_pagination() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (user_id, token) = create_test_user(&server, "history3@test.com", "password123").await;

    // Create 25 test swaps
    for _i in 0..25 {
        create_test_swap(&server, &user_id, "BTC", "ETH").await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Get first page (limit 10)
    let response = server
        .get("/swap/history?limit=10")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    
    let json: Value = response.json();
    assert_eq!(json["swaps"].as_array().unwrap().len(), 10);
    assert_eq!(json["pagination"]["has_more"].as_bool().unwrap(), true);
    assert!(json["pagination"]["next_cursor"].is_string());

    let cursor = json["pagination"]["next_cursor"].as_str().unwrap();

    // Get second page using cursor
    let response2 = server
        .get(&format!("/swap/history?limit=10&cursor={}", cursor))
        .authorization_bearer(&token)
        .await;

    assert_eq!(response2.status_code(), 200);
    
    let json2: Value = response2.json();
    assert_eq!(json2["swaps"].as_array().unwrap().len(), 10);
    assert_eq!(json2["pagination"]["has_more"].as_bool().unwrap(), true);

    // Get third page
    let cursor2 = json2["pagination"]["next_cursor"].as_str().unwrap();
    let response3 = server
        .get(&format!("/swap/history?limit=10&cursor={}", cursor2))
        .authorization_bearer(&token)
        .await;

    assert_eq!(response3.status_code(), 200);
    
    let json3: Value = response3.json();
    assert_eq!(json3["swaps"].as_array().unwrap().len(), 5); // Remaining swaps
    assert_eq!(json3["pagination"]["has_more"].as_bool().unwrap(), false);
    assert!(json3["pagination"]["next_cursor"].is_null());
}

#[tokio::test]
#[serial]
async fn test_history_filter_by_status() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (user_id, token) = create_test_user(&server, "history4@test.com", "password123").await;

    // Create swaps
    create_test_swap(&server, &user_id, "BTC", "ETH").await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    create_test_swap(&server, &user_id, "ETH", "USDT").await;

    // Get history filtered by status
    let response = server
        .get("/swap/history?status=waiting")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    
    let json: Value = response.json();
    assert!(json["swaps"].is_array());
    assert!(json["filters_applied"]["status"].as_str().unwrap() == "waiting");
}

#[tokio::test]
#[serial]
async fn test_history_filter_by_currency() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (user_id, token) = create_test_user(&server, "history5@test.com", "password123").await;

    // Create swaps with different currencies
    create_test_swap(&server, &user_id, "BTC", "ETH").await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    create_test_swap(&server, &user_id, "ETH", "USDT").await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    create_test_swap(&server, &user_id, "BTC", "USDT").await;

    // Get history filtered by from_currency
    let response = server
        .get("/swap/history?from_currency=BTC")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    
    let json: Value = response.json();
    let swaps = json["swaps"].as_array().unwrap();
    
    // All swaps should have BTC as from_currency
    for swap in swaps {
        assert_eq!(swap["from_currency"].as_str().unwrap(), "BTC");
    }
}

#[tokio::test]
#[serial]
async fn test_history_invalid_cursor() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (_user_id, token) = create_test_user(&server, "history6@test.com", "password123").await;

    // Try with invalid cursor
    let response = server
        .get("/swap/history?cursor=invalid_base64_!@#")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 400, "Should reject invalid cursor");
}

#[tokio::test]
#[serial]
async fn test_history_limit_validation() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create user and login
    let (_user_id, token) = create_test_user(&server, "history7@test.com", "password123").await;

    // Test max limit (should cap at 100)
    let response = server
        .get("/swap/history?limit=500")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response.status_code(), 200);
    let json: Value = response.json();
    assert!(json["pagination"]["limit"].as_u64().unwrap() <= 100);

    // Test min limit (should default to 1)
    let response2 = server
        .get("/swap/history?limit=0")
        .authorization_bearer(&token)
        .await;

    assert_eq!(response2.status_code(), 200);
    let json2: Value = response2.json();
    assert!(json2["pagination"]["limit"].as_u64().unwrap() >= 1);
}

#[tokio::test]
#[serial]
async fn test_history_no_cross_user_access() {
    let app = setup_test_app().await;
    let server = TestServer::new(app).unwrap();

    // Create two users
    let (user1_id, token1) = create_test_user(&server, "user1@test.com", "password123").await;
    let (user2_id, token2) = create_test_user(&server, "user2@test.com", "password123").await;

    // Create swaps for user1
    create_test_swap(&server, &user1_id, "BTC", "ETH").await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    create_test_swap(&server, &user1_id, "ETH", "USDT").await;

    // Create swaps for user2
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    create_test_swap(&server, &user2_id, "BTC", "USDT").await;

    // User1 should only see their swaps
    let response1 = server
        .get("/swap/history")
        .authorization_bearer(&token1)
        .await;

    let json1: Value = response1.json();
    assert_eq!(json1["swaps"].as_array().unwrap().len(), 2);

    // User2 should only see their swaps
    let response2 = server
        .get("/swap/history")
        .authorization_bearer(&token2)
        .await;

    let json2: Value = response2.json();
    assert_eq!(json2["swaps"].as_array().unwrap().len(), 1);
}
