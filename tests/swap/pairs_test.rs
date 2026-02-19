use serial_test::serial;
use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};

// =============================================================================
// INTEGRATION TESTS - PAIRS ENDPOINT
// =============================================================================

#[serial]
#[tokio::test]
async fn test_get_pairs_basic() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    
    // Check response structure
    assert!(body.get("pairs").is_some(), "Missing 'pairs' field");
    assert!(body.get("pagination").is_some(), "Missing 'pagination' field");
    
    let pairs = body["pairs"].as_array().unwrap();
    let pagination = &body["pagination"];
    
    // Validate pagination structure
    assert!(pagination.get("page").is_some());
    assert!(pagination.get("size").is_some());
    assert!(pagination.get("total_elements").is_some());
    assert!(pagination.get("total_pages").is_some());
    assert!(pagination.get("has_next").is_some());
    assert!(pagination.get("has_prev").is_some());
    
    assert_eq!(pagination["page"].as_u64().unwrap(), 0);
    assert_eq!(pagination["size"].as_u64().unwrap(), 20);
}

#[serial]
#[tokio::test]
async fn test_get_pairs_with_pagination() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?page=0&size=10").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    let pagination = &body["pagination"];
    
    assert!(pairs.len() <= 10, "Expected at most 10 pairs, got {}", pairs.len());
    assert_eq!(pagination["page"].as_u64().unwrap(), 0);
    assert_eq!(pagination["size"].as_u64().unwrap(), 10);
}

#[serial]
#[tokio::test]
async fn test_get_pairs_filter_by_base_currency() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?base_currency=BTC&size=5").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    
    for pair in pairs {
        assert_eq!(
            pair["base_currency"].as_str().unwrap(),
            "BTC",
            "Expected base_currency to be BTC"
        );
    }
}

#[serial]
#[tokio::test]
async fn test_get_pairs_filter_by_status() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?status=active&size=5").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    
    for pair in pairs {
        assert_eq!(
            pair["status"].as_str().unwrap(),
            "active",
            "Expected status to be active"
        );
    }
}

#[serial]
#[tokio::test]
async fn test_get_pairs_sorting() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?order_by=name%20asc&size=10").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    
    if pairs.len() > 1 {
        // Check if sorted alphabetically by name
        for i in 0..pairs.len() - 1 {
            let name1 = pairs[i]["name"].as_str().unwrap();
            let name2 = pairs[i + 1]["name"].as_str().unwrap();
            assert!(
                name1 <= name2,
                "Pairs not sorted: {} should come before {}",
                name1,
                name2
            );
        }
    }
}

#[serial]
#[tokio::test]
async fn test_get_pairs_multiple_filters() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?base_currency=BTC&status=active&size=5").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    
    assert!(pairs.len() <= 5);
    
    for pair in pairs {
        assert_eq!(pair["base_currency"].as_str().unwrap(), "BTC");
        assert_eq!(pair["status"].as_str().unwrap(), "active");
    }
}

#[serial]
#[tokio::test]
async fn test_pairs_response_structure() {
    let server = setup_test_server().await;
    
    let response = timed_get(&server, "/swap/pairs?size=1").await;
    
    response.assert_status_ok();
    
    let body: Value = response.json();
    let pairs = body["pairs"].as_array().unwrap();
    
    // Check pagination structure
    let pagination = &body["pagination"];
    assert!(pagination["total_elements"].as_i64().unwrap() >= 0);
    assert!(pagination["total_pages"].as_u64().unwrap() >= 0);
    
    // Check pair structure if any pairs exist
    if !pairs.is_empty() {
        let pair = &pairs[0];
        assert!(pair.get("name").is_some(), "Missing 'name' field");
        assert!(pair.get("base_currency").is_some(), "Missing 'base_currency' field");
        assert!(pair.get("quote_currency").is_some(), "Missing 'quote_currency' field");
        assert!(pair.get("base_network").is_some(), "Missing 'base_network' field");
        assert!(pair.get("quote_network").is_some(), "Missing 'quote_network' field");
        assert!(pair.get("status").is_some(), "Missing 'status' field");
        assert!(pair.get("last_updated").is_some(), "Missing 'last_updated' field");
        
        // Validate data types
        assert!(pair["name"].is_string());
        assert!(pair["base_currency"].is_string());
        assert!(pair["quote_currency"].is_string());
        assert!(pair["base_network"].is_string());
        assert!(pair["quote_network"].is_string());
        assert!(pair["status"].is_string());
        
        let status = pair["status"].as_str().unwrap();
        assert!(status == "active" || status == "disabled");
    }
}

#[serial]
#[tokio::test]
async fn test_pairs_pagination_navigation() {
    let server = setup_test_server().await;
    
    // Get first page
    let response1 = timed_get(&server, "/swap/pairs?page=0&size=5").await;
    response1.assert_status_ok();
    let body1: Value = response1.json();
    let pagination1 = &body1["pagination"];
    
    // Get second page
    let response2 = timed_get(&server, "/swap/pairs?page=1&size=5").await;
    response2.assert_status_ok();
    let body2: Value = response2.json();
    let pagination2 = &body2["pagination"];
    
    // Verify pagination flags
    let total_elements = pagination1["total_elements"].as_i64().unwrap();
    
    if total_elements > 5 {
        assert_eq!(pagination1["has_next"].as_bool().unwrap(), true);
        assert_eq!(pagination1["has_prev"].as_bool().unwrap(), false);
        
        if total_elements > 10 {
            assert_eq!(pagination2["has_prev"].as_bool().unwrap(), true);
        }
    }
}


