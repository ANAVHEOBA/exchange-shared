use exchange_shared::services::rpc::{
    config::{RpcConfig, RpcEndpoint, LoadBalancingStrategy, CircuitBreakerConfig, RpcAuth},
    manager::RpcManager,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use serial_test::serial;

// =============================================================================
// INTEGRATION TESTS - RPC MANAGER
// =============================================================================

fn create_test_endpoint(url: &str, priority: u8, weight: u32) -> RpcEndpoint {
    RpcEndpoint {
        url: url.to_string(),
        priority,
        weight,
        max_requests_per_second: None,
        timeout_ms: 5000,
        auth: None,
    }
}

fn create_test_config(chain: &str, endpoints: Vec<RpcEndpoint>, strategy: LoadBalancingStrategy) -> RpcConfig {
    RpcConfig {
        chain: chain.to_string(),
        endpoints,
        strategy,
        health_check_interval: 30,
        circuit_breaker_config: CircuitBreakerConfig::default(),
    }
}

// =============================================================================
// BASIC FUNCTIONALITY TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_rpc_manager_initialization() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "ethereum",
        vec![create_test_endpoint("https://eth-mainnet.public.blastapi.io", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("ethereum".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Should be able to select endpoint
    let endpoint = manager.select_endpoint("ethereum").await;
    assert!(endpoint.is_ok(), "Failed to select endpoint");
    assert_eq!(endpoint.unwrap(), "https://eth-mainnet.public.blastapi.io");
}

#[serial]
#[tokio::test]
async fn test_multiple_chains_configuration() {
    let mut configs = HashMap::new();
    
    configs.insert(
        "ethereum".to_string(),
        create_test_config(
            "ethereum",
            vec![create_test_endpoint("https://eth.example.com", 1, 100)],
            LoadBalancingStrategy::HealthScoreBased,
        ),
    );
    
    configs.insert(
        "polygon".to_string(),
        create_test_config(
            "polygon",
            vec![create_test_endpoint("https://polygon.example.com", 1, 100)],
            LoadBalancingStrategy::RoundRobin,
        ),
    );
    
    configs.insert(
        "bsc".to_string(),
        create_test_config(
            "bsc",
            vec![create_test_endpoint("https://bsc.example.com", 1, 100)],
            LoadBalancingStrategy::LeastLatency,
        ),
    );
    
    let manager = RpcManager::new(configs);
    
    // Should be able to select endpoints for all chains
    assert!(manager.select_endpoint("ethereum").await.is_ok());
    assert!(manager.select_endpoint("polygon").await.is_ok());
    assert!(manager.select_endpoint("bsc").await.is_ok());
}

#[serial]
#[tokio::test]
async fn test_chain_not_configured() {
    let configs = HashMap::new();
    let manager = RpcManager::new(configs);
    
    let result = manager.select_endpoint("nonexistent").await;
    assert!(result.is_err(), "Should fail for non-configured chain");
    
    match result {
        Err(e) => assert!(e.to_string().contains("Chain not configured")),
        Ok(_) => panic!("Expected error for non-configured chain"),
    }
}

#[serial]
#[tokio::test]
async fn test_empty_endpoints_list() {
    let mut configs = HashMap::new();
    
    let config = create_test_config("test", vec![], LoadBalancingStrategy::RoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let result = manager.select_endpoint("test").await;
    assert!(result.is_err(), "Should fail with empty endpoints");
}

// =============================================================================
// LOAD BALANCING STRATEGY TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_round_robin_strategy() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://endpoint1.example.com", 1, 100),
        create_test_endpoint("https://endpoint2.example.com", 1, 100),
        create_test_endpoint("https://endpoint3.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::RoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Select multiple times and verify round-robin behavior
    let mut selections = Vec::new();
    for _ in 0..6 {
        let endpoint = manager.select_endpoint("test").await.unwrap();
        selections.push(endpoint);
    }
    
    // Should cycle through endpoints
    assert_eq!(selections[0], selections[3]);
    assert_eq!(selections[1], selections[4]);
    assert_eq!(selections[2], selections[5]);
    
    // All three endpoints should be different
    assert_ne!(selections[0], selections[1]);
    assert_ne!(selections[1], selections[2]);
    assert_ne!(selections[0], selections[2]);
}

#[serial]
#[tokio::test]
async fn test_weighted_round_robin_strategy() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://high-weight.example.com", 1, 300),
        create_test_endpoint("https://low-weight.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::WeightedRoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Select multiple times and count
    let mut selections = HashMap::new();
    for _ in 0..40 {
        let endpoint = manager.select_endpoint("test").await.unwrap();
        *selections.entry(endpoint).or_insert(0) += 1;
    }
    
    let high_count = *selections.get("https://high-weight.example.com").unwrap_or(&0);
    let low_count = *selections.get("https://low-weight.example.com").unwrap_or(&0);
    
    // High weight endpoint should be selected ~3x more often
    assert!(high_count > low_count, "High weight endpoint should be selected more: high={}, low={}", high_count, low_count);
    
    // Rough ratio check (should be around 3:1)
    let ratio = high_count as f64 / low_count as f64;
    assert!(ratio > 2.0 && ratio < 4.0, "Weight ratio should be around 3:1, got {:.2}", ratio);
}

#[serial]
#[tokio::test]
async fn test_health_score_based_strategy() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://endpoint1.example.com", 1, 100),
        create_test_endpoint("https://endpoint2.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::HealthScoreBased);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Initially, all endpoints have same health score
    let endpoint1 = manager.select_endpoint("test").await.unwrap();
    
    // Record some failures for endpoint1
    for _ in 0..5 {
        manager.record_result(&endpoint1, Duration::from_millis(100), false, None).await;
    }
    
    // Record successes for endpoint2
    let endpoint2 = if endpoint1 == "https://endpoint1.example.com" {
        "https://endpoint2.example.com"
    } else {
        "https://endpoint1.example.com"
    };
    
    for _ in 0..5 {
        manager.record_result(endpoint2, Duration::from_millis(50), true, Some(1000)).await;
    }
    
    // Now endpoint2 should be preferred
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, endpoint2, "Should select healthier endpoint");
}

#[serial]
#[tokio::test]
async fn test_least_latency_strategy() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://fast.example.com", 1, 100),
        create_test_endpoint("https://slow.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::LeastLatency);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record fast latencies for first endpoint
    for _ in 0..10 {
        manager.record_result("https://fast.example.com", Duration::from_millis(50), true, None).await;
    }
    
    // Record slow latencies for second endpoint
    for _ in 0..10 {
        manager.record_result("https://slow.example.com", Duration::from_millis(500), true, None).await;
    }
    
    // Should select the faster endpoint
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, "https://fast.example.com", "Should select endpoint with lowest latency");
}

// =============================================================================
// PRIORITY TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_priority_ordering() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://low-priority.example.com", 5, 100),
        create_test_endpoint("https://high-priority.example.com", 1, 100),
        create_test_endpoint("https://medium-priority.example.com", 3, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::HealthScoreBased);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // High priority (lower number) should be preferred
    let endpoint = manager.select_endpoint("test").await.unwrap();
    assert_eq!(endpoint, "https://high-priority.example.com", "Should select highest priority endpoint");
}

#[serial]
#[tokio::test]
async fn test_priority_with_round_robin() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://priority1-a.example.com", 1, 100),
        create_test_endpoint("https://priority1-b.example.com", 1, 100),
        create_test_endpoint("https://priority2.example.com", 2, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::RoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Should only round-robin between priority 1 endpoints
    let mut selections = Vec::new();
    for _ in 0..10 {
        let endpoint = manager.select_endpoint("test").await.unwrap();
        selections.push(endpoint);
    }
    
    // Should never select priority 2 endpoint
    assert!(!selections.contains(&"https://priority2.example.com".to_string()));
    
    // Should alternate between priority 1 endpoints
    assert!(selections.contains(&"https://priority1-a.example.com".to_string()));
    assert!(selections.contains(&"https://priority1-b.example.com".to_string()));
}

// =============================================================================
// HEALTH TRACKING TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_health_status_retrieval() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "ethereum",
        vec![create_test_endpoint("https://eth.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("ethereum".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let health_status = manager.get_health_status("ethereum").await;
    assert_eq!(health_status.len(), 1, "Should have one endpoint");
    assert!(health_status[0].is_healthy, "Endpoint should be healthy initially");
    assert_eq!(health_status[0].url, "https://eth.example.com");
    assert_eq!(health_status[0].total_requests, 0);
    assert_eq!(health_status[0].success_rate, 1.0);
}

#[serial]
#[tokio::test]
async fn test_health_status_after_requests() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://test.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record some successful requests
    for _ in 0..10 {
        manager.record_result("https://test.example.com", Duration::from_millis(100), true, Some(1000)).await;
    }
    
    let health_status = manager.get_health_status("test").await;
    assert_eq!(health_status[0].total_requests, 10);
    assert_eq!(health_status[0].success_rate, 1.0);
    assert!(health_status[0].average_latency_ms > 0.0);
    assert_eq!(health_status[0].last_block_height, Some(1000));
}

#[serial]
#[tokio::test]
async fn test_health_status_with_failures() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://test.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record mixed results: 7 successes, 3 failures
    for _ in 0..7 {
        manager.record_result("https://test.example.com", Duration::from_millis(100), true, None).await;
    }
    for _ in 0..3 {
        manager.record_result("https://test.example.com", Duration::from_millis(200), false, None).await;
    }
    
    let health_status = manager.get_health_status("test").await;
    assert_eq!(health_status[0].total_requests, 10);
    assert_eq!(health_status[0].success_rate, 0.7);
}

#[serial]
#[tokio::test]
async fn test_health_status_multiple_endpoints() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://endpoint1.example.com", 1, 100),
        create_test_endpoint("https://endpoint2.example.com", 1, 100),
        create_test_endpoint("https://endpoint3.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::RoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let health_status = manager.get_health_status("test").await;
    assert_eq!(health_status.len(), 3, "Should have three endpoints");
    
    for status in &health_status {
        assert!(status.is_healthy);
        assert_eq!(status.total_requests, 0);
    }
}

// =============================================================================
// CIRCUIT BREAKER TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_circuit_breaker_opens_on_failures() {
    let mut configs = HashMap::new();
    
    let mut cb_config = CircuitBreakerConfig::default();
    cb_config.failure_threshold = 0.5; // 50% failure rate
    cb_config.min_requests = 5;
    
    let config = RpcConfig {
        chain: "test".to_string(),
        endpoints: vec![
            create_test_endpoint("https://failing.example.com", 1, 100),
            create_test_endpoint("https://backup.example.com", 2, 100),
        ],
        strategy: LoadBalancingStrategy::HealthScoreBased,
        health_check_interval: 30,
        circuit_breaker_config: cb_config,
    };
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record failures to open circuit breaker
    for _ in 0..10 {
        manager.record_result("https://failing.example.com", Duration::from_millis(100), false, None).await;
    }
    
    // Circuit should be open, so backup should be selected
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, "https://backup.example.com", "Should failover to backup endpoint");
    
    let health_status = manager.get_health_status("test").await;
    let failing_status = health_status.iter().find(|s| s.url == "https://failing.example.com").unwrap();
    assert_eq!(failing_status.state, "Open", "Circuit breaker should be open");
}

#[serial]
#[tokio::test]
async fn test_no_healthy_endpoints_error() {
    let mut configs = HashMap::new();
    
    let mut cb_config = CircuitBreakerConfig::default();
    cb_config.failure_threshold = 0.5;
    cb_config.min_requests = 3;
    
    let config = RpcConfig {
        chain: "test".to_string(),
        endpoints: vec![create_test_endpoint("https://only.example.com", 1, 100)],
        strategy: LoadBalancingStrategy::HealthScoreBased,
        health_check_interval: 30,
        circuit_breaker_config: cb_config,
    };
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record failures to open circuit breaker
    for _ in 0..10 {
        manager.record_result("https://only.example.com", Duration::from_millis(100), false, None).await;
    }
    
    // Should fail with no healthy endpoints
    let result = manager.select_endpoint("test").await;
    assert!(result.is_err(), "Should fail when no healthy endpoints available");
}

// =============================================================================
// AUTHENTICATION TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_endpoint_with_api_key_auth() {
    let mut configs = HashMap::new();
    
    let mut endpoint = create_test_endpoint("https://auth.example.com", 1, 100);
    endpoint.auth = Some(RpcAuth::ApiKey {
        key: "test-api-key-123".to_string(),
    });
    
    let config = create_test_config("test", vec![endpoint], LoadBalancingStrategy::HealthScoreBased);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, "https://auth.example.com");
}

#[serial]
#[tokio::test]
async fn test_endpoint_with_bearer_auth() {
    let mut configs = HashMap::new();
    
    let mut endpoint = create_test_endpoint("https://bearer.example.com", 1, 100);
    endpoint.auth = Some(RpcAuth::Bearer {
        token: "bearer-token-xyz".to_string(),
    });
    
    let config = create_test_config("test", vec![endpoint], LoadBalancingStrategy::HealthScoreBased);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, "https://bearer.example.com");
}

#[serial]
#[tokio::test]
async fn test_endpoint_with_basic_auth() {
    let mut configs = HashMap::new();
    
    let mut endpoint = create_test_endpoint("https://basic.example.com", 1, 100);
    endpoint.auth = Some(RpcAuth::Basic {
        username: "user".to_string(),
        password: "pass".to_string(),
    });
    
    let config = create_test_config("test", vec![endpoint], LoadBalancingStrategy::HealthScoreBased);
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    let selected = manager.select_endpoint("test").await.unwrap();
    assert_eq!(selected, "https://basic.example.com");
}

// =============================================================================
// LATENCY TRACKING TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_latency_tracking() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://test.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record requests with varying latencies
    let latencies = vec![50, 100, 150, 200, 250];
    for lat in &latencies {
        manager.record_result("https://test.example.com", Duration::from_millis(*lat), true, None).await;
    }
    
    let health_status = manager.get_health_status("test").await;
    let avg_latency = health_status[0].average_latency_ms;
    
    // Average should be 150ms
    assert!(avg_latency > 140.0 && avg_latency < 160.0, "Average latency should be around 150ms, got {}", avg_latency);
}

#[serial]
#[tokio::test]
async fn test_p95_latency_calculation() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://test.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record 100 requests: 95 fast, 5 slow
    for _ in 0..95 {
        manager.record_result("https://test.example.com", Duration::from_millis(100), true, None).await;
    }
    for _ in 0..5 {
        manager.record_result("https://test.example.com", Duration::from_millis(1000), true, None).await;
    }
    
    let health_status = manager.get_health_status("test").await;
    let p95 = health_status[0].p95_latency_ms.unwrap();
    
    // P95 should be around 100-1000ms range
    assert!(p95 >= 100 && p95 <= 1000, "P95 latency should be in range, got {}", p95);
}

// =============================================================================
// BLOCK HEIGHT TRACKING TESTS
// =============================================================================

#[serial]
#[tokio::test]
async fn test_block_height_tracking() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://test.example.com", 1, 100)],
        LoadBalancingStrategy::HealthScoreBased,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Record requests with increasing block heights
    for height in 1000..1010 {
        manager.record_result("https://test.example.com", Duration::from_millis(100), true, Some(height)).await;
    }
    
    let health_status = manager.get_health_status("test").await;
    assert_eq!(health_status[0].last_block_height, Some(1009), "Should track latest block height");
}

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

#[serial]
#[tokio::test]
async fn test_single_endpoint_selection() {
    let mut configs = HashMap::new();
    
    let config = create_test_config(
        "test",
        vec![create_test_endpoint("https://only.example.com", 1, 100)],
        LoadBalancingStrategy::RoundRobin,
    );
    
    configs.insert("test".to_string(), config);
    
    let manager = RpcManager::new(configs);
    
    // Should always return the same endpoint
    for _ in 0..10 {
        let endpoint = manager.select_endpoint("test").await.unwrap();
        assert_eq!(endpoint, "https://only.example.com");
    }
}

#[serial]
#[tokio::test]
async fn test_health_status_for_nonexistent_chain() {
    let configs = HashMap::new();
    let manager = RpcManager::new(configs);
    
    let health_status = manager.get_health_status("nonexistent").await;
    assert_eq!(health_status.len(), 0, "Should return empty vec for non-existent chain");
}

#[serial]
#[tokio::test]
async fn test_concurrent_endpoint_selection() {
    let mut configs = HashMap::new();
    
    let endpoints = vec![
        create_test_endpoint("https://endpoint1.example.com", 1, 100),
        create_test_endpoint("https://endpoint2.example.com", 1, 100),
    ];
    
    let config = create_test_config("test", endpoints, LoadBalancingStrategy::RoundRobin);
    configs.insert("test".to_string(), config);
    
    let manager = Arc::new(RpcManager::new(configs));
    
    // Spawn multiple concurrent tasks
    let mut handles = vec![];
    for _ in 0..10 {
        let manager_clone = Arc::clone(&manager);
        let handle = tokio::spawn(async move {
            manager_clone.select_endpoint("test").await
        });
        handles.push(handle);
    }
    
    // All should succeed
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent selection should succeed");
    }
}
