use exchange_shared::services::metrics::MetricsRegistry;
use serial_test::serial;

// =============================================================================
// INTEGRATION TESTS - METRICS REGISTRY
// =============================================================================

#[serial]
#[test]
fn test_metrics_registry_initialization() {
    let metrics = MetricsRegistry::new();
    assert!(metrics.is_ok(), "Failed to initialize metrics registry");
}

#[serial]
#[test]
fn test_http_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    // Record HTTP request
    metrics
        .http_requests_total
        .with_label_values(&["GET", "/api/swap", "200"])
        .inc();
    
    // Export and verify
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_http_requests_total"));
    assert!(output.contains("method=\"GET\""));
    assert!(output.contains("endpoint=\"/api/swap\""));
    assert!(output.contains("status=\"200\""));
}

#[serial]
#[test]
fn test_swap_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    // Record swap initiated
    metrics
        .swap_initiated_total
        .with_label_values(&["BTC", "ETH", "trocador"])
        .inc();
    
    // Record swap completed
    metrics
        .swap_completed_total
        .with_label_values(&["BTC", "ETH", "trocador"])
        .inc();
    
    // Record processing duration
    metrics
        .swap_processing_duration_seconds
        .with_label_values(&["BTC", "ETH", "trocador"])
        .observe(45.5);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_initiated_total"));
    assert!(output.contains("exchange_swap_completed_total"));
    assert!(output.contains("exchange_swap_processing_duration_seconds"));
}

#[serial]
#[test]
fn test_payout_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .payout_initiated_total
        .with_label_values(&["ethereum", "ETH"])
        .inc();
    
    metrics
        .payout_completed_total
        .with_label_values(&["ethereum", "ETH"])
        .inc();
    
    metrics
        .payout_duration_seconds
        .with_label_values(&["ethereum", "ETH"])
        .observe(12.3);
    
    metrics
        .payout_gas_cost
        .with_label_values(&["ethereum"])
        .observe(0.005);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_payout_initiated_total"));
    assert!(output.contains("exchange_payout_completed_total"));
    assert!(output.contains("chain=\"ethereum\""));
}

#[serial]
#[test]
fn test_rpc_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .rpc_endpoint_health_score
        .with_label_values(&["ethereum", "primary"])
        .set(0.95);
    
    metrics
        .rpc_requests_total
        .with_label_values(&["ethereum", "primary", "eth_blockNumber", "success"])
        .inc();
    
    metrics
        .rpc_request_duration_seconds
        .with_label_values(&["ethereum", "primary", "eth_blockNumber"])
        .observe(0.123);
    
    metrics
        .rpc_circuit_breaker_state
        .with_label_values(&["ethereum", "primary"])
        .set(0.0); // Closed
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_rpc_endpoint_health_score"));
    assert!(output.contains("exchange_rpc_requests_total"));
    assert!(output.contains("exchange_rpc_circuit_breaker_state"));
}

#[serial]
#[test]
fn test_cache_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .cache_operations_total
        .with_label_values(&["redis", "get", "hit"])
        .inc();
    
    metrics
        .cache_hit_ratio
        .with_label_values(&["redis", "rate:*"])
        .set(0.85);
    
    metrics
        .cache_size_bytes
        .with_label_values(&["redis"])
        .set(1024000.0);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_cache_operations_total"));
    assert!(output.contains("exchange_cache_hit_ratio"));
}

#[serial]
#[test]
fn test_database_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .db_queries_total
        .with_label_values(&["select", "swaps", "success"])
        .inc();
    
    metrics
        .db_query_duration_seconds
        .with_label_values(&["select", "swaps"])
        .observe(0.015);
    
    metrics.db_connections_active.set(5.0);
    metrics.db_connections_idle.set(10.0);
    metrics.db_connections_max.set(20.0);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_db_queries_total"));
    assert!(output.contains("exchange_db_connections_active"));
}

#[serial]
#[test]
fn test_business_metrics_recording() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .revenue_total_usd
        .with_label_values(&["BTC", "commission"])
        .inc_by(15.50);
    
    metrics
        .tvl_usd
        .with_label_values(&["BTC"])
        .set(1000000.0);
    
    metrics
        .user_swaps_total
        .with_label_values(&["premium"])
        .inc();
    
    metrics
        .commission_per_swap_usd
        .with_label_values(&["BTC_ETH"])
        .observe(5.25);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_revenue_total_usd"));
    assert!(output.contains("exchange_tvl_usd"));
    assert!(output.contains("exchange_user_swaps_total"));
}

#[serial]
#[test]
fn test_histogram_buckets() {
    let metrics = MetricsRegistry::new().unwrap();
    
    // Record multiple observations
    for duration in [0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0] {
        metrics
            .http_request_duration_seconds
            .with_label_values(&["GET", "/api/swap"])
            .observe(duration);
    }
    
    let output = metrics.export().unwrap();
    
    // Verify bucket labels exist
    assert!(output.contains("le=\"0.01\""));
    assert!(output.contains("le=\"0.1\""));
    assert!(output.contains("le=\"1\""));
    assert!(output.contains("le=\"+Inf\""));
}

#[serial]
#[test]
fn test_metrics_export_format() {
    let metrics = MetricsRegistry::new().unwrap();
    
    metrics
        .http_requests_total
        .with_label_values(&["GET", "/api/swap", "200"])
        .inc();
    
    let output = metrics.export().unwrap();
    
    // Verify Prometheus text format
    assert!(output.contains("# HELP"));
    assert!(output.contains("# TYPE"));
    assert!(output.contains("exchange_http_requests_total"));
}

#[serial]
#[test]
fn test_multiple_label_combinations() {
    let metrics = MetricsRegistry::new().unwrap();
    
    // Record different combinations
    metrics
        .swap_initiated_total
        .with_label_values(&["BTC", "ETH", "trocador"])
        .inc();
    
    metrics
        .swap_initiated_total
        .with_label_values(&["ETH", "USDT", "trocador"])
        .inc();
    
    metrics
        .swap_initiated_total
        .with_label_values(&["BTC", "USDT", "other"])
        .inc();
    
    let output = metrics.export().unwrap();
    
    // All combinations should be present
    assert!(output.contains("base_currency=\"BTC\""));
    assert!(output.contains("base_currency=\"ETH\""));
    assert!(output.contains("quote_currency=\"ETH\""));
    assert!(output.contains("quote_currency=\"USDT\""));
    assert!(output.contains("provider=\"trocador\""));
    assert!(output.contains("provider=\"other\""));
}

#[serial]
#[test]
fn test_gauge_set_operations() {
    let metrics = MetricsRegistry::new().unwrap();
    
    // Set initial value
    metrics
        .swap_active_count
        .with_label_values(&["pending"])
        .set(10.0);
    
    let output1 = metrics.export().unwrap();
    assert!(output1.contains("10"));
    
    // Update value
    metrics
        .swap_active_count
        .with_label_values(&["pending"])
        .set(15.0);
    
    let output2 = metrics.export().unwrap();
    assert!(output2.contains("15"));
}

#[serial]
#[test]
fn test_counter_increment() {
    let metrics = MetricsRegistry::new().unwrap();
    
    let counter = metrics
        .http_requests_total
        .with_label_values(&["POST", "/api/swap", "201"]);
    
    // Increment multiple times
    counter.inc();
    counter.inc();
    counter.inc();
    
    let output = metrics.export().unwrap();
    
    // Should show count of 3
    let lines: Vec<&str> = output.lines().collect();
    let metric_line = lines
        .iter()
        .find(|l| l.contains("exchange_http_requests_total") 
            && l.contains("POST") 
            && l.contains("/api/swap")
            && l.contains("201"))
        .expect("Metric not found");
    
    assert!(metric_line.contains("3"));
}
