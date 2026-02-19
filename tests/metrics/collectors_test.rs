use exchange_shared::services::metrics::{MetricsRegistry, collectors::*};
use serial_test::serial;

// =============================================================================
// INTEGRATION TESTS - METRICS COLLECTORS
// =============================================================================

#[serial]
#[test]
fn test_swap_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = SwapMetricsCollector::new(metrics.clone());
    
    // Record swap lifecycle
    collector.record_swap_initiated("BTC", "ETH", "trocador");
    collector.record_swap_completed("BTC", "ETH", "trocador", 45.5, 1000.0);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_initiated_total"));
    assert!(output.contains("exchange_swap_completed_total"));
    assert!(output.contains("exchange_swap_processing_duration_seconds"));
}

#[serial]
#[test]
fn test_swap_failure_tracking() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = SwapMetricsCollector::new(metrics.clone());
    
    collector.record_swap_failed("BTC", "ETH", "trocador", "timeout");
    collector.record_swap_failed("ETH", "USDT", "trocador", "rpc_error");
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_failed_total"));
    assert!(output.contains("reason=\"timeout\""));
    assert!(output.contains("reason=\"rpc_error\""));
}

#[serial]
#[test]
fn test_active_swaps_gauge() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = SwapMetricsCollector::new(metrics.clone());
    
    collector.set_active_swaps("pending", 5);
    collector.set_active_swaps("processing", 3);
    collector.set_active_swaps("awaiting_payout", 2);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_active_count"));
    assert!(output.contains("status=\"pending\""));
    assert!(output.contains("status=\"processing\""));
}

#[serial]
#[test]
fn test_payout_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = PayoutMetricsCollector::new(metrics.clone());
    
    collector.record_payout_initiated("ethereum", "ETH");
    collector.record_payout_completed("ethereum", "ETH", 12.3, 0.005);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_payout_initiated_total"));
    assert!(output.contains("exchange_payout_completed_total"));
    assert!(output.contains("exchange_payout_duration_seconds"));
    assert!(output.contains("exchange_payout_gas_cost"));
}

#[serial]
#[test]
fn test_payout_failure_reasons() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = PayoutMetricsCollector::new(metrics.clone());
    
    collector.record_payout_failed("ethereum", "ETH", "insufficient_gas");
    collector.record_payout_failed("bitcoin", "BTC", "rpc_error");
    collector.record_payout_failed("solana", "SOL", "invalid_address");
    
    let output = metrics.export().unwrap();
    assert!(output.contains("reason=\"insufficient_gas\""));
    assert!(output.contains("reason=\"rpc_error\""));
    assert!(output.contains("reason=\"invalid_address\""));
}

#[serial]
#[test]
fn test_rpc_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = RpcMetricsCollector::new(metrics.clone());
    
    collector.set_health_score("ethereum", "primary", 0.95);
    collector.record_rpc_request("ethereum", "primary", "eth_blockNumber", "success", 0.123);
    collector.set_circuit_breaker_state("ethereum", "primary", 0.0);
    collector.set_block_height_lag("ethereum", "primary", 2);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_rpc_endpoint_health_score"));
    assert!(output.contains("exchange_rpc_requests_total"));
    assert!(output.contains("exchange_rpc_circuit_breaker_state"));
    assert!(output.contains("exchange_rpc_block_height_lag"));
}

#[serial]
#[test]
fn test_cache_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = CacheMetricsCollector::new(metrics.clone());
    
    collector.record_cache_operation("redis", "get", "hit", 0.001);
    collector.record_cache_operation("redis", "get", "miss", 0.002);
    collector.set_hit_ratio("redis", "rate:*", 0.85);
    collector.set_cache_size("redis", 1024000);
    collector.set_cache_entries("redis", 5000);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_cache_operations_total"));
    assert!(output.contains("exchange_cache_hit_ratio"));
    assert!(output.contains("exchange_cache_size_bytes"));
    assert!(output.contains("exchange_cache_entries_total"));
}

#[serial]
#[test]
fn test_database_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = DatabaseMetricsCollector::new(metrics.clone());
    
    collector.record_query("select", "swaps", "success", 0.015);
    collector.record_query("insert", "swaps", "success", 0.008);
    collector.set_connection_stats(5, 10, 20);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_db_queries_total"));
    assert!(output.contains("exchange_db_query_duration_seconds"));
    assert!(output.contains("exchange_db_connections_active"));
    assert!(output.contains("exchange_db_connections_idle"));
    assert!(output.contains("exchange_db_connections_max"));
}

#[serial]
#[test]
fn test_business_metrics_collector() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = BusinessMetricsCollector::new(metrics.clone());
    
    collector.record_revenue("BTC", "commission", 15.50);
    collector.set_tvl("BTC", 1000000.0);
    collector.record_user_swap("premium");
    collector.record_commission("BTC_ETH", 5.25);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_revenue_total_usd"));
    assert!(output.contains("exchange_tvl_usd"));
    assert!(output.contains("exchange_user_swaps_total"));
    assert!(output.contains("exchange_commission_per_swap_usd"));
}

#[serial]
#[test]
fn test_metrics_timer() {
    let timer = MetricsTimer::new();
    
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    let elapsed = timer.elapsed_secs();
    assert!(elapsed >= 0.1, "Timer should measure at least 0.1 seconds");
    assert!(elapsed < 0.2, "Timer should measure less than 0.2 seconds");
}

#[serial]
#[test]
fn test_multiple_collectors_same_registry() {
    let metrics = MetricsRegistry::new().unwrap();
    
    let swap_collector = SwapMetricsCollector::new(metrics.clone());
    let payout_collector = PayoutMetricsCollector::new(metrics.clone());
    let rpc_collector = RpcMetricsCollector::new(metrics.clone());
    
    swap_collector.record_swap_initiated("BTC", "ETH", "trocador");
    payout_collector.record_payout_initiated("ethereum", "ETH");
    rpc_collector.set_health_score("ethereum", "primary", 0.95);
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_initiated_total"));
    assert!(output.contains("exchange_payout_initiated_total"));
    assert!(output.contains("exchange_rpc_endpoint_health_score"));
}

#[serial]
#[test]
fn test_high_volume_metrics() {
    let metrics = MetricsRegistry::new().unwrap();
    let collector = SwapMetricsCollector::new(metrics.clone());
    
    // Simulate high volume
    for _ in 0..1000 {
        collector.record_swap_initiated("BTC", "ETH", "trocador");
    }
    
    let output = metrics.export().unwrap();
    assert!(output.contains("exchange_swap_initiated_total"));
    
    // Verify count
    let lines: Vec<&str> = output.lines().collect();
    let metric_line = lines
        .iter()
        .find(|l| l.contains("exchange_swap_initiated_total") 
            && l.contains("BTC") 
            && l.contains("ETH"))
        .expect("Metric not found");
    
    assert!(metric_line.contains("1000"));
}
