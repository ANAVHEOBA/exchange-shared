use std::sync::Arc;
use std::time::Instant;

use super::MetricsRegistry;

/// Collector for swap metrics
pub struct SwapMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl SwapMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn record_swap_initiated(&self, base_currency: &str, quote_currency: &str, provider: &str) {
        self.metrics
            .swap_initiated_total
            .with_label_values(&[base_currency, quote_currency, provider])
            .inc();
    }
    
    pub fn record_swap_completed(
        &self,
        base_currency: &str,
        quote_currency: &str,
        provider: &str,
        duration_secs: f64,
        amount_usd: f64,
    ) {
        self.metrics
            .swap_completed_total
            .with_label_values(&[base_currency, quote_currency, provider])
            .inc();
        
        self.metrics
            .swap_processing_duration_seconds
            .with_label_values(&[base_currency, quote_currency, provider])
            .observe(duration_secs);
        
        self.metrics
            .swap_amount_usd
            .with_label_values(&[base_currency, quote_currency])
            .observe(amount_usd);
    }
    
    pub fn record_swap_failed(
        &self,
        base_currency: &str,
        quote_currency: &str,
        provider: &str,
        reason: &str,
    ) {
        self.metrics
            .swap_failed_total
            .with_label_values(&[base_currency, quote_currency, provider, reason])
            .inc();
    }
    
    pub fn set_active_swaps(&self, status: &str, count: i64) {
        self.metrics
            .swap_active_count
            .with_label_values(&[status])
            .set(count as f64);
    }
}

/// Collector for payout metrics
pub struct PayoutMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl PayoutMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn record_payout_initiated(&self, chain: &str, currency: &str) {
        self.metrics
            .payout_initiated_total
            .with_label_values(&[chain, currency])
            .inc();
    }
    
    pub fn record_payout_completed(
        &self,
        chain: &str,
        currency: &str,
        duration_secs: f64,
        gas_cost: f64,
    ) {
        self.metrics
            .payout_completed_total
            .with_label_values(&[chain, currency])
            .inc();
        
        self.metrics
            .payout_duration_seconds
            .with_label_values(&[chain, currency])
            .observe(duration_secs);
        
        self.metrics
            .payout_gas_cost
            .with_label_values(&[chain])
            .observe(gas_cost);
    }
    
    pub fn record_payout_failed(&self, chain: &str, currency: &str, reason: &str) {
        self.metrics
            .payout_failed_total
            .with_label_values(&[chain, currency, reason])
            .inc();
    }
}

/// Collector for RPC metrics
pub struct RpcMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl RpcMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn set_health_score(&self, chain: &str, endpoint: &str, score: f64) {
        self.metrics
            .rpc_endpoint_health_score
            .with_label_values(&[chain, endpoint])
            .set(score);
    }
    
    pub fn record_rpc_request(
        &self,
        chain: &str,
        endpoint: &str,
        method: &str,
        status: &str,
        duration_secs: f64,
    ) {
        self.metrics
            .rpc_requests_total
            .with_label_values(&[chain, endpoint, method, status])
            .inc();
        
        self.metrics
            .rpc_request_duration_seconds
            .with_label_values(&[chain, endpoint, method])
            .observe(duration_secs);
    }
    
    pub fn set_circuit_breaker_state(&self, chain: &str, endpoint: &str, state: f64) {
        self.metrics
            .rpc_circuit_breaker_state
            .with_label_values(&[chain, endpoint])
            .set(state);
    }
    
    pub fn set_block_height_lag(&self, chain: &str, endpoint: &str, lag: i64) {
        self.metrics
            .rpc_block_height_lag
            .with_label_values(&[chain, endpoint])
            .set(lag as f64);
    }
}

/// Collector for cache metrics
pub struct CacheMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl CacheMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn record_cache_operation(
        &self,
        cache: &str,
        operation: &str,
        result: &str,
        duration_secs: f64,
    ) {
        self.metrics
            .cache_operations_total
            .with_label_values(&[cache, operation, result])
            .inc();
        
        self.metrics
            .cache_operation_duration_seconds
            .with_label_values(&[cache, operation])
            .observe(duration_secs);
    }
    
    pub fn set_hit_ratio(&self, cache: &str, key_pattern: &str, ratio: f64) {
        self.metrics
            .cache_hit_ratio
            .with_label_values(&[cache, key_pattern])
            .set(ratio);
    }
    
    pub fn set_cache_size(&self, cache: &str, size_bytes: i64) {
        self.metrics
            .cache_size_bytes
            .with_label_values(&[cache])
            .set(size_bytes as f64);
    }
    
    pub fn set_cache_entries(&self, cache: &str, count: i64) {
        self.metrics
            .cache_entries_total
            .with_label_values(&[cache])
            .set(count as f64);
    }
}

/// Collector for database metrics
pub struct DatabaseMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl DatabaseMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn record_query(
        &self,
        operation: &str,
        table: &str,
        status: &str,
        duration_secs: f64,
    ) {
        self.metrics
            .db_queries_total
            .with_label_values(&[operation, table, status])
            .inc();
        
        self.metrics
            .db_query_duration_seconds
            .with_label_values(&[operation, table])
            .observe(duration_secs);
    }
    
    pub fn set_connection_stats(&self, active: u32, idle: u32, max: u32) {
        self.metrics.db_connections_active.set(active as f64);
        self.metrics.db_connections_idle.set(idle as f64);
        self.metrics.db_connections_max.set(max as f64);
    }
}

/// Collector for business metrics
pub struct BusinessMetricsCollector {
    metrics: Arc<MetricsRegistry>,
}

impl BusinessMetricsCollector {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics }
    }
    
    pub fn record_revenue(&self, currency: &str, fee_type: &str, amount_usd: f64) {
        self.metrics
            .revenue_total_usd
            .with_label_values(&[currency, fee_type])
            .inc_by(amount_usd);
    }
    
    pub fn set_tvl(&self, currency: &str, amount_usd: f64) {
        self.metrics
            .tvl_usd
            .with_label_values(&[currency])
            .set(amount_usd);
    }
    
    pub fn record_user_swap(&self, user_tier: &str) {
        self.metrics
            .user_swaps_total
            .with_label_values(&[user_tier])
            .inc();
    }
    
    pub fn record_commission(&self, currency_pair: &str, amount_usd: f64) {
        self.metrics
            .commission_per_swap_usd
            .with_label_values(&[currency_pair])
            .observe(amount_usd);
    }
}

/// Timer helper for measuring durations
pub struct MetricsTimer {
    start: Instant,
}

impl MetricsTimer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
    
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

impl Default for MetricsTimer {
    fn default() -> Self {
        Self::new()
    }
}
