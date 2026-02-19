use prometheus::{
    Registry, CounterVec, HistogramVec, HistogramOpts, Gauge, GaugeVec, Opts,
    Encoder, TextEncoder,
};
use std::sync::Arc;

/// Central metrics registry for the exchange platform
pub struct MetricsRegistry {
    registry: Registry,
    
    // HTTP Metrics
    pub http_requests_total: CounterVec,
    pub http_request_duration_seconds: HistogramVec,
    pub http_response_size_bytes: HistogramVec,
    
    // Swap Metrics
    pub swap_initiated_total: CounterVec,
    pub swap_completed_total: CounterVec,
    pub swap_failed_total: CounterVec,
    pub swap_processing_duration_seconds: HistogramVec,
    pub swap_amount_usd: HistogramVec,
    pub swap_active_count: GaugeVec,
    
    // Payout Metrics
    pub payout_initiated_total: CounterVec,
    pub payout_completed_total: CounterVec,
    pub payout_failed_total: CounterVec,
    pub payout_duration_seconds: HistogramVec,
    pub payout_gas_cost: HistogramVec,
    
    // RPC Metrics
    pub rpc_endpoint_health_score: GaugeVec,
    pub rpc_requests_total: CounterVec,
    pub rpc_request_duration_seconds: HistogramVec,
    pub rpc_circuit_breaker_state: GaugeVec,
    pub rpc_block_height_lag: GaugeVec,
    
    // Cache Metrics
    pub cache_operations_total: CounterVec,
    pub cache_hit_ratio: GaugeVec,
    pub cache_size_bytes: GaugeVec,
    pub cache_entries_total: GaugeVec,
    pub cache_operation_duration_seconds: HistogramVec,
    
    // Database Metrics
    pub db_queries_total: CounterVec,
    pub db_query_duration_seconds: HistogramVec,
    pub db_connections_active: Gauge,
    pub db_connections_idle: Gauge,
    pub db_connections_max: Gauge,
    
    // Business Metrics
    pub revenue_total_usd: CounterVec,
    pub tvl_usd: GaugeVec,
    pub user_swaps_total: CounterVec,
    pub commission_per_swap_usd: HistogramVec,
}

impl MetricsRegistry {
    pub fn new() -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let registry = Registry::new();
        
        // HTTP Metrics
        let http_requests_total = CounterVec::new(
            Opts::new("exchange_http_requests_total", "Total HTTP requests")
                .namespace("exchange"),
            &["method", "endpoint", "status"],
        )?;
        registry.register(Box::new(http_requests_total.clone()))?;
        
        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_http_request_duration_seconds", "HTTP request duration")
                .namespace("exchange")
                .buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["method", "endpoint"],
        )?;
        registry.register(Box::new(http_request_duration_seconds.clone()))?;
        
        let http_response_size_bytes = HistogramVec::new(
            HistogramOpts::new("exchange_http_response_size_bytes", "HTTP response size")
                .namespace("exchange")
                .buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0]),
            &["method", "endpoint"],
        )?;
        registry.register(Box::new(http_response_size_bytes.clone()))?;
        
        // Swap Metrics
        let swap_initiated_total = CounterVec::new(
            Opts::new("exchange_swap_initiated_total", "Total swaps initiated")
                .namespace("exchange"),
            &["base_currency", "quote_currency", "provider"],
        )?;
        registry.register(Box::new(swap_initiated_total.clone()))?;
        
        let swap_completed_total = CounterVec::new(
            Opts::new("exchange_swap_completed_total", "Total swaps completed")
                .namespace("exchange"),
            &["base_currency", "quote_currency", "provider"],
        )?;
        registry.register(Box::new(swap_completed_total.clone()))?;
        
        let swap_failed_total = CounterVec::new(
            Opts::new("exchange_swap_failed_total", "Total swaps failed")
                .namespace("exchange"),
            &["base_currency", "quote_currency", "provider", "reason"],
        )?;
        registry.register(Box::new(swap_failed_total.clone()))?;
        
        let swap_processing_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_swap_processing_duration_seconds", "Swap processing duration")
                .namespace("exchange")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0]),
            &["base_currency", "quote_currency", "provider"],
        )?;
        registry.register(Box::new(swap_processing_duration_seconds.clone()))?;
        
        let swap_amount_usd = HistogramVec::new(
            HistogramOpts::new("exchange_swap_amount_usd", "Swap amount in USD")
                .namespace("exchange")
                .buckets(vec![10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]),
            &["base_currency", "quote_currency"],
        )?;
        registry.register(Box::new(swap_amount_usd.clone()))?;
        
        let swap_active_count = GaugeVec::new(
            Opts::new("exchange_swap_active_count", "Active swaps by status")
                .namespace("exchange"),
            &["status"],
        )?;
        registry.register(Box::new(swap_active_count.clone()))?;
        
        // Payout Metrics
        let payout_initiated_total = CounterVec::new(
            Opts::new("exchange_payout_initiated_total", "Total payouts initiated")
                .namespace("exchange"),
            &["chain", "currency"],
        )?;
        registry.register(Box::new(payout_initiated_total.clone()))?;
        
        let payout_completed_total = CounterVec::new(
            Opts::new("exchange_payout_completed_total", "Total payouts completed")
                .namespace("exchange"),
            &["chain", "currency"],
        )?;
        registry.register(Box::new(payout_completed_total.clone()))?;
        
        let payout_failed_total = CounterVec::new(
            Opts::new("exchange_payout_failed_total", "Total payouts failed")
                .namespace("exchange"),
            &["chain", "currency", "reason"],
        )?;
        registry.register(Box::new(payout_failed_total.clone()))?;
        
        let payout_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_payout_duration_seconds", "Payout processing duration")
                .namespace("exchange")
                .buckets(vec![1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0]),
            &["chain", "currency"],
        )?;
        registry.register(Box::new(payout_duration_seconds.clone()))?;
        
        let payout_gas_cost = HistogramVec::new(
            HistogramOpts::new("exchange_payout_gas_cost", "Payout gas cost in native currency")
                .namespace("exchange")
                .buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 10.0, 100.0]),
            &["chain"],
        )?;
        registry.register(Box::new(payout_gas_cost.clone()))?;
        
        // RPC Metrics
        let rpc_endpoint_health_score = GaugeVec::new(
            Opts::new("exchange_rpc_endpoint_health_score", "RPC endpoint health score (0.0-1.0)")
                .namespace("exchange"),
            &["chain", "endpoint"],
        )?;
        registry.register(Box::new(rpc_endpoint_health_score.clone()))?;
        
        let rpc_requests_total = CounterVec::new(
            Opts::new("exchange_rpc_requests_total", "Total RPC requests")
                .namespace("exchange"),
            &["chain", "endpoint", "method", "status"],
        )?;
        registry.register(Box::new(rpc_requests_total.clone()))?;
        
        let rpc_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_rpc_request_duration_seconds", "RPC request duration")
                .namespace("exchange")
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["chain", "endpoint", "method"],
        )?;
        registry.register(Box::new(rpc_request_duration_seconds.clone()))?;
        
        let rpc_circuit_breaker_state = GaugeVec::new(
            Opts::new("exchange_rpc_circuit_breaker_state", "Circuit breaker state (0=closed, 1=half-open, 2=open)")
                .namespace("exchange"),
            &["chain", "endpoint"],
        )?;
        registry.register(Box::new(rpc_circuit_breaker_state.clone()))?;
        
        let rpc_block_height_lag = GaugeVec::new(
            Opts::new("exchange_rpc_block_height_lag", "Block height lag from highest known")
                .namespace("exchange"),
            &["chain", "endpoint"],
        )?;
        registry.register(Box::new(rpc_block_height_lag.clone()))?;
        
        // Cache Metrics
        let cache_operations_total = CounterVec::new(
            Opts::new("exchange_cache_operations_total", "Total cache operations")
                .namespace("exchange"),
            &["cache", "operation", "result"],
        )?;
        registry.register(Box::new(cache_operations_total.clone()))?;
        
        let cache_hit_ratio = GaugeVec::new(
            Opts::new("exchange_cache_hit_ratio", "Cache hit ratio")
                .namespace("exchange"),
            &["cache", "key_pattern"],
        )?;
        registry.register(Box::new(cache_hit_ratio.clone()))?;
        
        let cache_size_bytes = GaugeVec::new(
            Opts::new("exchange_cache_size_bytes", "Cache size in bytes")
                .namespace("exchange"),
            &["cache"],
        )?;
        registry.register(Box::new(cache_size_bytes.clone()))?;
        
        let cache_entries_total = GaugeVec::new(
            Opts::new("exchange_cache_entries_total", "Total cache entries")
                .namespace("exchange"),
            &["cache"],
        )?;
        registry.register(Box::new(cache_entries_total.clone()))?;
        
        let cache_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_cache_operation_duration_seconds", "Cache operation duration")
                .namespace("exchange")
                .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
            &["cache", "operation"],
        )?;
        registry.register(Box::new(cache_operation_duration_seconds.clone()))?;
        
        // Database Metrics
        let db_queries_total = CounterVec::new(
            Opts::new("exchange_db_queries_total", "Total database queries")
                .namespace("exchange"),
            &["operation", "table", "status"],
        )?;
        registry.register(Box::new(db_queries_total.clone()))?;
        
        let db_query_duration_seconds = HistogramVec::new(
            HistogramOpts::new("exchange_db_query_duration_seconds", "Database query duration")
                .namespace("exchange")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["operation", "table"],
        )?;
        registry.register(Box::new(db_query_duration_seconds.clone()))?;
        
        let db_connections_active = Gauge::new(
            "exchange_db_connections_active",
            "Active database connections",
        )?;
        registry.register(Box::new(db_connections_active.clone()))?;
        
        let db_connections_idle = Gauge::new(
            "exchange_db_connections_idle",
            "Idle database connections",
        )?;
        registry.register(Box::new(db_connections_idle.clone()))?;
        
        let db_connections_max = Gauge::new(
            "exchange_db_connections_max",
            "Maximum database connections",
        )?;
        registry.register(Box::new(db_connections_max.clone()))?;
        
        // Business Metrics
        let revenue_total_usd = CounterVec::new(
            Opts::new("exchange_revenue_total_usd", "Total revenue in USD")
                .namespace("exchange"),
            &["currency", "fee_type"],
        )?;
        registry.register(Box::new(revenue_total_usd.clone()))?;
        
        let tvl_usd = GaugeVec::new(
            Opts::new("exchange_tvl_usd", "Total Value Locked in USD")
                .namespace("exchange"),
            &["currency"],
        )?;
        registry.register(Box::new(tvl_usd.clone()))?;
        
        let user_swaps_total = CounterVec::new(
            Opts::new("exchange_user_swaps_total", "Total user swaps")
                .namespace("exchange"),
            &["user_tier"],
        )?;
        registry.register(Box::new(user_swaps_total.clone()))?;
        
        let commission_per_swap_usd = HistogramVec::new(
            HistogramOpts::new("exchange_commission_per_swap_usd", "Commission per swap in USD")
                .namespace("exchange")
                .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0]),
            &["currency_pair"],
        )?;
        registry.register(Box::new(commission_per_swap_usd.clone()))?;
        
        Ok(Arc::new(Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            http_response_size_bytes,
            swap_initiated_total,
            swap_completed_total,
            swap_failed_total,
            swap_processing_duration_seconds,
            swap_amount_usd,
            swap_active_count,
            payout_initiated_total,
            payout_completed_total,
            payout_failed_total,
            payout_duration_seconds,
            payout_gas_cost,
            rpc_endpoint_health_score,
            rpc_requests_total,
            rpc_request_duration_seconds,
            rpc_circuit_breaker_state,
            rpc_block_height_lag,
            cache_operations_total,
            cache_hit_ratio,
            cache_size_bytes,
            cache_entries_total,
            cache_operation_duration_seconds,
            db_queries_total,
            db_query_duration_seconds,
            db_connections_active,
            db_connections_idle,
            db_connections_max,
            revenue_total_usd,
            tvl_usd,
            user_swaps_total,
            commission_per_swap_usd,
        }))
    }
    
    /// Export metrics in Prometheus text format
    pub fn export(&self) -> Result<String, Box<dyn std::error::Error>> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
    
    /// Get the underlying registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}
