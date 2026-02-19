use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde_json::{json, Value};
use serde::de::DeserializeOwned;

use super::config::{RpcConfig, RpcEndpoint, LoadBalancingStrategy, RpcAuth};
use super::health::{EndpointHealth, EndpointHealthStatus};

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("Chain not configured: {0}")]
    ChainNotConfigured(String),
    #[error("No healthy endpoints available")]
    NoHealthyEndpoints,
    #[error("All endpoints failed")]
    AllEndpointsFailed,
    #[error("Network error: {0}")]
    Network(String),
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Circuit breaker open")]
    CircuitBreakerOpen,
}

pub struct RpcManager {
    configs: HashMap<String, RpcConfig>,
    health_tracker: Arc<RwLock<HashMap<String, EndpointHealth>>>,
    client: reqwest::Client,
    round_robin_indices: Arc<RwLock<HashMap<String, usize>>>,
}

impl RpcManager {
    pub fn new(configs: HashMap<String, RpcConfig>) -> Self {
        let mut health_tracker = HashMap::new();
        
        // Initialize health tracking for all endpoints
        for (_chain, config) in &configs {
            for endpoint in &config.endpoints {
                let health = EndpointHealth::new(
                    endpoint.url.clone(),
                    config.circuit_breaker_config.failure_threshold,
                    config.circuit_breaker_config.min_requests,
                    config.circuit_breaker_config.timeout_seconds,
                    config.circuit_breaker_config.half_open_max_requests,
                    endpoint.weight,
                );
                health_tracker.insert(endpoint.url.clone(), health);
            }
        }
        
        Self {
            configs,
            health_tracker: Arc::new(RwLock::new(health_tracker)),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            round_robin_indices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Select best endpoint based on health scores and strategy
    pub async fn select_endpoint(&self, chain: &str) -> Result<String, RpcError> {
        let config = self.configs.get(chain)
            .ok_or_else(|| RpcError::ChainNotConfigured(chain.to_string()))?;
        
        let health = self.health_tracker.read().await;
        
        // Filter to only healthy endpoints (circuit not open)
        let mut candidates: Vec<&RpcEndpoint> = config.endpoints.iter()
            .filter(|ep| {
                health.get(&ep.url)
                    .map(|h| h.circuit_breaker.allow_request())
                    .unwrap_or(true)
            })
            .collect();
        
        if candidates.is_empty() {
            return Err(RpcError::NoHealthyEndpoints);
        }
        
        // Sort by priority first
        candidates.sort_by_key(|ep| ep.priority);
        
        // Filter to only highest priority endpoints
        let min_priority = candidates[0].priority;
        candidates.retain(|ep| ep.priority == min_priority);
        
        // Apply strategy
        match config.strategy {
            LoadBalancingStrategy::HealthScoreBased => {
                // Select endpoint with highest health score
                candidates.sort_by(|a, b| {
                    let score_a = health.get(&a.url).map(|h| h.health_score).unwrap_or(0.0);
                    let score_b = health.get(&b.url).map(|h| h.health_score).unwrap_or(0.0);
                    score_b.partial_cmp(&score_a).unwrap()
                });
                Ok(candidates[0].url.clone())
            }
            LoadBalancingStrategy::WeightedRoundRobin => {
                drop(health);
                self.weighted_round_robin_select(chain, candidates).await
            }
            LoadBalancingStrategy::LeastLatency => {
                // Select endpoint with lowest P95 latency
                candidates.sort_by(|a, b| {
                    let lat_a = health.get(&a.url).and_then(|h| h.calculate_p95()).unwrap_or(u64::MAX);
                    let lat_b = health.get(&b.url).and_then(|h| h.calculate_p95()).unwrap_or(u64::MAX);
                    lat_a.cmp(&lat_b)
                });
                Ok(candidates[0].url.clone())
            }
            LoadBalancingStrategy::RoundRobin => {
                drop(health);
                self.round_robin_select(chain, candidates).await
            }
        }
    }

    /// Weighted Round Robin selection (Smooth WRR)
    async fn weighted_round_robin_select(&self, _chain: &str, candidates: Vec<&RpcEndpoint>) -> Result<String, RpcError> {
        let mut health = self.health_tracker.write().await;
        
        let mut max_weight = i32::MIN;
        let mut selected_url = String::new();
        let mut total_weight = 0i32;
        
        for endpoint in &candidates {
            if let Some(h) = health.get_mut(&endpoint.url) {
                h.current_weight += h.effective_weight;
                total_weight += h.effective_weight;
                
                if h.current_weight > max_weight {
                    max_weight = h.current_weight;
                    selected_url = endpoint.url.clone();
                }
            }
        }
        
        // Decrease selected endpoint's current weight
        if let Some(h) = health.get_mut(&selected_url) {
            h.current_weight -= total_weight;
        }
        
        Ok(selected_url)
    }

    /// Simple Round Robin selection
    async fn round_robin_select(&self, chain: &str, candidates: Vec<&RpcEndpoint>) -> Result<String, RpcError> {
        let mut indices = self.round_robin_indices.write().await;
        let index = indices.entry(chain.to_string()).or_insert(0);
        
        let selected = &candidates[*index % candidates.len()];
        *index = (*index + 1) % candidates.len();
        
        Ok(selected.url.clone())
    }

    /// Execute RPC call with automatic failover
    pub async fn call<T: DeserializeOwned>(
        &self,
        chain: &str,
        method: &str,
        params: Value,
    ) -> Result<T, RpcError> {
        let config = self.configs.get(chain)
            .ok_or_else(|| RpcError::ChainNotConfigured(chain.to_string()))?;
        
        let max_attempts = config.endpoints.len().min(3);
        
        for attempt in 0..max_attempts {
            // Select endpoint
            let url = self.select_endpoint(chain).await?;
            
            // Get endpoint config for auth
            let endpoint = config.endpoints.iter()
                .find(|ep| ep.url == url)
                .ok_or_else(|| RpcError::Network("Endpoint not found".to_string()))?;
            
            // Execute request with timeout
            let start = Instant::now();
            let result = self.execute_rpc_call(&url, method, params.clone(), endpoint).await;
            let latency = start.elapsed();
            
            match result {
                Ok(response) => {
                    // Record success
                    self.record_result(&url, latency, true, None).await;
                    return Ok(response);
                }
                Err(e) => {
                    // Record failure
                    self.record_result(&url, latency, false, None).await;
                    
                    // Check if we should retry
                    if attempt < max_attempts - 1 {
                        // Apply exponential backoff with jitter
                        let backoff = calculate_backoff(attempt as u32);
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    
                    return Err(e);
                }
            }
        }
        
        Err(RpcError::AllEndpointsFailed)
    }

    /// Execute single RPC call
    async fn execute_rpc_call<T: DeserializeOwned>(
        &self,
        url: &str,
        method: &str,
        params: Value,
        endpoint: &RpcEndpoint,
    ) -> Result<T, RpcError> {
        let payload = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });

        let mut request = self.client.post(url)
            .json(&payload)
            .timeout(Duration::from_millis(endpoint.timeout_ms));

        // Add authentication if configured
        if let Some(auth) = &endpoint.auth {
            request = match auth {
                RpcAuth::ApiKey { key } => request.header("X-API-Key", key),
                RpcAuth::Bearer { token } => request.bearer_auth(token),
                RpcAuth::Basic { username, password } => request.basic_auth(username, Some(password)),
            };
        }

        let response = request.send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?;

        let rpc_response: RpcResponse<T> = response.json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        if let Some(err) = rpc_response.error {
            return Err(RpcError::Rpc(err.message));
        }

        rpc_response.result.ok_or_else(|| RpcError::Parse("Missing result".to_string()))
    }

    /// Record request result and update health metrics
    pub async fn record_result(&self, url: &str, latency: Duration, success: bool, block_height: Option<u64>) {
        let mut health = self.health_tracker.write().await;
        
        if let Some(h) = health.get_mut(url) {
            let latency_ms = latency.as_millis() as u64;
            
            if success {
                h.record_success(latency_ms, block_height);
            } else {
                h.record_failure(latency_ms);
            }
        }
    }

    /// Background health check loop
    pub async fn health_check_loop(self: Arc<Self>) {
        loop {
            for (chain, config) in &self.configs {
                for endpoint in &config.endpoints {
                    let result = self.check_endpoint_health(&endpoint.url, chain).await;
                    
                    let latency = result.latency;
                    let success = result.success;
                    let block_height = result.block_height;
                    
                    self.record_result(&endpoint.url, latency, success, block_height).await;
                }
            }
            
            // Sleep based on shortest health check interval
            let min_interval = self.configs.values()
                .map(|c| c.health_check_interval)
                .min()
                .unwrap_or(30);
            
            tokio::time::sleep(Duration::from_secs(min_interval)).await;
        }
    }

    /// Check endpoint health
    async fn check_endpoint_health(&self, url: &str, chain: &str) -> HealthCheckResult {
        let start = Instant::now();
        
        // Determine health check method based on chain
        let method = match chain {
            "ethereum" | "polygon" | "bsc" | "arbitrum" | "optimism" => "eth_blockNumber",
            "bitcoin" => "getblockcount",
            "solana" => "getBlockHeight",
            _ => "eth_blockNumber", // Default to EVM
        };
        
        let endpoint = self.configs.get(chain)
            .and_then(|c| c.endpoints.iter().find(|ep| ep.url == url));
        
        if endpoint.is_none() {
            return HealthCheckResult {
                success: false,
                latency: start.elapsed(),
                block_height: None,
            };
        }
        
        let result = self.execute_rpc_call::<Value>(url, method, json!([]), endpoint.unwrap()).await;
        let latency = start.elapsed();
        
        match result {
            Ok(response) => {
                // Extract block height if available
                let block_height = extract_block_height(&response, chain);
                
                HealthCheckResult {
                    success: true,
                    latency,
                    block_height,
                }
            }
            Err(_) => HealthCheckResult {
                success: false,
                latency,
                block_height: None,
            }
        }
    }

    /// Get current health status for all endpoints
    pub async fn get_health_status(&self, chain: &str) -> Vec<EndpointHealthStatus> {
        let config = match self.configs.get(chain) {
            Some(c) => c,
            None => return Vec::new(),
        };
        
        let health = self.health_tracker.read().await;
        
        config.endpoints.iter()
            .filter_map(|ep| {
                health.get(&ep.url).map(|h| EndpointHealthStatus::from(h))
            })
            .collect()
    }
}

#[derive(serde::Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcErrorObj>,
}

#[derive(serde::Deserialize)]
struct RpcErrorObj {
    message: String,
}

struct HealthCheckResult {
    success: bool,
    latency: Duration,
    block_height: Option<u64>,
}

/// Calculate exponential backoff with jitter
fn calculate_backoff(attempt: u32) -> Duration {
    let base_ms = 100;
    let max_ms = 30_000;
    let delay_ms = std::cmp::min(base_ms * 2_u64.pow(attempt), max_ms);
    
    // Add ±10% jitter
    let jitter = rand::random::<f64>() * 0.2 - 0.1;
    let final_delay = (delay_ms as f64 * (1.0 + jitter)) as u64;
    
    Duration::from_millis(final_delay)
}

/// Extract block height from RPC response
fn extract_block_height(response: &Value, chain: &str) -> Option<u64> {
    match chain {
        "ethereum" | "polygon" | "bsc" | "arbitrum" | "optimism" => {
            // EVM chains return hex string
            response.as_str()
                .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        }
        "bitcoin" | "solana" => {
            // Bitcoin and Solana return number
            response.as_u64()
        }
        _ => None,
    }
}
