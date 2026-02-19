use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    pub chain: String,
    pub endpoints: Vec<RpcEndpoint>,
    pub strategy: LoadBalancingStrategy,
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval: u64, // seconds
    pub circuit_breaker_config: CircuitBreakerConfig,
}

fn default_health_check_interval() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEndpoint {
    pub url: String,
    #[serde(default = "default_priority")]
    pub priority: u8,
    #[serde(default = "default_weight")]
    pub weight: u32,
    pub max_requests_per_second: Option<u32>,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    pub auth: Option<RpcAuth>,
}

fn default_priority() -> u8 {
    5
}

fn default_weight() -> u32 {
    100
}

fn default_timeout() -> u64 {
    5000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RpcAuth {
    ApiKey { key: String },
    Bearer { token: String },
    Basic { username: String, password: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    WeightedRoundRobin,
    LeastLatency,
    HealthScoreBased,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: f64,
    #[serde(default = "default_min_requests")]
    pub min_requests: u32,
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default = "default_half_open_max_requests")]
    pub half_open_max_requests: u32,
}

fn default_failure_threshold() -> f64 {
    0.2
}

fn default_min_requests() -> u32 {
    5
}

fn default_timeout_seconds() -> u64 {
    30
}

fn default_half_open_max_requests() -> u32 {
    3
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 0.2,
            min_requests: 5,
            timeout_seconds: 30,
            half_open_max_requests: 3,
        }
    }
}

/// Load RPC configuration from JSON file with environment variable substitution
pub fn load_rpc_config(path: &str) -> Result<std::collections::HashMap<String, RpcConfig>, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    
    // Substitute environment variables
    let content = substitute_env_vars(&content);
    
    let configs: std::collections::HashMap<String, RpcConfig> = serde_json::from_str(&content)?;
    Ok(configs)
}

/// Substitute ${VAR_NAME} with environment variable values
fn substitute_env_vars(content: &str) -> String {
    let mut result = content.to_string();
    
    // Find all ${VAR_NAME} patterns
    let re = regex::Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)\}").unwrap();
    
    for cap in re.captures_iter(content) {
        let var_name = &cap[1];
        if let Ok(value) = std::env::var(var_name) {
            result = result.replace(&format!("${{{}}}", var_name), &value);
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("TEST_VAR", "test_value");
        let input = r#"{"url": "${TEST_VAR}"}"#;
        let output = substitute_env_vars(input);
        assert_eq!(output, r#"{"url": "test_value"}"#);
    }
}
