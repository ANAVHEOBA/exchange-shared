use std::time::Duration;
use serde::{Deserialize, Serialize};
use rand::Rng;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundConfig {
    // Timeout settings (seconds)
    pub deposit_timeout: u64,
    pub processing_timeout: u64,
    pub payout_timeout: u64,
    pub refund_timeout: u64,
    
    // Retry settings
    pub max_retry_attempts: u32,
    pub base_retry_delay: u64,
    pub max_retry_delay: u64,
    pub jitter_factor: f64,
    
    // Gas escalation
    pub gas_multiplier_per_retry: f64,
    pub max_gas_multiplier: f64,
    
    // Economic thresholds (in base units)
    pub min_refund_threshold_btc: f64,
    pub min_refund_threshold_eth: f64,
    pub min_refund_threshold_usd: f64,
    
    // Processing
    pub worker_pool_size: usize,
    pub batch_size: usize,
    pub check_interval: u64,
    
    // Priority weights
    pub priority_weight_age: f64,
    pub priority_weight_amount: f64,
    pub priority_weight_retry: f64,
}

impl Default for RefundConfig {
    fn default() -> Self {
        Self {
            deposit_timeout: 1800,
            processing_timeout: 7200,
            payout_timeout: 3600,
            refund_timeout: 1800,
            
            max_retry_attempts: 5,
            base_retry_delay: 60,
            max_retry_delay: 1800,
            jitter_factor: 0.1,
            
            gas_multiplier_per_retry: 0.1,
            max_gas_multiplier: 2.0,
            
            min_refund_threshold_btc: 0.0001,
            min_refund_threshold_eth: 0.001,
            min_refund_threshold_usd: 1.0,
            
            worker_pool_size: 10,
            batch_size: 100,
            check_interval: 60,
            
            priority_weight_age: 0.5,
            priority_weight_amount: 0.3,
            priority_weight_retry: 0.2,
        }
    }
}

impl RefundConfig {
    /// Load from environment variables
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        
        if let Ok(val) = std::env::var("REFUND_DEPOSIT_TIMEOUT") {
            config.deposit_timeout = val.parse().map_err(|e| format!("Invalid REFUND_DEPOSIT_TIMEOUT: {}", e))?;
        }
        
        if let Ok(val) = std::env::var("REFUND_MAX_RETRY_ATTEMPTS") {
            config.max_retry_attempts = val.parse().map_err(|e| format!("Invalid REFUND_MAX_RETRY_ATTEMPTS: {}", e))?;
        }
        
        if let Ok(val) = std::env::var("REFUND_CHECK_INTERVAL") {
            config.check_interval = val.parse().map_err(|e| format!("Invalid REFUND_CHECK_INTERVAL: {}", e))?;
        }
        
        // Validate weights sum to 1.0
        let weight_sum = config.priority_weight_age + config.priority_weight_amount + config.priority_weight_retry;
        if (weight_sum - 1.0).abs() > 0.01 {
            return Err(format!("Priority weights must sum to 1.0, got {}", weight_sum));
        }
        
        Ok(config)
    }
    
    /// Calculate retry delay with exponential backoff and jitter
    pub fn calculate_retry_delay(&self, attempt: u32) -> Duration {
        let base = self.base_retry_delay as f64;
        let exponential = base * 2_f64.powi(attempt as i32);
        
        // Add jitter: random value in range [1 - jitter_factor, 1 + jitter_factor]
        let mut rng = rand::rng();
        let jitter = 1.0 + (rng.random::<f64>() * 2.0 - 1.0) * self.jitter_factor;
        let with_jitter = exponential * jitter;
        
        // Cap at max_delay
        let capped = with_jitter.min(self.max_retry_delay as f64);
        
        Duration::from_secs(capped as u64)
    }
    
    /// Calculate gas multiplier for retry attempt
    pub fn calculate_gas_multiplier(&self, attempt: u32) -> f64 {
        let multiplier = 1.0 + (self.gas_multiplier_per_retry * attempt as f64);
        multiplier.min(self.max_gas_multiplier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = RefundConfig::default();
        assert_eq!(config.deposit_timeout, 1800);
        assert_eq!(config.max_retry_attempts, 5);
        assert_eq!(config.check_interval, 60);
    }
    
    #[test]
    fn test_retry_delay_calculation() {
        let config = RefundConfig::default();
        
        let delay0 = config.calculate_retry_delay(0);
        assert!(delay0.as_secs() >= 54 && delay0.as_secs() <= 66); // 60s ± 10%
        
        let delay1 = config.calculate_retry_delay(1);
        assert!(delay1.as_secs() >= 108 && delay1.as_secs() <= 132); // 120s ± 10%
        
        let delay10 = config.calculate_retry_delay(10);
        assert_eq!(delay10.as_secs(), 1800); // Capped at max_delay
    }
    
    #[test]
    fn test_gas_multiplier() {
        let config = RefundConfig::default();
        
        assert_eq!(config.calculate_gas_multiplier(0), 1.0);
        assert_eq!(config.calculate_gas_multiplier(1), 1.1);
        assert_eq!(config.calculate_gas_multiplier(5), 1.5);
        assert_eq!(config.calculate_gas_multiplier(20), 2.0); // Capped at max
    }
    
    #[test]
    fn test_weight_validation() {
        let mut config = RefundConfig::default();
        config.priority_weight_age = 0.5;
        config.priority_weight_amount = 0.3;
        config.priority_weight_retry = 0.2;
        
        // Should be valid (sums to 1.0)
        let weight_sum = config.priority_weight_age + config.priority_weight_amount + config.priority_weight_retry;
        assert!((weight_sum - 1.0).abs() < 0.01);
    }
}
