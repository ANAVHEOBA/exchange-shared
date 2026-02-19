use std::time::Duration;
use rand::Rng;

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub base_delay_secs: u64,
    pub max_delay_secs: u64,
    pub max_attempts: u32,
    pub jitter_factor: f64,
    pub timeout_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay_secs: 30,        // 30 seconds
            max_delay_secs: 86400,      // 24 hours
            max_attempts: 10,           // 10 attempts
            jitter_factor: 0.1,         // ±10%
            timeout_secs: 30,           // 30 seconds per request
        }
    }
}

impl RetryConfig {
    /// Calculate delay for given attempt with exponential backoff and jitter
    /// Formula: delay = min(base_delay × 2^attempt × (1 + jitter), max_delay)
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base = self.base_delay_secs as f64;
        let exponential = base * 2_f64.powi(attempt as i32);
        
        // Add jitter: random value in range [1 - jitter_factor, 1 + jitter_factor]
        let mut rng = rand::rng();
        let jitter = 1.0 + (rng.random::<f64>() * 2.0 - 1.0) * self.jitter_factor;
        let with_jitter = exponential * jitter;
        
        // Cap at max_delay
        let capped = with_jitter.min(self.max_delay_secs as f64);
        
        Duration::from_secs(capped as u64)
    }
    
    /// Check if should retry based on attempt number
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
    
    /// Get timeout duration for HTTP request
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

/// Calculate retry schedule for visualization/testing
pub fn calculate_retry_schedule(config: &RetryConfig) -> Vec<Duration> {
    (0..config.max_attempts)
        .map(|attempt| config.calculate_delay(attempt))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.base_delay_secs, 30);
        assert_eq!(config.max_delay_secs, 86400);
        assert_eq!(config.max_attempts, 10);
    }

    #[test]
    fn test_calculate_delay_exponential() {
        let config = RetryConfig {
            base_delay_secs: 30,
            max_delay_secs: 86400,
            max_attempts: 10,
            jitter_factor: 0.0, // No jitter for predictable testing
            timeout_secs: 30,
        };
        
        // Attempt 0: 30 * 2^0 = 30s
        let delay0 = config.calculate_delay(0);
        assert_eq!(delay0.as_secs(), 30);
        
        // Attempt 1: 30 * 2^1 = 60s
        let delay1 = config.calculate_delay(1);
        assert_eq!(delay1.as_secs(), 60);
        
        // Attempt 2: 30 * 2^2 = 120s
        let delay2 = config.calculate_delay(2);
        assert_eq!(delay2.as_secs(), 120);
        
        // Attempt 3: 30 * 2^3 = 240s
        let delay3 = config.calculate_delay(3);
        assert_eq!(delay3.as_secs(), 240);
    }

    #[test]
    fn test_calculate_delay_with_jitter() {
        let config = RetryConfig::default();
        
        // Run multiple times to ensure jitter is applied
        let delays: Vec<u64> = (0..10)
            .map(|_| config.calculate_delay(0).as_secs())
            .collect();
        
        // All delays should be around 30s ± 10%
        for delay in &delays {
            assert!(*delay >= 27 && *delay <= 33, "Delay {} out of range", delay);
        }
        
        // Should have some variation
        let min = *delays.iter().min().unwrap();
        let max = *delays.iter().max().unwrap();
        assert!(max > min, "No jitter variation detected");
    }

    #[test]
    fn test_calculate_delay_capped() {
        let config = RetryConfig {
            base_delay_secs: 30,
            max_delay_secs: 1000, // Cap at 1000s
            max_attempts: 20,
            jitter_factor: 0.0,
            timeout_secs: 30,
        };
        
        // Attempt 10: 30 * 2^10 = 30720s, but capped at 1000s
        let delay = config.calculate_delay(10);
        assert_eq!(delay.as_secs(), 1000);
    }

    #[test]
    fn test_should_retry() {
        let config = RetryConfig::default();
        
        assert!(config.should_retry(0));
        assert!(config.should_retry(5));
        assert!(config.should_retry(9));
        assert!(!config.should_retry(10));
        assert!(!config.should_retry(11));
    }

    #[test]
    fn test_timeout() {
        let config = RetryConfig::default();
        assert_eq!(config.timeout().as_secs(), 30);
    }

    #[test]
    fn test_retry_schedule() {
        let config = RetryConfig {
            base_delay_secs: 10,
            max_delay_secs: 1000,
            max_attempts: 5,
            jitter_factor: 0.0,
            timeout_secs: 30,
        };
        
        let schedule = calculate_retry_schedule(&config);
        assert_eq!(schedule.len(), 5);
        
        // Verify exponential growth
        assert_eq!(schedule[0].as_secs(), 10);  // 10 * 2^0
        assert_eq!(schedule[1].as_secs(), 20);  // 10 * 2^1
        assert_eq!(schedule[2].as_secs(), 40);  // 10 * 2^2
        assert_eq!(schedule[3].as_secs(), 80);  // 10 * 2^3
        assert_eq!(schedule[4].as_secs(), 160); // 10 * 2^4
    }
}
