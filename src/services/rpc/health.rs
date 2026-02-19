use std::collections::VecDeque;
use std::time::Instant;
use super::circuit_breaker::{CircuitBreaker, CircuitState};

const MAX_LATENCY_SAMPLES: usize = 100;

#[derive(Debug, Clone)]
pub struct EndpointHealth {
    pub url: String,
    pub circuit_breaker: CircuitBreaker,
    pub health_score: f64,
    
    // Metrics (sliding window)
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub latencies: VecDeque<u64>, // Last N latencies in milliseconds
    
    // Availability tracking
    pub last_success: Option<Instant>,
    pub last_failure: Option<Instant>,
    
    // Block height tracking (for blockchain RPCs)
    pub last_block_height: Option<u64>,
    pub last_block_time: Option<Instant>,
    
    // Weighted round robin state
    pub current_weight: i32,
    pub effective_weight: i32,
}

impl EndpointHealth {
    pub fn new(url: String, failure_threshold: f64, min_requests: u32, timeout_seconds: u64, half_open_max: u32, weight: u32) -> Self {
        Self {
            url,
            circuit_breaker: CircuitBreaker::new(failure_threshold, min_requests, timeout_seconds, half_open_max),
            health_score: 1.0,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            latencies: VecDeque::with_capacity(MAX_LATENCY_SAMPLES),
            last_success: None,
            last_failure: None,
            last_block_height: None,
            last_block_time: None,
            current_weight: 0,
            effective_weight: weight as i32,
        }
    }

    /// Record successful request
    pub fn record_success(&mut self, latency_ms: u64, block_height: Option<u64>) {
        self.total_requests += 1;
        self.successful_requests += 1;
        self.last_success = Some(Instant::now());
        
        // Update latencies (keep last N)
        if self.latencies.len() >= MAX_LATENCY_SAMPLES {
            self.latencies.pop_front();
        }
        self.latencies.push_back(latency_ms);
        
        // Update block height
        if let Some(height) = block_height {
            self.last_block_height = Some(height);
            self.last_block_time = Some(Instant::now());
        }
        
        // Update circuit breaker
        self.circuit_breaker.record_success();
        
        // Recalculate health score
        self.calculate_health_score();
    }

    /// Record failed request
    pub fn record_failure(&mut self, latency_ms: u64) {
        self.total_requests += 1;
        self.failed_requests += 1;
        self.last_failure = Some(Instant::now());
        
        // Still record latency for failed requests
        if self.latencies.len() >= MAX_LATENCY_SAMPLES {
            self.latencies.pop_front();
        }
        self.latencies.push_back(latency_ms);
        
        // Update circuit breaker
        self.circuit_breaker.record_failure();
        
        // Recalculate health score
        self.calculate_health_score();
    }

    /// Calculate composite health score (0.0 to 1.0)
    fn calculate_health_score(&mut self) {
        // Weights for each component
        const W_AVAILABILITY: f64 = 0.3;
        const W_LATENCY: f64 = 0.3;
        const W_SUCCESS_RATE: f64 = 0.3;
        const W_BLOCK_HEIGHT: f64 = 0.1;

        // 1. Availability Score (based on circuit breaker state)
        let availability_score = match self.circuit_breaker.state {
            CircuitState::Closed => 1.0,
            CircuitState::HalfOpen => 0.5,
            CircuitState::Open => 0.0,
        };

        // 2. Latency Score (inverse normalized)
        let latency_score = if self.latencies.is_empty() {
            1.0
        } else {
            let avg_latency = self.average_latency();
            let max_acceptable = 5000.0; // 5 seconds
            1.0 - (avg_latency / max_acceptable).min(1.0)
        };

        // 3. Success Rate Score
        let success_rate_score = if self.total_requests == 0 {
            1.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        };

        // 4. Block Height Score (freshness)
        let block_height_score = if let Some(last_block_time) = self.last_block_time {
            let age_ms = last_block_time.elapsed().as_millis() as f64;
            let max_staleness = 60000.0; // 1 minute
            1.0 - (age_ms / max_staleness).min(1.0)
        } else {
            0.5 // Neutral if no block height data
        };

        // Calculate composite score
        self.health_score = W_AVAILABILITY * availability_score
            + W_LATENCY * latency_score
            + W_SUCCESS_RATE * success_rate_score
            + W_BLOCK_HEIGHT * block_height_score;
    }

    /// Calculate average latency
    pub fn average_latency(&self) -> f64 {
        if self.latencies.is_empty() {
            0.0
        } else {
            let sum: u64 = self.latencies.iter().sum();
            sum as f64 / self.latencies.len() as f64
        }
    }

    /// Calculate P95 latency
    pub fn calculate_p95(&self) -> Option<u64> {
        if self.latencies.is_empty() {
            return None;
        }

        let mut sorted: Vec<u64> = self.latencies.iter().copied().collect();
        sorted.sort_unstable();
        
        let index = ((sorted.len() as f64 * 0.95) as usize).min(sorted.len() - 1);
        Some(sorted[index])
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            1.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        }
    }

    /// Check if endpoint is healthy
    pub fn is_healthy(&self) -> bool {
        self.circuit_breaker.state != CircuitState::Open && self.health_score > 0.3
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EndpointHealthStatus {
    pub url: String,
    pub state: String,
    pub health_score: f64,
    pub total_requests: u64,
    pub success_rate: f64,
    pub average_latency_ms: f64,
    pub p95_latency_ms: Option<u64>,
    pub last_block_height: Option<u64>,
    pub is_healthy: bool,
}

use serde::{Deserialize, Serialize};

impl From<&EndpointHealth> for EndpointHealthStatus {
    fn from(health: &EndpointHealth) -> Self {
        Self {
            url: health.url.clone(),
            state: format!("{:?}", health.circuit_breaker.state),
            health_score: health.health_score,
            total_requests: health.total_requests,
            success_rate: health.success_rate(),
            average_latency_ms: health.average_latency(),
            p95_latency_ms: health.calculate_p95(),
            last_block_height: health.last_block_height,
            is_healthy: health.is_healthy(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_score_calculation() {
        let mut health = EndpointHealth::new("http://test".to_string(), 0.2, 5, 30, 3, 100);
        
        // Record some successes
        for _ in 0..10 {
            health.record_success(100, Some(1000));
        }
        
        // Health score should be high
        assert!(health.health_score > 0.8);
        assert!(health.is_healthy());
    }

    #[test]
    fn test_p95_calculation() {
        let mut health = EndpointHealth::new("http://test".to_string(), 0.2, 5, 30, 3, 100);
        
        // Add latencies: 100ms x 95, 1000ms x 5
        for _ in 0..95 {
            health.record_success(100, None);
        }
        for _ in 0..5 {
            health.record_success(1000, None);
        }
        
        let p95 = health.calculate_p95().unwrap();
        assert!(p95 >= 100 && p95 <= 1000);
    }
}
