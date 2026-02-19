use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
pub struct WebhookCircuitBreaker {
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub total_requests: u32,
    pub opened_at: Option<Instant>,
    pub half_open_attempts: u32,
    
    // Configuration
    pub failure_threshold: f64,
    pub min_requests: u32,
    pub timeout_secs: u64,
    pub half_open_max: u32,
}

impl WebhookCircuitBreaker {
    pub fn new(
        failure_threshold: f64,
        min_requests: u32,
        timeout_secs: u64,
        half_open_max: u32,
    ) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            total_requests: 0,
            opened_at: None,
            half_open_attempts: 0,
            failure_threshold,
            min_requests,
            timeout_secs,
            half_open_max,
        }
    }
    
    /// Check if request should be allowed (non-mutating)
    pub fn allow_request(&self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(opened_at) = self.opened_at {
                    opened_at.elapsed().as_secs() >= self.timeout_secs
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                self.half_open_attempts < self.half_open_max
            }
        }
    }
    
    /// Check and potentially transition state (mutating version)
    pub fn check_and_allow(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed().as_secs() >= self.timeout_secs {
                        self.transition_to_half_open();
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                self.half_open_attempts < self.half_open_max
            }
        }
    }
    
    /// Record successful request
    pub fn record_success(&mut self) {
        self.total_requests += 1;
        self.success_count += 1;
        
        match self.state {
            CircuitState::HalfOpen => {
                self.half_open_attempts += 1;
                if self.half_open_attempts >= self.half_open_max {
                    self.transition_to_closed();
                }
            }
            CircuitState::Closed => {
                // Check if we should stay closed
                self.check_state_transition();
            }
            CircuitState::Open => {}
        }
    }
    
    /// Record failed request
    pub fn record_failure(&mut self) {
        self.total_requests += 1;
        self.failure_count += 1;
        
        match self.state {
            CircuitState::HalfOpen => {
                // Any failure in half-open state reopens the circuit
                self.transition_to_open();
            }
            CircuitState::Closed => {
                self.check_state_transition();
            }
            CircuitState::Open => {}
        }
    }
    
    fn check_state_transition(&mut self) {
        if self.total_requests < self.min_requests {
            return;
        }
        
        let failure_rate = self.failure_count as f64 / self.total_requests as f64;
        
        if failure_rate >= self.failure_threshold {
            self.transition_to_open();
        }
    }
    
    fn transition_to_open(&mut self) {
        self.state = CircuitState::Open;
        self.opened_at = Some(Instant::now());
        tracing::warn!(
            "Webhook circuit breaker opened: failure_rate={:.2}%, failures={}, total={}",
            (self.failure_count as f64 / self.total_requests as f64) * 100.0,
            self.failure_count,
            self.total_requests
        );
    }
    
    fn transition_to_half_open(&mut self) {
        self.state = CircuitState::HalfOpen;
        self.half_open_attempts = 0;
        tracing::info!("Webhook circuit breaker half-open: testing recovery");
    }
    
    fn transition_to_closed(&mut self) {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.success_count = 0;
        self.total_requests = 0;
        self.opened_at = None;
        self.half_open_attempts = 0;
        tracing::info!("Webhook circuit breaker closed: endpoint recovered");
    }
    
    /// Get current failure rate
    pub fn failure_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.failure_count as f64 / self.total_requests as f64
        }
    }
    
    /// Reset circuit breaker (for manual intervention)
    pub fn reset(&mut self) {
        self.transition_to_closed();
    }
}

impl Default for WebhookCircuitBreaker {
    fn default() -> Self {
        Self::new(
            0.5,  // 50% failure threshold
            10,   // minimum 10 requests
            3600, // 1 hour timeout
            3,    // 3 test requests in half-open
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let mut cb = WebhookCircuitBreaker::new(0.5, 10, 3600, 3);
        
        // Record 5 successes, 6 failures (60% failure rate > 50% threshold)
        for _ in 0..5 {
            cb.record_success();
        }
        for _ in 0..6 {
            cb.record_failure();
        }
        
        assert_eq!(cb.state, CircuitState::Open);
        assert_eq!(cb.failure_rate(), 6.0 / 11.0);
    }

    #[test]
    fn test_circuit_breaker_min_requests() {
        let mut cb = WebhookCircuitBreaker::new(0.5, 10, 3600, 3);
        
        // Only 5 requests (below min_requests threshold)
        for _ in 0..5 {
            cb.record_failure();
        }
        
        // Should stay closed
        assert_eq!(cb.state, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_recovery() {
        let mut cb = WebhookCircuitBreaker::new(0.5, 10, 0, 3); // 0 timeout for testing
        
        // Open the circuit
        for _ in 0..11 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
        
        // Check and allow should transition to half-open
        assert!(cb.check_and_allow());
        assert_eq!(cb.state, CircuitState::HalfOpen);
        
        // Record 3 successes to close
        cb.record_success();
        cb.record_success();
        cb.record_success();
        
        assert_eq!(cb.state, CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure() {
        let mut cb = WebhookCircuitBreaker::new(0.5, 10, 0, 3);
        
        // Open the circuit
        for _ in 0..11 {
            cb.record_failure();
        }
        
        // Transition to half-open
        cb.check_and_allow();
        assert_eq!(cb.state, CircuitState::HalfOpen);
        
        // Record failure in half-open state
        cb.record_failure();
        
        // Should reopen
        assert_eq!(cb.state, CircuitState::Open);
    }

    #[test]
    fn test_allow_request() {
        let mut cb = WebhookCircuitBreaker::default();
        
        // Closed state allows requests
        assert!(cb.allow_request());
        
        // Open circuit
        for _ in 0..11 {
            cb.record_failure();
        }
        
        // Open state blocks requests
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_reset() {
        let mut cb = WebhookCircuitBreaker::default();
        
        // Open the circuit
        for _ in 0..11 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
        
        // Reset
        cb.reset();
        
        assert_eq!(cb.state, CircuitState::Closed);
        assert_eq!(cb.failure_count, 0);
        assert_eq!(cb.total_requests, 0);
    }
}
