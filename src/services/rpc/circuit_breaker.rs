use std::time::Instant;

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    HalfOpen,
    Open,
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub total_requests: u32,
    pub consecutive_failures: u32,
    pub consecutive_successes: u32,
    pub opened_at: Option<Instant>,
    pub half_open_requests: u32,
    
    // Configuration
    pub failure_threshold: f64,
    pub min_requests: u32,
    pub timeout_seconds: u64,
    pub half_open_max_requests: u32,
}

impl CircuitBreaker {
    pub fn new(
        failure_threshold: f64,
        min_requests: u32,
        timeout_seconds: u64,
        half_open_max_requests: u32,
    ) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            total_requests: 0,
            consecutive_failures: 0,
            consecutive_successes: 0,
            opened_at: None,
            half_open_requests: 0,
            failure_threshold,
            min_requests,
            timeout_seconds,
            half_open_max_requests,
        }
    }

    /// Check if request should be allowed (non-mutating check)
    pub fn allow_request(&self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout has expired
                if let Some(opened_at) = self.opened_at {
                    opened_at.elapsed().as_secs() >= self.timeout_seconds
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in half-open state
                self.half_open_requests < self.half_open_max_requests
            }
        }
    }
    
    /// Check and potentially transition state (mutating version)
    pub fn check_and_allow(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout has expired
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed().as_secs() >= self.timeout_seconds {
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
                // Allow limited requests in half-open state
                self.half_open_requests < self.half_open_max_requests
            }
        }
    }

    /// Record successful request
    pub fn record_success(&mut self) {
        self.total_requests += 1;
        self.success_count += 1;
        self.consecutive_successes += 1;
        self.consecutive_failures = 0;

        match self.state {
            CircuitState::HalfOpen => {
                self.half_open_requests += 1;
                if self.consecutive_successes >= self.half_open_max_requests {
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
        self.consecutive_failures += 1;
        self.consecutive_successes = 0;

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

    /// Check if circuit should transition to open state
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
            "Circuit breaker opened: failure_rate={:.2}%, failures={}, total={}",
            (self.failure_count as f64 / self.total_requests as f64) * 100.0,
            self.failure_count,
            self.total_requests
        );
    }

    fn transition_to_half_open(&mut self) {
        self.state = CircuitState::HalfOpen;
        self.half_open_requests = 0;
        self.consecutive_successes = 0;
        self.consecutive_failures = 0;
        tracing::info!("Circuit breaker half-open: testing recovery");
    }

    fn transition_to_closed(&mut self) {
        self.state = CircuitState::Closed;
        self.failure_count = 0;
        self.success_count = 0;
        self.total_requests = 0;
        self.consecutive_failures = 0;
        self.consecutive_successes = 0;
        self.opened_at = None;
        self.half_open_requests = 0;
        tracing::info!("Circuit breaker closed: service recovered");
    }

    /// Get current failure rate
    pub fn failure_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.failure_count as f64 / self.total_requests as f64
        }
    }

    /// Reset circuit breaker (for testing or manual intervention)
    pub fn reset(&mut self) {
        self.transition_to_closed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let mut cb = CircuitBreaker::new(0.2, 5, 30, 3);
        
        // Record failures to trigger opening
        for _ in 0..2 {
            cb.record_success();
        }
        for _ in 0..4 {
            cb.record_failure();
        }
        
        // Should be open now (4/6 = 66% > 20%)
        assert_eq!(cb.state, CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_half_open_recovery() {
        let mut cb = CircuitBreaker::new(0.2, 5, 0, 3);
        
        // Open the circuit
        for _ in 0..6 {
            cb.record_failure();
        }
        assert_eq!(cb.state, CircuitState::Open);
        
        // Allow request should transition to half-open
        assert!(cb.allow_request());
        assert_eq!(cb.state, CircuitState::HalfOpen);
        
        // Record successes to close
        for _ in 0..3 {
            cb.record_success();
        }
        assert_eq!(cb.state, CircuitState::Closed);
    }
}
