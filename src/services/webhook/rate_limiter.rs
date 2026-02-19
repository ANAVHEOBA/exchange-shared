use std::time::Instant;

/// Token bucket rate limiter
#[derive(Debug, Clone)]
pub struct TokenBucketRateLimiter {
    pub tokens_available: f64,
    pub capacity: f64,
    pub refill_rate: f64, // tokens per second
    pub last_refill_at: Instant,
}

impl TokenBucketRateLimiter {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            tokens_available: capacity,
            capacity,
            refill_rate,
            last_refill_at: Instant::now(),
        }
    }
    
    /// Try to consume a token, returns true if allowed
    pub fn allow_request(&mut self) -> bool {
        self.refill();
        
        if self.tokens_available >= 1.0 {
            self.tokens_available -= 1.0;
            true
        } else {
            false
        }
    }
    
    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill_at).as_secs_f64();
        
        let tokens_to_add = elapsed * self.refill_rate;
        self.tokens_available = (self.tokens_available + tokens_to_add).min(self.capacity);
        self.last_refill_at = now;
    }
    
    /// Get current token count
    pub fn available_tokens(&mut self) -> f64 {
        self.refill();
        self.tokens_available
    }
    
    /// Reset to full capacity
    pub fn reset(&mut self) {
        self.tokens_available = self.capacity;
        self.last_refill_at = Instant::now();
    }
}

impl Default for TokenBucketRateLimiter {
    fn default() -> Self {
        Self::new(
            100.0, // 100 token capacity (burst)
            10.0,  // 10 tokens per second (600/minute)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rate_limiter_initial_capacity() {
        let mut limiter = TokenBucketRateLimiter::new(100.0, 10.0);
        
        // Should have full capacity initially
        assert_eq!(limiter.available_tokens(), 100.0);
    }

    #[test]
    fn test_rate_limiter_consume_tokens() {
        let mut limiter = TokenBucketRateLimiter::new(10.0, 10.0);
        
        // Consume 5 tokens
        for _ in 0..5 {
            assert!(limiter.allow_request());
        }
        
        // Should have approximately 5 tokens left (allow for floating point precision)
        let available = limiter.available_tokens();
        assert!((available - 5.0).abs() < 0.01, "Expected ~5.0 tokens, got {}", available);
    }

    #[test]
    fn test_rate_limiter_exhaustion() {
        let mut limiter = TokenBucketRateLimiter::new(5.0, 10.0);
        
        // Consume all tokens
        for _ in 0..5 {
            assert!(limiter.allow_request());
        }
        
        // Next request should be denied
        assert!(!limiter.allow_request());
    }

    #[test]
    fn test_rate_limiter_refill() {
        let mut limiter = TokenBucketRateLimiter::new(100.0, 10.0);
        
        // Consume 50 tokens
        for _ in 0..50 {
            limiter.allow_request();
        }
        
        // Wait 1 second (should refill 10 tokens)
        thread::sleep(Duration::from_secs(1));
        
        let available = limiter.available_tokens();
        assert!(available >= 59.0 && available <= 61.0, "Expected ~60 tokens, got {}", available);
    }

    #[test]
    fn test_rate_limiter_cap() {
        let mut limiter = TokenBucketRateLimiter::new(100.0, 10.0);
        
        // Wait 20 seconds (would refill 200 tokens, but capped at 100)
        thread::sleep(Duration::from_millis(2100));
        
        let available = limiter.available_tokens();
        assert!(available <= 100.0, "Tokens should be capped at capacity");
    }

    #[test]
    fn test_rate_limiter_reset() {
        let mut limiter = TokenBucketRateLimiter::new(100.0, 10.0);
        
        // Consume all tokens
        for _ in 0..100 {
            limiter.allow_request();
        }
        
        let available = limiter.available_tokens();
        assert!(available < 0.01, "Expected ~0.0 tokens, got {}", available);
        
        // Reset
        limiter.reset();
        
        assert_eq!(limiter.available_tokens(), 100.0);
    }

    #[test]
    fn test_rate_limiter_burst() {
        let mut limiter = TokenBucketRateLimiter::new(100.0, 10.0);
        
        // Should allow burst of 100 requests
        for i in 0..100 {
            assert!(limiter.allow_request(), "Request {} should be allowed", i);
        }
        
        // 101st request should be denied
        assert!(!limiter.allow_request());
    }
}
