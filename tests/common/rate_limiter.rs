use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration};
use std::sync::Mutex;
use lazy_static::lazy_static;

/// Global test rate limiter to prevent overwhelming Trocador API
/// Uses token bucket algorithm with automatic backoff
pub struct TestRateLimiter {
    /// Semaphore to limit concurrent API calls
    semaphore: Arc<Semaphore>,
    /// Last API call timestamp
    last_call: Arc<Mutex<Option<std::time::Instant>>>,
    /// Minimum delay between calls (milliseconds)
    min_delay_ms: u64,
}

impl TestRateLimiter {
    /// Create a new rate limiter
    /// - max_concurrent: Maximum number of simultaneous API calls
    /// - min_delay_ms: Minimum milliseconds between consecutive calls
    pub fn new(max_concurrent: usize, min_delay_ms: u64) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            last_call: Arc::new(Mutex::new(None)),
            min_delay_ms,
        }
    }

    /// Acquire permission to make an API call
    /// Automatically throttles to respect rate limits
    pub async fn acquire(&self) -> RateLimitGuard {
        // Wait for available slot
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();

        // Enforce minimum delay between calls
        let mut last_call = self.last_call.lock().unwrap();
        
        if let Some(last) = *last_call {
            let elapsed = last.elapsed();
            let elapsed_ms = elapsed.as_millis() as u64;
            
            if elapsed_ms < self.min_delay_ms {
                let wait_ms = self.min_delay_ms - elapsed_ms;
                drop(last_call); // Release lock before sleeping
                sleep(Duration::from_millis(wait_ms)).await;
                last_call = self.last_call.lock().unwrap();
            }
        }

        // Update last call timestamp
        *last_call = Some(std::time::Instant::now());
        drop(last_call);

        RateLimitGuard { _permit: permit }
    }
}

/// RAII guard that releases the rate limit permit when dropped
pub struct RateLimitGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

// Global rate limiter instance for all tests
lazy_static! {
    /// Global rate limiter for Trocador API calls
    /// Limits to 1 concurrent call with 1000ms (1 second) minimum delay
    /// This prevents 429 rate limit errors during test runs
    /// 
    /// Trocador's rate limit appears to be around 60 requests per minute
    /// So we use 1 second delay = 60 requests/minute (safe limit)
    pub static ref TROCADOR_RATE_LIMITER: TestRateLimiter = 
        TestRateLimiter::new(1, 1000);
}
