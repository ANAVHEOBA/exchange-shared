use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::services::redis_cache::RedisService;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBucket {
    pub tokens: u32,
    pub last_refill: u64,
    pub capacity: u32,
    pub refill_rate: u32, // tokens per second
}

impl TokenBucket {
    pub fn new(capacity: u32, refill_rate: u32) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        Self {
            tokens: capacity,
            last_refill: now,
            capacity,
            refill_rate,
        }
    }

    pub fn try_consume(&mut self, tokens: u32) -> bool {
        self.refill();
        
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let time_passed = now - self.last_refill;
        let tokens_to_add = (time_passed * self.refill_rate as u64) as u32;
        
        if tokens_to_add > 0 {
            self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
            self.last_refill = now;
        }
    }
}

pub struct DistributedRateLimiter {
    redis: RedisService,
    default_capacity: u32,
    default_refill_rate: u32,
}

impl DistributedRateLimiter {
    pub fn new(redis: RedisService) -> Self {
        Self {
            redis,
            default_capacity: 10, // 10 requests per bucket
            default_refill_rate: 1, // 1 token per second
        }
    }

    pub async fn try_acquire(&self, key: &str, tokens: u32) -> Result<bool, String> {
        let bucket_key = format!("rate_limit:{}", key);
        
        // Get current bucket state
        let mut bucket: TokenBucket = match self.redis.get_json(&bucket_key).await? {
            Some(bucket) => bucket,
            None => TokenBucket::new(self.default_capacity, self.default_refill_rate),
        };

        // Try to consume tokens
        let allowed = bucket.try_consume(tokens);
        
        // Save updated bucket state with TTL
        self.redis.set_json(&bucket_key, &bucket, 3600).await?;
        
        Ok(allowed)
    }

    pub async fn get_wait_time(&self, key: &str) -> Result<Duration, String> {
        let bucket_key = format!("rate_limit:{}", key);
        
        let bucket: TokenBucket = match self.redis.get_json::<TokenBucket>(&bucket_key).await? {
            Some(mut bucket) => {
                bucket.refill();
                bucket
            },
            None => return Ok(Duration::from_secs(0)),
        };

        if bucket.tokens > 0 {
            Ok(Duration::from_secs(0))
        } else {
            // Calculate time needed for next token
            let time_for_token = 1.0 / bucket.refill_rate as f64;
            Ok(Duration::from_secs_f64(time_for_token))
        }
    }
}
