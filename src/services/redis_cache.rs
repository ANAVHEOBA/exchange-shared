use redis::{AsyncCommands, Client};
use serde::{de::DeserializeOwned, Serialize};

#[derive(Clone)]
pub struct RedisService {
    client: Client,
}

impl RedisService {
    pub fn new(redis_url: &str) -> Self {
        let client = Client::open(redis_url).expect("Invalid Redis URL");
        Self { client }
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub async fn set_json<T: Serialize>(&self, key: &str, value: &T, ttl_seconds: u64) -> Result<(), String> {
        let json = serde_json::to_string(value).map_err(|e| e.to_string())?;
        
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        
        conn.set_ex(key, json, ttl_seconds)
            .await
            .map_err(|e: redis::RedisError| e.to_string())
    }

    pub async fn get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, String> {
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
            
        let result: Option<String> = conn.get(key)
            .await
            .map_err(|e: redis::RedisError| e.to_string())?;

        match result {
            Some(json) => serde_json::from_str(&json).map(Some).map_err(|e| e.to_string()),
            None => Ok(None),
        }
    }

    // Rate limiting with simple counter
    pub async fn check_rate_limit(&self, key: &str, limit: u32, window_seconds: u64) -> Result<bool, String> {
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;

        let count: u32 = conn.get(key)
            .await
            .unwrap_or(0);
        
        if count < limit {
            let _: () = conn.incr(key, 1)
                .await
                .map_err(|e: redis::RedisError| e.to_string())?;
            
            let _: () = conn.expire(key, window_seconds as i64)
                .await
                .map_err(|e: redis::RedisError| e.to_string())?;
            
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // Distributed Lock: Set key only if it doesn't exist
    pub async fn try_lock(&self, key: &str, ttl_seconds: u64) -> Result<bool, String> {
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;

        // SET key value NX EX ttl
        // Returns OK if set, Null if not set
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg("locked")
            .arg("NX")
            .arg("EX")
            .arg(ttl_seconds)
            .query_async(&mut conn)
            .await
            .map_err(|e: redis::RedisError| e.to_string())?;

        Ok(result.is_some())
    }

    pub async fn set_string(&self, key: &str, value: &str, ttl_seconds: u64) -> Result<(), String> {
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
        
        conn.set_ex(key, value, ttl_seconds)
            .await
            .map_err(|e: redis::RedisError| e.to_string())
    }

    pub async fn get_string(&self, key: &str) -> Result<Option<String>, String> {
        let mut conn = self.client.get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;
            
        let result: Option<String> = conn.get(key)
            .await
            .map_err(|e: redis::RedisError| e.to_string())?;

        Ok(result)
    }

    // Cache with deduplication
    pub async fn get_or_set_json<T, F, Fut>(&self, key: &str, ttl_seconds: u64, fetch_fn: F) -> Result<T, String>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        // Try to get from cache first
        if let Some(cached) = self.get_json::<T>(key).await? {
            return Ok(cached);
        }

        // Not in cache, fetch and store
        let data = fetch_fn().await?;
        self.set_json(key, &data, ttl_seconds).await?;
        Ok(data)
    }
}
