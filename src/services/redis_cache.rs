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
}
