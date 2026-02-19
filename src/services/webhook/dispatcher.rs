use sqlx::MySqlPool;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use chrono::Utc;
use uuid::Uuid;

use crate::services::webhook::{
    Webhook, WebhookPayload, WebhookError,
    WebhookDeliveryClient, RetryConfig, WebhookCircuitBreaker,
    TokenBucketRateLimiter, IdempotencyStatus,
};

/// Webhook dispatcher manages webhook delivery with retry logic
pub struct WebhookDispatcher {
    pool: MySqlPool,
    client: WebhookDeliveryClient,
    retry_config: RetryConfig,
    circuit_breakers: Arc<RwLock<HashMap<Uuid, WebhookCircuitBreaker>>>,
    rate_limiters: Arc<RwLock<HashMap<Uuid, TokenBucketRateLimiter>>>,
}

impl WebhookDispatcher {
    pub fn new(pool: MySqlPool, retry_config: RetryConfig) -> Self {
        Self {
            pool,
            client: WebhookDeliveryClient::new(retry_config.clone()),
            retry_config,
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            rate_limiters: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Dispatch webhook for given event
    pub async fn dispatch(
        &self,
        webhook: &Webhook,
        payload: WebhookPayload,
    ) -> Result<(), WebhookError> {
        // Check if webhook is enabled
        if !webhook.enabled {
            return Ok(());
        }
        
        // Generate idempotency key
        let idempotency_key = self.generate_idempotency_key(
            &webhook.swap_id,
            &payload.event_type,
            payload.created_at,
        );
        
        // Check idempotency
        match self.check_idempotency(&idempotency_key).await? {
            IdempotencyStatus::AlreadyDelivered(_) => {
                tracing::info!("Webhook already delivered: {}", idempotency_key);
                return Ok(());
            }
            IdempotencyStatus::InProgress => {
                tracing::warn!("Webhook delivery in progress: {}", idempotency_key);
                return Ok(());
            }
            IdempotencyStatus::New => {
                // Continue with delivery
            }
        }
        
        // Check circuit breaker
        {
            let mut breakers = self.circuit_breakers.write().await;
            let breaker = breakers
                .entry(webhook.id)
                .or_insert_with(WebhookCircuitBreaker::default);
            
            if !breaker.check_and_allow() {
                tracing::warn!("Circuit breaker open for webhook: {}", webhook.id);
                return Err(WebhookError::CircuitBreakerOpen);
            }
        }
        
        // Check rate limiter
        {
            let mut limiters = self.rate_limiters.write().await;
            let limiter = limiters
                .entry(webhook.id)
                .or_insert_with(|| {
                    TokenBucketRateLimiter::new(
                        100.0,
                        webhook.rate_limit_per_second as f64,
                    )
                });
            
            if !limiter.allow_request() {
                tracing::warn!("Rate limited for webhook: {}", webhook.id);
                return Err(WebhookError::RateLimited);
            }
        }
        
        // Create delivery record
        let delivery_id = self.create_delivery_record(
            webhook.id,
            webhook.swap_id,
            &payload,
            &idempotency_key,
        ).await?;
        
        // Attempt delivery
        let result = self.client.deliver(&webhook.url, &webhook.secret_key, &payload).await?;
        
        // Update circuit breaker
        {
            let mut breakers = self.circuit_breakers.write().await;
            if let Some(breaker) = breakers.get_mut(&webhook.id) {
                if result.is_success() {
                    breaker.record_success();
                } else {
                    breaker.record_failure();
                }
            }
        }
        
        // Update delivery record
        if result.is_success() {
            self.mark_delivered(
                delivery_id,
                result.response_status,
                result.response_body.as_deref(),
                result.duration.as_millis() as i32,
            ).await?;
        } else if result.is_retryable() {
            // Schedule retry
            let next_retry = self.calculate_next_retry(0);
            self.schedule_retry(
                delivery_id,
                result.error_message.as_deref(),
                next_retry,
            ).await?;
        } else {
            // Move to DLQ
            self.move_to_dlq(
                delivery_id,
                result.error_message.as_deref(),
            ).await?;
        }
        
        Ok(())
    }
    
    /// Process retry queue
    pub async fn process_retries(&self) -> Result<usize, WebhookError> {
        let pending = self.get_pending_retries().await?;
        let mut processed = 0;
        
        for (delivery_id, webhook_id, attempt_number) in pending {
            // Get webhook
            let webhook = match self.get_webhook(webhook_id).await? {
                Some(w) => w,
                None => continue,
            };
            
            // Get delivery
            let delivery = match self.get_delivery(delivery_id).await? {
                Some(d) => d,
                None => continue,
            };
            
            // Reconstruct payload
            let payload: WebhookPayload = serde_json::from_value(delivery.payload)?;
            
            // Attempt delivery
            let result = self.client.deliver(&webhook.url, &webhook.secret_key, &payload).await?;
            
            // Update based on result
            if result.is_success() {
                self.mark_delivered(
                    delivery_id,
                    result.response_status,
                    result.response_body.as_deref(),
                    result.duration.as_millis() as i32,
                ).await?;
            } else if attempt_number < self.retry_config.max_attempts as i32 {
                // Schedule next retry
                let next_retry = self.calculate_next_retry(attempt_number as u32 + 1);
                self.schedule_retry(
                    delivery_id,
                    result.error_message.as_deref(),
                    next_retry,
                ).await?;
            } else {
                // Exhausted retries, move to DLQ
                self.move_to_dlq(
                    delivery_id,
                    result.error_message.as_deref(),
                ).await?;
            }
            
            processed += 1;
        }
        
        Ok(processed)
    }
    
    fn generate_idempotency_key(&self, swap_id: &Uuid, event_type: &str, timestamp: i64) -> String {
        use sha2::{Sha256, Digest};
        let message = format!("{}.{}.{}", swap_id, event_type, timestamp);
        let hash = Sha256::digest(message.as_bytes());
        hex::encode(hash)
    }
    
    async fn check_idempotency(&self, key: &str) -> Result<IdempotencyStatus, WebhookError> {
        let result: Option<(Option<chrono::NaiveDateTime>, Option<i32>)> = sqlx::query_as(
            "SELECT delivered_at, response_status FROM webhook_deliveries WHERE idempotency_key = ?"
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        
        match result {
            None => Ok(IdempotencyStatus::New),
            Some((delivered_at, response_status)) => {
                if delivered_at.is_some() {
                    Ok(IdempotencyStatus::AlreadyDelivered(response_status.unwrap_or(0)))
                } else {
                    Ok(IdempotencyStatus::InProgress)
                }
            }
        }
    }
    
    async fn create_delivery_record(
        &self,
        webhook_id: Uuid,
        swap_id: Uuid,
        payload: &WebhookPayload,
        idempotency_key: &str,
    ) -> Result<Uuid, WebhookError> {
        let payload_json = serde_json::to_value(payload)?;
        let signature = ""; // Will be generated during delivery
        let id = Uuid::new_v4();
        
        sqlx::query!(
            r#"
            INSERT INTO webhook_deliveries (
                id, webhook_id, swap_id, event_type, idempotency_key,
                payload, signature, attempt_number, max_attempts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
            "#,
            id.to_string(),
            webhook_id.to_string(),
            swap_id.to_string(),
            payload.event_type,
            idempotency_key,
            payload_json,
            signature,
            self.retry_config.max_attempts as i32
        )
        .execute(&self.pool)
        .await?;
        
        Ok(id)
    }
    
    async fn mark_delivered(
        &self,
        delivery_id: Uuid,
        response_status: Option<i32>,
        response_body: Option<&str>,
        response_time_ms: i32,
    ) -> Result<(), WebhookError> {
        sqlx::query!(
            r#"
            UPDATE webhook_deliveries
            SET delivered_at = NOW(),
                response_status = ?,
                response_body = ?,
                response_time_ms = ?,
                updated_at = NOW()
            WHERE id = ?
            "#,
            response_status,
            response_body,
            response_time_ms,
            delivery_id.to_string()
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    async fn schedule_retry(
        &self,
        delivery_id: Uuid,
        error_message: Option<&str>,
        next_retry: chrono::DateTime<Utc>,
    ) -> Result<(), WebhookError> {
        sqlx::query!(
            r#"
            UPDATE webhook_deliveries
            SET attempt_number = attempt_number + 1,
                next_retry_at = ?,
                error_message = ?,
                updated_at = NOW()
            WHERE id = ?
            "#,
            next_retry,
            error_message,
            delivery_id.to_string()
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    async fn move_to_dlq(
        &self,
        delivery_id: Uuid,
        error_message: Option<&str>,
    ) -> Result<(), WebhookError> {
        sqlx::query!(
            r#"
            UPDATE webhook_deliveries
            SET is_dlq = true,
                error_message = ?,
                updated_at = NOW()
            WHERE id = ?
            "#,
            error_message,
            delivery_id.to_string()
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    fn calculate_next_retry(&self, attempt: u32) -> chrono::DateTime<Utc> {
        let delay = self.retry_config.calculate_delay(attempt);
        Utc::now() + chrono::Duration::from_std(delay).unwrap()
    }
    
    async fn get_pending_retries(&self) -> Result<Vec<(Uuid, Uuid, i32)>, WebhookError> {
        let results = sqlx::query!(
            r#"
            SELECT id, webhook_id, attempt_number
            FROM webhook_deliveries
            WHERE next_retry_at IS NOT NULL
              AND next_retry_at <= NOW()
              AND delivered_at IS NULL
              AND is_dlq = false
            ORDER BY next_retry_at
            LIMIT 100
            "#
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(results
            .into_iter()
            .map(|r| (
                Uuid::parse_str(&r.id).unwrap(),
                Uuid::parse_str(&r.webhook_id).unwrap(),
                r.attempt_number
            ))
            .collect())
    }
    
    async fn get_webhook(&self, webhook_id: Uuid) -> Result<Option<Webhook>, WebhookError> {
        let result = sqlx::query!(
            r#"
            SELECT id, swap_id, url, secret_key, events, enabled,
                   rate_limit_per_second, created_at, updated_at
            FROM webhooks
            WHERE id = ?
            "#,
            webhook_id.to_string()
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(result.map(|r| {
            let events: Vec<String> = serde_json::from_value(r.events).unwrap_or_default();
            Webhook {
                id: Uuid::parse_str(&r.id).unwrap(),
                swap_id: Uuid::parse_str(&r.swap_id).unwrap(),
                url: r.url,
                secret_key: r.secret_key,
                events,
                enabled: r.enabled.map(|e| e != 0).unwrap_or(false),
                rate_limit_per_second: r.rate_limit_per_second.unwrap_or(10),
                created_at: r.created_at.unwrap_or_else(Utc::now),
                updated_at: r.updated_at.unwrap_or_else(Utc::now),
            }
        }))
    }
    
    async fn get_delivery(&self, delivery_id: Uuid) -> Result<Option<DeliveryRecord>, WebhookError> {
        let result = sqlx::query!(
            r#"
            SELECT id, webhook_id, swap_id, event_type, payload, attempt_number
            FROM webhook_deliveries
            WHERE id = ?
            "#,
            delivery_id.to_string()
        )
        .fetch_optional(&self.pool)
        .await?;
        
        Ok(result.map(|r| DeliveryRecord {
            id: Uuid::parse_str(&r.id).unwrap(),
            webhook_id: Uuid::parse_str(&r.webhook_id).unwrap(),
            swap_id: Uuid::parse_str(&r.swap_id).unwrap(),
            event_type: r.event_type,
            payload: r.payload,
            attempt_number: r.attempt_number,
        }))
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct DeliveryRecord {
    id: Uuid,
    webhook_id: Uuid,
    swap_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    attempt_number: i32,
}
