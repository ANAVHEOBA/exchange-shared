use reqwest::Client;
use std::time::{Duration, Instant};

use crate::services::webhook::{
    WebhookError, DeliveryStatus, RetryConfig, WebhookPayload,
    generate_signature,
};

/// Webhook delivery client
pub struct WebhookDeliveryClient {
    client: Client,
    retry_config: RetryConfig,
}

impl WebhookDeliveryClient {
    pub fn new(retry_config: RetryConfig) -> Self {
        let client = Client::builder()
            .timeout(retry_config.timeout())
            .build()
            .unwrap_or_default();
        
        Self {
            client,
            retry_config,
        }
    }
    
    /// Deliver webhook to endpoint
    pub async fn deliver(
        &self,
        url: &str,
        secret_key: &str,
        payload: &WebhookPayload,
    ) -> Result<DeliveryResult, WebhookError> {
        let start = Instant::now();
        
        // Serialize payload
        let payload_json = serde_json::to_string(payload)?;
        
        // Generate signature
        let timestamp = payload.created_at;
        let signature = generate_signature(secret_key, timestamp, &payload_json);
        
        // Build request
        let request = self.client
            .post(url)
            .header("Content-Type", "application/json")
            .header("X-Webhook-Signature", &signature)
            .header("X-Webhook-Id", &payload.id)
            .header("X-Webhook-Timestamp", timestamp.to_string())
            .header("User-Agent", "ExchangePlatform-Webhooks/1.0")
            .body(payload_json)
            .timeout(self.retry_config.timeout());
        
        // Send request
        let response = match request.send().await {
            Ok(resp) => resp,
            Err(e) => {
                let duration = start.elapsed();
                
                if e.is_timeout() {
                    return Ok(DeliveryResult {
                        status: DeliveryStatus::Timeout,
                        response_status: None,
                        response_body: None,
                        duration,
                        error_message: Some("Request timeout".to_string()),
                    });
                }
                
                return Ok(DeliveryResult {
                    status: DeliveryStatus::Failure,
                    response_status: None,
                    response_body: None,
                    duration,
                    error_message: Some(e.to_string()),
                });
            }
        };
        
        let duration = start.elapsed();
        let status_code = response.status().as_u16();
        let response_body = response.text().await.ok();
        
        // Determine delivery status
        let status = if status_code >= 200 && status_code < 300 {
            DeliveryStatus::Success
        } else {
            DeliveryStatus::Failure
        };
        
        Ok(DeliveryResult {
            status,
            response_status: Some(status_code as i32),
            response_body,
            duration,
            error_message: None,
        })
    }
}

impl Default for WebhookDeliveryClient {
    fn default() -> Self {
        Self::new(RetryConfig::default())
    }
}

/// Result of webhook delivery attempt
#[derive(Debug, Clone)]
pub struct DeliveryResult {
    pub status: DeliveryStatus,
    pub response_status: Option<i32>,
    pub response_body: Option<String>,
    pub duration: Duration,
    pub error_message: Option<String>,
}

impl DeliveryResult {
    pub fn is_success(&self) -> bool {
        matches!(self.status, DeliveryStatus::Success)
    }
    
    pub fn is_retryable(&self) -> bool {
        matches!(
            self.status,
            DeliveryStatus::Failure | DeliveryStatus::Timeout
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::webhook::WebhookEvent;

    #[tokio::test]
    async fn test_delivery_client_creation() {
        let client = WebhookDeliveryClient::default();
        assert_eq!(client.retry_config.timeout_secs, 30);
    }

    #[test]
    fn test_delivery_result_is_success() {
        let result = DeliveryResult {
            status: DeliveryStatus::Success,
            response_status: Some(200),
            response_body: None,
            duration: Duration::from_millis(100),
            error_message: None,
        };
        
        assert!(result.is_success());
        assert!(!result.is_retryable());
    }

    #[test]
    fn test_delivery_result_is_retryable() {
        let result = DeliveryResult {
            status: DeliveryStatus::Failure,
            response_status: Some(500),
            response_body: None,
            duration: Duration::from_millis(100),
            error_message: Some("Server error".to_string()),
        };
        
        assert!(!result.is_success());
        assert!(result.is_retryable());
    }

    #[test]
    fn test_delivery_result_timeout() {
        let result = DeliveryResult {
            status: DeliveryStatus::Timeout,
            response_status: None,
            response_body: None,
            duration: Duration::from_secs(30),
            error_message: Some("Request timeout".to_string()),
        };
        
        assert!(!result.is_success());
        assert!(result.is_retryable());
    }
}
