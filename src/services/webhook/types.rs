use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Webhook event types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    SwapCreated,
    SwapPending,
    SwapProcessing,
    SwapCompleted,
    SwapFailed,
    SwapExpired,
    PayoutInitiated,
    PayoutCompleted,
    PayoutFailed,
}

impl WebhookEvent {
    pub fn as_str(&self) -> &str {
        match self {
            Self::SwapCreated => "swap.created",
            Self::SwapPending => "swap.pending",
            Self::SwapProcessing => "swap.processing",
            Self::SwapCompleted => "swap.completed",
            Self::SwapFailed => "swap.failed",
            Self::SwapExpired => "swap.expired",
            Self::PayoutInitiated => "payout.initiated",
            Self::PayoutCompleted => "payout.completed",
            Self::PayoutFailed => "payout.failed",
        }
    }
}

/// Webhook registration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Webhook {
    pub id: Uuid,
    pub swap_id: Uuid,
    pub url: String,
    pub secret_key: String,
    pub events: Vec<String>,
    pub enabled: bool,
    pub rate_limit_per_second: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Webhook payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPayload {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub created_at: i64,
    pub data: serde_json::Value,
}

/// Webhook delivery attempt
#[derive(Debug, Clone)]
pub struct WebhookDelivery {
    pub id: Uuid,
    pub webhook_id: Uuid,
    pub swap_id: Uuid,
    pub event_type: String,
    pub idempotency_key: String,
    pub payload: serde_json::Value,
    pub signature: String,
    pub attempt_number: i32,
    pub max_attempts: i32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub response_status: Option<i32>,
    pub response_body: Option<String>,
    pub response_time_ms: Option<i32>,
    pub error_message: Option<String>,
    pub is_dlq: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Webhook delivery status
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryStatus {
    Success,
    Failure,
    Timeout,
    CircuitOpen,
    RateLimited,
}

/// Idempotency check result
#[derive(Debug, Clone, PartialEq)]
pub enum IdempotencyStatus {
    New,
    AlreadyDelivered(i32), // response_status
    InProgress,
}

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Timestamp too old")]
    TimestampTooOld,
    #[error("Circuit breaker open")]
    CircuitBreakerOpen,
    #[error("Rate limited")]
    RateLimited,
    #[error("Network error: {0}")]
    Network(String),
    #[error("Timeout")]
    Timeout,
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}
