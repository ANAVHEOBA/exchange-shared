use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use rust_decimal::Decimal;

/// Swap states for refund eligibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SwapStatus {
    Created,
    Pending,
    Processing,
    Completed,
    Failed,
    Expired,
    Refunding,
    Refunded,
    RefundFailed,
}

impl SwapStatus {
    /// Check if swap is in a refundable state
    pub fn is_refundable(&self) -> bool {
        matches!(self, Self::Failed | Self::Refunding | Self::RefundFailed)
    }
    
    /// Check if swap is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Refunded | Self::Expired)
    }
}

impl std::fmt::Display for SwapStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "CREATED"),
            Self::Pending => write!(f, "PENDING"),
            Self::Processing => write!(f, "PROCESSING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Expired => write!(f, "EXPIRED"),
            Self::Refunding => write!(f, "REFUNDING"),
            Self::Refunded => write!(f, "REFUNDED"),
            Self::RefundFailed => write!(f, "REFUND_FAILED"),
        }
    }
}

/// Timeout stage for progressive detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutStage {
    Deposit,      // Waiting for user deposit
    Processing,   // Provider processing swap
    Payout,       // Executing payout transaction
    Refund,       // Refund transaction pending
}

impl TimeoutStage {
    pub fn timeout_seconds(&self) -> u64 {
        match self {
            Self::Deposit => 1800,      // 30 minutes
            Self::Processing => 7200,   // 2 hours
            Self::Payout => 3600,       // 1 hour
            Self::Refund => 1800,       // 30 minutes
        }
    }
}

/// Refund transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RefundStatus {
    Pending,
    Processing,
    Submitted,
    Confirmed,
    Failed,
    Manual,
}

impl std::fmt::Display for RefundStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Processing => write!(f, "PROCESSING"),
            Self::Submitted => write!(f, "SUBMITTED"),
            Self::Confirmed => write!(f, "CONFIRMED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Manual => write!(f, "MANUAL"),
        }
    }
}

/// Refund record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refund {
    pub id: Uuid,
    pub swap_id: Uuid,
    pub idempotency_key: String,
    
    // Refund details
    pub refund_address: String,
    pub refund_amount: Decimal,
    pub refund_currency: String,
    pub refund_network: String,
    
    // Transaction details
    pub tx_hash: Option<String>,
    pub tx_status: RefundStatus,
    pub confirmations: u32,
    pub required_confirmations: u32,
    
    // Retry tracking
    pub attempt_number: u32,
    pub max_attempts: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    
    // Gas/fee tracking
    pub gas_price: Option<Decimal>,
    pub gas_used: Option<u64>,
    pub total_fee: Option<Decimal>,
    
    // Priority and status
    pub priority_score: f64,
    pub status: RefundStatus,
    
    // Audit trail
    pub initiated_by: String,
    pub initiated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    
    // Metadata
    pub failure_reason: Option<String>,
    pub metadata: Option<serde_json::Value>,
    
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Refund calculation result
#[derive(Debug, Clone)]
pub struct RefundCalculation {
    pub refund_amount: Decimal,
    pub deposit_amount: Decimal,
    pub fees_paid: Decimal,
    pub gas_cost_estimate: Decimal,
    pub is_economical: bool,
    pub reason: String,
}

/// Timeout detection result
#[derive(Debug, Clone)]
pub struct TimeoutDetection {
    pub swap_id: Uuid,
    pub stage: TimeoutStage,
    pub elapsed_seconds: u64,
    pub timeout_seconds: u64,
    pub is_timeout: bool,
    pub recommended_action: TimeoutAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutAction {
    Wait,
    QueryProvider,
    RetryPayout,
    InitiateRefund,
    EscalateManual,
}

/// Refund errors
#[derive(Debug, thiserror::Error)]
pub enum RefundError {
    #[error("Swap not found: {0}")]
    SwapNotFound(Uuid),
    
    #[error("Swap not refundable: {0}")]
    NotRefundable(String),
    
    #[error("Refund amount too small: {0}")]
    AmountTooSmall(String),
    
    #[error("Duplicate refund: {0}")]
    DuplicateRefund(String),
    
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    
    #[error("Insufficient balance: {0}")]
    InsufficientBalance(String),
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    
    #[error("Wallet error: {0}")]
    Wallet(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_swap_status_is_refundable() {
        assert!(SwapStatus::Failed.is_refundable());
        assert!(SwapStatus::Refunding.is_refundable());
        assert!(SwapStatus::RefundFailed.is_refundable());
        
        assert!(!SwapStatus::Created.is_refundable());
        assert!(!SwapStatus::Completed.is_refundable());
    }
    
    #[test]
    fn test_swap_status_is_terminal() {
        assert!(SwapStatus::Completed.is_terminal());
        assert!(SwapStatus::Refunded.is_terminal());
        assert!(SwapStatus::Expired.is_terminal());
        
        assert!(!SwapStatus::Pending.is_terminal());
        assert!(!SwapStatus::Processing.is_terminal());
    }
    
    #[test]
    fn test_timeout_stage_seconds() {
        assert_eq!(TimeoutStage::Deposit.timeout_seconds(), 1800);
        assert_eq!(TimeoutStage::Processing.timeout_seconds(), 7200);
        assert_eq!(TimeoutStage::Payout.timeout_seconds(), 3600);
        assert_eq!(TimeoutStage::Refund.timeout_seconds(), 1800);
    }
}
