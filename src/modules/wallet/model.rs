use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// =============================================================================
// DATABASE MODELS
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SwapAddressInfo {
    pub swap_id: String,
    pub our_address: String,
    pub address_index: u32,
    pub blockchain_id: i32,
    pub coin_type: i32,
    pub recipient_address: String,
    pub recipient_extra_id: Option<String>,
    pub commission_rate: f64,
    pub payout_tx_hash: Option<String>,
    pub payout_amount: Option<f64>,
    pub actual_received: Option<f64>,
    pub commission_taken: Option<f64>,
    pub network_fee_paid: Option<f64>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub signed_at: Option<DateTime<Utc>>,
    pub broadcast_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub last_balance_check: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PayoutFeeQuote {
    pub platform_fee: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingDeposit {
    pub address: String,
    pub balance: f64,
    pub swap_id: String,
    pub chain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PayoutAuditEntry {
    pub id: i64,
    pub swap_id: String,
    pub status: String,
    pub message: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AddressUsageTracking {
    pub id: i64,
    pub address: String,
    pub swap_id: String,
    pub blockchain_id: i32,
    pub used_count: i32,
    pub created_at: DateTime<Utc>,
}

// =============================================================================
// ENUMS
// =============================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum PayoutStatus {
    Pending,
    Success,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum TxStatus {
    Pending,
    Confirmed,
    Failed,
    NotFound,
}

// =============================================================================
// INTERNAL HELPERS
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivateKey {
    pub key: String,
    pub chain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivationPath {
    pub coin_type: i32,
    pub account: i32,
    pub change: i32,
    pub index: u32,
}

impl DerivationPath {
    pub fn bip44_string(&self) -> String {
        format!(
            "m/44'/{}'/{}'/{}/{}",
            self.coin_type, self.account, self.change, self.index
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainInfo {
    pub id: i32,
    pub name: String,
    pub coin_type: i32,
    pub rpc_url: String,
}
