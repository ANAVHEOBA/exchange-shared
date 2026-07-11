use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

fn validate_non_empty(value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        Err(ValidationError::new("empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct AdminLoginRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminUserResponse {
    pub id: String,
    pub email: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminLoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: &'static str,
    pub expires_in: i64,
    pub admin: AdminUserResponse,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct AdminSwapExportQuery {
    pub provider: Option<String>,
    pub provider_swap_id: Option<String>,
    pub status: Option<String>,
    pub from_currency: Option<String>,
    pub from_network: Option<String>,
    pub to_currency: Option<String>,
    pub to_network: Option<String>,
    pub user_id: Option<String>,
    pub client_id: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub is_sandbox: Option<bool>,
    pub is_payment: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminErrorResponse {
    pub error: String,
}

impl AdminErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminOverviewSwapMetrics {
    pub open: u64,
    pub failed_last_24h: u64,
    pub refunded_last_24h: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminOverviewWhatsAppMetrics {
    pub open_conversations: u64,
    pub giftcard_sell_leads: u64,
    pub waiting_user: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminOverviewResponse {
    pub swaps: AdminOverviewSwapMetrics,
    pub whatsapp: AdminOverviewWhatsAppMetrics,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsSearchQuery {
    #[param(example = "wisdomvolt@gmail.com")]
    pub q: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSearchSwapResult {
    pub id: String,
    pub provider: String,
    pub provider_swap_id: Option<String>,
    pub status: String,
    pub from_currency: String,
    pub from_network: String,
    pub to_currency: String,
    pub to_network: String,
    pub amount: f64,
    pub estimated_receive: f64,
    pub client_id: Option<String>,
    pub user_id: Option<String>,
    pub tx_hash_in: Option<String>,
    pub tx_hash_out: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSearchGiftCardResult {
    pub id: String,
    pub trade_id: Option<String>,
    pub order_kind: String,
    pub product_id: Option<String>,
    pub prepaid_provider: Option<String>,
    pub currency_code: Option<String>,
    pub recipient_email_masked: String,
    pub status: String,
    pub provider_status: Option<String>,
    pub provider: Option<String>,
    pub provider_trade_id: Option<String>,
    pub source_ticker: String,
    pub source_network: String,
    pub amount: f64,
    pub amount_to: Option<f64>,
    pub client_id: Option<String>,
    pub user_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSearchSupportResult {
    pub wa_id: String,
    pub status: String,
    pub tag: Option<String>,
    pub assigned_to: Option<String>,
    pub state: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSearchResponse {
    pub query: String,
    pub swaps: Vec<OpsSearchSwapResult>,
    pub giftcards: Vec<OpsSearchGiftCardResult>,
    pub support: Vec<OpsSearchSupportResult>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsProviderHealthRow {
    pub provider: String,
    pub open_swaps: u64,
    pub failed_swaps_24h: u64,
    pub giftcard_active: u64,
    pub giftcard_failed_24h: u64,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsWorkerHealth {
    pub giftcard_queued: u64,
    pub giftcard_retry_pending: u64,
    pub giftcard_creating: u64,
    pub giftcard_stale_active: u64,
    pub swap_polling_due: u64,
    pub swap_polling_stale: u64,
    pub webhook_retry_due: u64,
    pub webhook_dead_letters: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsRiskFlag {
    pub entity_type: String,
    pub entity_id: String,
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsHealthResponse {
    pub generated_at: String,
    pub worker: OpsWorkerHealth,
    pub providers: Vec<OpsProviderHealthRow>,
    pub risk_flags: Vec<OpsRiskFlag>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsFinanceQuery {
    pub date_from: Option<String>,
    pub date_to: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsFinanceTotals {
    pub completed_swaps: u64,
    pub failed_swaps: u64,
    pub expired_swaps: u64,
    pub swap_volume_input: f64,
    pub swap_platform_fees: f64,
    pub swap_provider_fees: f64,
    pub giftcard_completed: u64,
    pub giftcard_failed: u64,
    pub giftcard_volume: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsFinanceDailyRow {
    pub date: String,
    pub completed_swaps: u64,
    pub failed_swaps: u64,
    pub swap_volume_input: f64,
    pub swap_platform_fees: f64,
    pub giftcard_completed: u64,
    pub giftcard_volume: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsFinanceProviderRow {
    pub provider: String,
    pub swaps: u64,
    pub completed_swaps: u64,
    pub failed_swaps: u64,
    pub volume_input: f64,
    pub platform_fees: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsFinanceResponse {
    pub generated_at: String,
    pub totals: OpsFinanceTotals,
    pub daily: Vec<OpsFinanceDailyRow>,
    pub providers: Vec<OpsFinanceProviderRow>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsWebhookDeliveryRow {
    pub id: String,
    pub swap_id: String,
    pub event_type: String,
    pub attempt_number: i32,
    pub max_attempts: i32,
    pub next_retry_at: Option<String>,
    pub delivered_at: Option<String>,
    pub response_status: Option<i32>,
    pub response_time_ms: Option<i32>,
    pub error_message: Option<String>,
    pub is_dlq: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsWebhookMonitorResponse {
    pub deliveries: Vec<OpsWebhookDeliveryRow>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct OpsCreateNoteRequest {
    #[validate(length(min = 1, max = 32), custom(function = "validate_non_empty"))]
    pub entity_type: String,
    #[validate(length(min = 1, max = 120), custom(function = "validate_non_empty"))]
    pub entity_id: String,
    #[validate(length(min = 1, max = 5000), custom(function = "validate_non_empty"))]
    pub note: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsNoteResponse {
    pub id: u64,
    pub entity_type: String,
    pub entity_id: String,
    pub admin_email: String,
    pub note: String,
    pub created_at: String,
}
