use crate::modules::giftcard::schema::GiftCardProductResponse;
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

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardResponse {
    pub generated_at: String,
    pub summary: AdminOverviewResponse,
    pub kpis: OpsDashboardKpis,
    pub status_breakdown: OpsDashboardStatusBreakdown,
    pub quick_access: Vec<OpsDashboardQuickAccessItem>,
    pub recent_activity: Vec<OpsDashboardRecentActivityItem>,
    pub volume_trend: Vec<OpsDashboardVolumePoint>,
    pub top_pairs: Vec<OpsDashboardTopPair>,
    pub top_giftcards: Vec<OpsDashboardTopGiftCard>,
    pub worker: OpsWorkerHealth,
    pub providers: Vec<OpsProviderHealthRow>,
    pub risk_flags: Vec<OpsRiskFlag>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardKpis {
    pub total_swap_volume: f64,
    pub total_giftcard_sales: f64,
    pub total_platform_revenue: f64,
    pub total_transactions: u64,
    pub active_users: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardStatusBreakdown {
    pub completed: u64,
    pub failed: u64,
    pub expired: u64,
    pub refunded: u64,
    pub open: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardQuickAccessItem {
    pub key: String,
    pub label: String,
    pub description: String,
    pub path: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardRecentActivityItem {
    pub entity_type: String,
    pub entity_id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub status: String,
    pub provider: Option<String>,
    pub amount: Option<f64>,
    pub currency: Option<String>,
    pub detail_path: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardVolumePoint {
    pub date: String,
    pub completed_swaps: u64,
    pub failed_swaps: u64,
    pub swap_volume_input: f64,
    pub giftcard_completed: u64,
    pub giftcard_volume: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardTopPair {
    pub from_currency: String,
    pub from_network: String,
    pub to_currency: String,
    pub to_network: String,
    pub trades: u64,
    pub volume_input: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsDashboardTopGiftCard {
    pub product: String,
    pub currency: Option<String>,
    pub orders: u64,
    pub volume: f64,
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

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsWebhookQuery {
    pub include_delivered: Option<bool>,
    pub swap_id: Option<String>,
    pub event_type: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsWebhookDetailResponse {
    pub delivery: OpsWebhookDeliveryRow,
    pub webhook_id: String,
    pub signature: String,
    #[schema(value_type = Object)]
    pub payload: serde_json::Value,
    pub response_body: Option<String>,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsAssetQuery {
    pub search: Option<String>,
    pub ticker: Option<String>,
    pub network: Option<String>,
    pub memo_required: Option<bool>,
    pub active_only: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsAssetDetailQuery {
    pub network: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsAssetRow {
    pub ticker: String,
    pub name: String,
    pub network: String,
    pub memo_required: bool,
    pub extra_id_name: Option<String>,
    pub image: Option<String>,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub is_active: bool,
    pub last_synced_at: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsAssetListResponse {
    pub generated_at: String,
    pub assets: Vec<OpsAssetRow>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsAssetDetailResponse {
    pub generated_at: String,
    pub asset: OpsAssetRow,
    pub provider_count: u64,
    pub source_pair_count: u64,
    pub destination_pair_count: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSyncResponse {
    pub generated_at: String,
    pub synced_count: usize,
    pub target: String,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct OpsAssetValidateRequest {
    #[validate(length(min = 1, max = 20), custom(function = "validate_non_empty"))]
    pub ticker: String,
    #[validate(length(min = 1, max = 50), custom(function = "validate_non_empty"))]
    pub network: String,
    #[validate(length(min = 1, max = 255), custom(function = "validate_non_empty"))]
    pub address: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsAssetValidateResponse {
    pub valid: bool,
    pub ticker: String,
    pub network: String,
    pub address: String,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsGiftCardCatalogQuery {
    pub country: Option<String>,
    pub search: Option<String>,
    pub category: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsGiftCardCatalogDetailQuery {
    pub country: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsGiftCardCatalogResponse {
    pub generated_at: String,
    pub country: Option<String>,
    pub source: String,
    pub cards: Vec<GiftCardProductResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsGiftCardCatalogDetailResponse {
    pub generated_at: String,
    pub source: String,
    pub card: GiftCardProductResponse,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct OpsProviderListQuery {
    pub search: Option<String>,
    pub rating: Option<String>,
    pub markup_enabled: Option<bool>,
    pub active_only: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsProviderSummary {
    pub id: String,
    pub name: String,
    pub kyc_rating: String,
    pub insurance_percentage: Option<f64>,
    pub markup_enabled: bool,
    pub eta_minutes: Option<i32>,
    pub is_active: bool,
    pub last_synced_at: Option<String>,
    pub open_swaps: u64,
    pub failed_swaps_24h: u64,
    pub completed_swaps_30d: u64,
    pub volume_input_30d: f64,
    pub platform_fees_30d: f64,
    pub last_activity_at: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsProviderListResponse {
    pub generated_at: String,
    pub providers: Vec<OpsProviderSummary>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsProviderDetailResponse {
    pub generated_at: String,
    pub provider: OpsProviderSummary,
    pub top_pairs: Vec<OpsDashboardTopPair>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsPayoutPolicySettings {
    pub local_certified_chains: Vec<String>,
    pub trocador_only_chains: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSettingsResponse {
    pub generated_at: String,
    pub admin_email: String,
    pub trocador_api_key_configured: bool,
    pub trocador_webhook_enabled: bool,
    pub trocador_webhook_key_configured: bool,
    pub public_base_url: Option<String>,
    pub swap_webhook_url: Option<String>,
    pub giftcard_webhook_url: Option<String>,
    pub swap_markup: Option<String>,
    pub allowed_swap_markups: Vec<String>,
    pub allowed_card_markups: Vec<String>,
    pub payout_policy: OpsPayoutPolicySettings,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OpsSettingsDiagnosticsResponse {
    pub generated_at: String,
    pub api_key_valid: bool,
    pub providers_fetch_ok: bool,
    pub currencies_fetch_ok: bool,
    pub giftcards_fetch_ok: bool,
    pub webhook_base_url_present: bool,
    pub swap_webhook_config_complete: bool,
    pub giftcard_webhook_config_complete: bool,
    pub errors: Vec<String>,
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
