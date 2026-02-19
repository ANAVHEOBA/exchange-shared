use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

// =============================================================================
// PROVIDERS
// =============================================================================

// Request query parameters for /swap/providers
#[derive(Debug, Deserialize, Clone)]
pub struct ProvidersQuery {
    pub rating: Option<String>,         // Filter by KYC rating (A, B, C, D)
    pub markup_enabled: Option<bool>,   // Filter by markup support
    pub sort: Option<String>,           // Sort by: name, rating, eta
}

// Response DTO matching Trocador's /exchanges format EXACTLY
#[derive(Debug, Serialize)]
pub struct ProviderResponse {
    pub name: String,
    pub rating: String,           // Maps from kyc_rating (A/B/C/D)
    pub insurance: f64,           // Maps from insurance_percentage
    pub markup_enabled: bool,     // Maps from markup_enabled (note: Trocador uses "enabledmarkup")
    pub eta: i32,                 // Maps from eta_minutes
}

// Trocador's /exchanges response format (what we GET from them)
#[derive(Debug, Deserialize)]
pub struct TrocadorProvider {
    pub name: String,
    pub rating: String,           // A, B, C, or D
    pub insurance: f64,
    #[serde(rename = "enabledmarkup")]
    pub enabled_markup: bool,     // Note the different naming
    pub eta: f64,                 // Trocador returns this as float, we'll convert to i32
}

impl From<crate::modules::swap::model::Provider> for ProviderResponse {
    fn from(p: crate::modules::swap::model::Provider) -> Self {
        Self {
            name: p.name,
            rating: p.kyc_rating,
            insurance: p.insurance_percentage.unwrap_or(0.015),
            markup_enabled: p.markup_enabled,
            eta: p.eta_minutes.unwrap_or(10),
        }
    }
}

// =============================================================================
// CURRENCIES
// =============================================================================

// Request query parameters for /swap/currencies
#[derive(Debug, Deserialize, Default, Clone)]
pub struct CurrenciesQuery {
    pub ticker: Option<String>,         // Filter by ticker (e.g., "btc")
    pub network: Option<String>,        // Filter by network (e.g., "Mainnet")
    pub memo: Option<bool>,             // Filter by memo required
    pub page: Option<usize>,            // Pagination: Page number (1-based)
    pub limit: Option<usize>,           // Pagination: Items per page
}

// Response DTO matching Trocador's /coins format EXACTLY
#[derive(Debug, Serialize, Clone)]
pub struct CurrencyResponse {
    pub name: String,
    pub ticker: String,       // Maps from symbol
    pub network: String,
    pub memo: bool,           // Maps from requires_extra_id
    pub image: String,        // Maps from logo_url
    pub minimum: f64,         // Maps from min_amount
    pub maximum: f64,         // Maps from max_amount
}

// Trocador's /coins response format (what we GET from them)
#[derive(Debug, Deserialize)]
pub struct TrocadorCurrency {
    pub name: String,
    pub ticker: String,
    pub network: String,
    pub memo: bool,
    pub image: String,
    pub minimum: f64,
    pub maximum: f64,
}

impl From<crate::modules::swap::model::Currency> for CurrencyResponse {
    fn from(c: crate::modules::swap::model::Currency) -> Self {
        Self {
            name: c.name,
            ticker: c.symbol,
            network: c.network,
            memo: c.requires_extra_id,
            image: c.logo_url.unwrap_or_else(|| String::from("")),
            minimum: c.min_amount.unwrap_or(0.0),
            maximum: c.max_amount.unwrap_or(0.0),
        }
    }
}

// =============================================================================
// PAIRS
// =============================================================================

#[derive(Debug, Deserialize, Clone)]
pub struct PairsQuery {
    // Filtering
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub base_network: Option<String>,
    pub quote_network: Option<String>,
    pub status: Option<String>,  // "active", "disabled", "all"
    
    // Pagination
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_pairs_size")]
    pub size: u32,
    
    // Sorting
    pub order_by: Option<String>,  // e.g., "volume_24h desc", "name asc"
    
    // Filtering expression (advanced)
    pub filter: Option<String>,
}

fn default_page() -> u32 { 0 }
fn default_pairs_size() -> u32 { 20 }

#[derive(Debug, Serialize)]
pub struct PairResponse {
    pub name: String,  // e.g., "BTC/USDT"
    pub base_currency: String,
    pub quote_currency: String,
    pub base_network: String,
    pub quote_network: String,
    pub status: String,  // "active" or "disabled"
    pub min_amount: Option<f64>,
    pub max_amount: Option<f64>,
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct PairsResponse {
    pub pairs: Vec<PairResponse>,
    pub pagination: PairsPaginationInfo,
}

#[derive(Debug, Serialize)]
pub struct PairsPaginationInfo {
    pub page: u32,
    pub size: u32,
    pub total_elements: i64,
    pub total_pages: u32,
    pub has_next: bool,
    pub has_prev: bool,
}

// =============================================================================
// RATES
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RatesQuery {
    pub from: String,
    pub network_from: String,
    pub to: String,
    pub network_to: String,
    pub amount: f64,
    pub rate_type: Option<RateType>,
    pub provider: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(rename_all = "lowercase")]
pub enum RateType {
    Fixed,
    Floating,
}

impl Default for RateType {
    fn default() -> Self {
        RateType::Floating
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RateResponse {
    pub provider: String,
    pub provider_name: String,
    pub rate: f64,
    pub estimated_amount: f64,
    pub min_amount: f64,
    pub max_amount: f64,
    pub network_fee: f64,
    pub provider_fee: f64,
    pub platform_fee: f64,
    pub total_fee: f64,
    pub rate_type: RateType,
    pub kyc_required: bool,
    pub kyc_rating: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta_minutes: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RatesResponse {
    pub trade_id: String, // Trocador trade ID
    pub from: String,
    pub network_from: String,
    pub to: String,
    pub network_to: String,
    pub amount: f64,
    pub rates: Vec<RateResponse>,
}

// Trocador's internal rate response
#[derive(Debug, Deserialize)]
pub struct TrocadorQuote {
    pub provider: String,
    pub amount_to: String, // String in Trocador JSON
    pub min_amount: Option<f64>,
    pub max_amount: Option<f64>,
    pub kycrating: Option<String>,
    pub waste: Option<String>, // String in Trocador JSON
    pub eta: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct TrocadorQuotesWrapper {
    pub markup: bool,
    pub quotes: Vec<TrocadorQuote>,
}

#[derive(Debug, Deserialize)]
pub struct TrocadorRatesResponse {
    pub trade_id: String,
    pub ticker_from: String,
    pub network_from: String,
    pub ticker_to: String,
    pub network_to: String,
    pub amount_from: f64,
    pub provider: String,
    pub amount_to: f64,
    pub quotes: TrocadorQuotesWrapper,
}

// =============================================================================
// ESTIMATE - Quick rate preview without creating swap
// =============================================================================

use validator::Validate;

#[derive(Debug, Deserialize, Validate, Clone)]
pub struct EstimateQuery {
    #[validate(length(min = 1, max = 20))]
    pub from: String,
    
    #[validate(length(min = 1, max = 20))]
    pub to: String,
    
    #[validate(range(min = 0.0, max = 1000000.0))]
    pub amount: f64,
    
    #[validate(length(min = 1, max = 50))]
    pub network_from: String,
    
    #[validate(length(min = 1, max = 50))]
    pub network_to: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EstimateResponse {
    // Request echo
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub network_from: String,
    pub network_to: String,
    
    // Best rate summary
    pub best_rate: f64,
    pub estimated_receive: f64,
    pub estimated_receive_min: f64,  // After slippage
    pub estimated_receive_max: f64,  // Best case
    
    // Fee breakdown
    pub network_fee: f64,
    pub provider_fee: f64,
    pub platform_fee: f64,
    pub total_fee: f64,
    
    // Slippage info
    pub slippage_percentage: f64,
    pub price_impact: f64,
    
    // Provider info
    pub best_provider: String,
    pub provider_count: usize,
    
    // Metadata
    pub cached: bool,
    pub cache_age_seconds: i64,
    pub expires_in_seconds: i64,
    
    // Warning flags
    pub warnings: Vec<String>,
}

// Internal cache entry with metadata for PER
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EstimateCacheEntry {
    pub response: EstimateResponse,
    pub created_at: i64,      // Unix timestamp in milliseconds
    pub expires_at: i64,      // Unix timestamp in milliseconds
    pub compute_time_ms: i64, // How long it took to compute (delta for PER)
}

// =============================================================================
// CREATE SWAP
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct CreateSwapRequest {
    pub trade_id: Option<String>, // ID from new_rate
    pub from: String,
    pub network_from: String,
    pub to: String,
    pub network_to: String,
    pub amount: f64,
    pub provider: String,
    pub recipient_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_extra_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_extra_id: Option<String>,
    #[serde(default)]
    pub rate_type: RateType,
    #[serde(default)]
    pub sandbox: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateSwapResponse {
    pub swap_id: String,
    pub provider: String,
    pub from: String,
    pub to: String,
    pub deposit_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deposit_extra_id: Option<String>,
    pub deposit_amount: f64,
    pub recipient_address: String,
    pub estimated_receive: f64,
    pub rate: f64,
    pub status: SwapStatus,
    pub rate_type: RateType,
    pub is_sandbox: bool,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

// Trocador's internal trade response
#[derive(Debug, Deserialize)]
pub struct TrocadorTradeResponse {
    pub trade_id: String,
    pub status: String,
    pub ticker_from: String,
    pub network_from: String,
    pub ticker_to: String,
    pub network_to: String,
    pub amount_from: f64,
    pub amount_to: f64,
    pub provider: String,
    pub address_provider: String,
    pub address_provider_memo: Option<String>,
    pub address_user: String,
    pub address_user_memo: Option<String>,
    pub refund_address: Option<String>,
    pub refund_address_memo: Option<String>,
    pub id_provider: Option<String>,
    pub date: Option<String>,
}

// =============================================================================
// SWAP STATUS
// =============================================================================

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(rename_all = "lowercase")]
pub enum SwapStatus {
    Waiting,
    Confirming,
    Exchanging,
    Sending,
    Completed,
    Failed,
    Refunded,
    Expired,
}

impl Default for SwapStatus {
    fn default() -> Self {
        SwapStatus::Waiting
    }
}

#[derive(Debug, Serialize)]
pub struct SwapStatusResponse {
    pub swap_id: String,
    pub provider: String,
    pub provider_swap_id: Option<String>,
    pub status: SwapStatus,
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub deposit_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deposit_extra_id: Option<String>,
    pub recipient_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipient_extra_id: Option<String>,
    pub rate: f64,
    pub estimated_receive: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_receive: Option<f64>,
    pub network_fee: f64,
    pub total_fee: f64,
    pub rate_type: RateType,
    pub is_sandbox: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash_in: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_hash_out: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

// =============================================================================
// SWAP HISTORY (Keyset Pagination)
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    // Keyset pagination
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    
    // Filters
    pub status: Option<String>,
    pub from_currency: Option<String>,
    pub to_currency: Option<String>,
    pub provider: Option<String>,
    pub date_from: Option<String>,  // ISO 8601
    pub date_to: Option<String>,    // ISO 8601
    
    // Sorting (optional)
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

fn default_limit() -> u32 { 20 }

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryCursor {
    pub created_at: DateTime<Utc>,
    pub id: String,
    
    // Filter snapshot for validation
    pub status: Option<String>,
    pub from_currency: Option<String>,
    pub to_currency: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SwapSummary {
    pub id: String,
    pub status: SwapStatus,
    pub from_currency: String,
    pub from_network: String,
    pub to_currency: String,
    pub to_network: String,
    pub amount: f64,
    pub estimated_receive: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_receive: Option<f64>,
    pub rate: f64,
    pub platform_fee: f64,
    pub total_fee: f64,
    pub deposit_address: String,
    pub recipient_address: String,
    pub provider: String,
    pub rate_type: RateType,
    pub is_sandbox: bool,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub swaps: Vec<SwapSummary>,
    pub pagination: PaginationInfo,
    pub filters_applied: FiltersApplied,
}

#[derive(Debug, Serialize)]
pub struct PaginationInfo {
    pub limit: u32,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FiltersApplied {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_to: Option<String>,
}

// =============================================================================
// ADDRESS VALIDATION
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ValidateAddressRequest {
    pub ticker: String,
    pub network: String,
    pub address: String,
}

#[derive(Debug, Serialize)]
pub struct ValidateAddressResponse {
    pub valid: bool,
    pub ticker: String,
    pub network: String,
    pub address: String,
}

// =============================================================================
// ERROR RESPONSE
// =============================================================================

#[derive(Debug, Serialize)]
pub struct SwapErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_amount: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_amount: Option<f64>,
}

impl SwapErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            min_amount: None,
            max_amount: None,
        }
    }

    pub fn with_code(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: Some(code.into()),
            min_amount: None,
            max_amount: None,
        }
    }

    pub fn with_limits(error: impl Into<String>, min: f64, max: f64) -> Self {
        Self {
            error: error.into(),
            code: None,
            min_amount: Some(min),
            max_amount: Some(max),
        }
    }
}
