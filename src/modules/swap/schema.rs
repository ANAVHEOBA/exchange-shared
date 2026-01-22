use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

// =============================================================================
// PROVIDERS
// =============================================================================

// Request query parameters for /swap/providers
#[derive(Debug, Deserialize)]
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
#[derive(Debug, Deserialize)]
pub struct CurrenciesQuery {
    pub ticker: Option<String>,         // Filter by ticker (e.g., "btc")
    pub network: Option<String>,        // Filter by network (e.g., "Mainnet")
    pub memo: Option<bool>,             // Filter by memo required
}

// Response DTO matching Trocador's /coins format EXACTLY
#[derive(Debug, Serialize)]
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

#[derive(Debug, Deserialize)]
pub struct PairsQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub from_network: Option<String>,
    pub to_network: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PairResponse {
    pub from: String,
    pub to: String,
    pub from_network: String,
    pub to_network: String,
    pub is_active: bool,
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
// ESTIMATE
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct EstimateRequest {
    pub from: String,
    pub network_from: String,
    pub to: String,
    pub network_to: String,
    pub amount: f64,
    pub provider: String,
    #[serde(default)]
    pub rate_type: RateType,
}

#[derive(Debug, Serialize)]
pub struct EstimateResponse {
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub provider: String,
    pub rate: f64,
    pub estimated_amount: f64,
    pub min_amount: f64,
    pub max_amount: f64,
    pub network_fee: f64,
    pub total_fee: f64,
    pub rate_type: RateType,
    pub valid_until: DateTime<Utc>,
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
// SWAP HISTORY
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub status: Option<SwapStatus>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    #[serde(default)]
    pub sandbox: Option<bool>,
}

fn default_page() -> u32 { 1 }
fn default_limit() -> u32 { 20 }

#[derive(Debug, Serialize)]
pub struct SwapSummary {
    pub swap_id: String,
    pub provider: String,
    pub from: String,
    pub to: String,
    pub amount: f64,
    pub estimated_receive: f64,
    pub status: SwapStatus,
    pub rate_type: RateType,
    pub is_sandbox: bool,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub swaps: Vec<SwapSummary>,
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    pub total_pages: u32,
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
