use crate::modules::swap::schema::TrocadorTradeDetails;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::{IntoParams, ToSchema};
use validator::{Validate, ValidationError};

fn validate_card_markup(value: &str) -> Result<(), ValidationError> {
    match value.trim() {
        "1" | "2" | "3" => Ok(()),
        _ => Err(ValidationError::new("invalid_card_markup")),
    }
}

fn normalize_numeric_value(value: &serde_json::Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
}

fn normalize_numeric_list(values: Option<Vec<serde_json::Value>>) -> Vec<f64> {
    values
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| normalize_numeric_value(&value))
        .collect()
}

fn normalize_optional_numeric_list(value: Option<serde_json::Value>) -> Option<Vec<f64>> {
    match value {
        Some(serde_json::Value::Array(values)) => {
            let normalized = normalize_numeric_list(Some(values));
            if normalized.is_empty() {
                None
            } else {
                Some(normalized)
            }
        }
        Some(serde_json::Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("range") {
                return None;
            }

            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                if let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(trimmed) {
                    let normalized = normalize_numeric_list(Some(values));
                    if normalized.is_empty() {
                        return None;
                    }
                    return Some(normalized);
                }
            }

            trimmed.parse::<f64>().ok().map(|value| vec![value])
        }
        Some(other) => normalize_numeric_value(&other).map(|value| vec![value]),
        None => None,
    }
}

fn normalize_optional_numeric(value: Option<serde_json::Value>) -> Option<f64> {
    value.as_ref().and_then(normalize_numeric_value)
}

fn normalize_optional_string(value: Option<serde_json::Value>) -> Option<String> {
    match value {
        Some(serde_json::Value::String(raw)) => Some(raw),
        Some(serde_json::Value::Number(raw)) => Some(raw.to_string()),
        Some(serde_json::Value::Bool(raw)) => Some(raw.to_string()),
        _ => None,
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GiftCardErrorResponse {
    pub error: String,
}

impl GiftCardErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct GiftCardCatalogQuery {
    pub country: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct PrepaidCardResponse {
    pub provider: String,
    pub currency_code: String,
    pub brand: String,
    pub amounts: Vec<f64>,
    pub restricted_countries: Vec<String>,
    pub allowed_countries: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TrocadorPrepaidCard {
    pub provider: String,
    pub currency_code: String,
    pub brand: String,
    pub amounts: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub restricted_countries: Vec<String>,
    #[serde(default)]
    pub allowed_countries: Vec<String>,
}

impl From<TrocadorPrepaidCard> for PrepaidCardResponse {
    fn from(value: TrocadorPrepaidCard) -> Self {
        Self {
            provider: value.provider,
            currency_code: value.currency_code,
            brand: value.brand,
            amounts: normalize_numeric_list(value.amounts),
            restricted_countries: value.restricted_countries,
            allowed_countries: value.allowed_countries,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PrepaidCardsResponse {
    pub cards: Vec<PrepaidCardResponse>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct GiftCardProductResponse {
    pub product_id: String,
    pub name: String,
    pub category: Option<String>,
    pub description: Option<String>,
    pub terms_and_conditions: Option<String>,
    pub how_to_use: Option<String>,
    pub expiry_and_validity: Option<String>,
    pub card_image_url: Option<String>,
    pub country: Option<String>,
    pub min_amount: Option<f64>,
    pub max_amount: Option<f64>,
    pub denominations: Option<Vec<f64>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TrocadorGiftCardProduct {
    pub product_id: String,
    pub name: String,
    pub category: Option<String>,
    pub description: Option<String>,
    pub terms_and_conditions: Option<String>,
    pub how_to_use: Option<String>,
    pub expiry_and_validity: Option<String>,
    pub card_image_url: Option<String>,
    pub country: Option<String>,
    pub min_amount: Option<serde_json::Value>,
    pub max_amount: Option<serde_json::Value>,
    pub denominations: Option<serde_json::Value>,
}

impl From<TrocadorGiftCardProduct> for GiftCardProductResponse {
    fn from(value: TrocadorGiftCardProduct) -> Self {
        Self {
            product_id: value.product_id,
            name: value.name,
            category: value.category,
            description: value.description,
            terms_and_conditions: value.terms_and_conditions,
            how_to_use: value.how_to_use,
            expiry_and_validity: value.expiry_and_validity,
            card_image_url: value.card_image_url,
            country: value.country,
            min_amount: normalize_optional_numeric(value.min_amount),
            max_amount: normalize_optional_numeric(value.max_amount),
            denominations: normalize_optional_numeric_list(value.denominations),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GiftCardCatalogResponse {
    pub country: Option<String>,
    pub cards: Vec<GiftCardProductResponse>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateGiftCardOrderRequest {
    #[validate(length(min = 1, max = 120))]
    pub product_id: String,
    #[validate(length(min = 1, max = 20))]
    pub ticker_from: String,
    #[validate(length(min = 1, max = 50))]
    pub network_from: String,
    #[validate(range(min = 0.0, max = 1000000.0))]
    pub amount: f64,
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 1, max = 2048))]
    pub webhook: Option<String>,
    #[validate(length(min = 1, max = 255))]
    pub webhook_key: Option<String>,
    #[validate(custom(function = "validate_card_markup"))]
    pub card_markup: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreatePrepaidCardOrderRequest {
    #[validate(length(min = 1, max = 120))]
    pub provider: String,
    #[validate(length(min = 1, max = 12))]
    pub currency_code: String,
    #[validate(length(min = 1, max = 20))]
    pub ticker_from: String,
    #[validate(length(min = 1, max = 50))]
    pub network_from: String,
    #[validate(range(min = 0.0, max = 1000000.0))]
    pub amount: f64,
    #[validate(email)]
    pub email: String,
    #[validate(length(min = 1, max = 2048))]
    pub webhook: Option<String>,
    #[validate(length(min = 1, max = 255))]
    pub webhook_key: Option<String>,
    #[validate(custom(function = "validate_card_markup"))]
    pub card_markup: Option<String>,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct CardOrderDetailsResponse {
    pub hashout: Option<String>,
    pub id: Option<String>,
    pub email: Option<String>,
    pub status: Option<String>,
    pub value: Option<String>,
    pub activation_link: Option<String>,
    pub redeem_code: Option<String>,
    #[schema(value_type = Object)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl From<TrocadorTradeDetails> for CardOrderDetailsResponse {
    fn from(value: TrocadorTradeDetails) -> Self {
        Self {
            hashout: value.hashout,
            id: value.id,
            email: value.email,
            status: value.status,
            value: normalize_optional_string(value.value),
            activation_link: value.activation_link,
            redeem_code: value.redeem_code,
            extra: value.extra,
        }
    }
}

#[derive(Debug, Serialize, Clone, ToSchema)]
pub struct CardOrderResponse {
    pub order_id: String,
    pub trade_id: Option<String>,
    pub order_kind: String,
    pub product_id: Option<String>,
    pub prepaid_provider: Option<String>,
    pub currency_code: Option<String>,
    pub provider: Option<String>,
    pub provider_trade_id: Option<String>,
    pub provider_password: Option<String>,
    pub status: String,
    pub ticker_from: String,
    pub network_from: String,
    pub ticker_to: Option<String>,
    pub network_to: Option<String>,
    pub coin_from: Option<String>,
    pub coin_to: Option<String>,
    pub amount_from: f64,
    pub amount_to: Option<f64>,
    pub fixed: Option<bool>,
    pub payment: Option<bool>,
    pub deposit_address: Option<String>,
    pub deposit_extra_id: Option<String>,
    pub settlement_address: Option<String>,
    pub settlement_extra_id: Option<String>,
    pub refund_address: Option<String>,
    pub refund_extra_id: Option<String>,
    pub queued: bool,
    pub retryable: bool,
    pub last_error: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub completed_at: Option<String>,
    pub details: Option<CardOrderDetailsResponse>,
}
