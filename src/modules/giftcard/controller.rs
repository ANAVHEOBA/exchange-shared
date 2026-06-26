use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;
use validator::Validate;

use super::{
    fallback_catalog::fallback_catalog,
    schema::{
        CardOrderResponse, CreateGiftCardOrderRequest, CreatePrepaidCardOrderRequest,
        GiftCardCatalogQuery, GiftCardCatalogResponse, GiftCardErrorResponse, PrepaidCardsResponse,
    },
    service::{GiftCardService, GiftCardServiceError},
};
use crate::{
    middleware::{client_identity::AnonymousClientId, user::OptionalUser},
    modules::swap::schema::TrocadorTradeResponse,
    services::trocador::{TrocadorError, TrocadorGateway},
    AppState,
};

#[derive(Debug, Deserialize)]
struct TrocadorWebhookPayload {
    #[serde(flatten)]
    trade: TrocadorTradeResponse,
    #[serde(default)]
    webhook_key: Option<String>,
    #[serde(default)]
    key: Option<String>,
}

impl TrocadorWebhookPayload {
    fn provided_webhook_key(&self) -> Option<&str> {
        self.webhook_key.as_deref().or(self.key.as_deref())
    }
}

fn country_code_to_giftcard_country(code: &str) -> Option<&'static str> {
    match code {
        "AR" => Some("Argentina"),
        "AU" => Some("Australia"),
        "AT" => Some("Austria"),
        "BE" => Some("Belgium"),
        "BR" => Some("Brazil"),
        "CA" => Some("Canada"),
        "HR" => Some("Croatia"),
        "CY" => Some("Cyprus"),
        "EE" => Some("Estonia"),
        "FI" => Some("Finland"),
        "FR" => Some("France"),
        "DE" => Some("Germany"),
        "GR" => Some("Greece"),
        "GU" => Some("Guam"),
        "HU" => Some("Hungary"),
        "IN" => Some("India"),
        "IE" => Some("Ireland"),
        "IT" => Some("Italy"),
        "JP" => Some("Japan"),
        "LV" => Some("Latvia"),
        "LT" => Some("Lithuania"),
        "LU" => Some("Luxembourg"),
        "MT" => Some("Malta"),
        "MX" => Some("Mexico"),
        "NL" => Some("Netherlands"),
        "NZ" => Some("New Zealand"),
        "PE" => Some("Peru"),
        "PH" => Some("Philippines"),
        "PL" => Some("Poland"),
        "PT" => Some("Portugal"),
        "PR" => Some("Puerto Rico"),
        "RO" => Some("Romania"),
        "SG" => Some("Singapore"),
        "SK" => Some("Slovakia"),
        "SI" => Some("Slovenia"),
        "ES" => Some("Spain"),
        "CH" => Some("Switzerland"),
        "TR" => Some("Turkey"),
        "AE" => Some("United Arab Emirates"),
        "GB" => Some("UK"),
        "US" => Some("USA"),
        _ => None,
    }
}

fn giftcard_country_candidates(country: Option<&str>) -> Vec<String> {
    let Some(country) = country.map(str::trim).filter(|value| !value.is_empty()) else {
        return Vec::new();
    };

    let mut candidates = Vec::new();
    let mut push = |value: &str| {
        if !candidates
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(value))
        {
            candidates.push(value.to_string());
        }
    };

    let uppercase = country.to_ascii_uppercase();

    if let Some(mapped) = country_code_to_giftcard_country(&uppercase) {
        push(mapped);
    }

    if uppercase == "GB" {
        push("United Kingdom");
    } else if uppercase == "US" {
        push("United States");
    } else if uppercase == "AE" {
        push("UAE");
    }

    push(country);
    candidates
}

fn gateway() -> Result<TrocadorGateway, (StatusCode, Json<GiftCardErrorResponse>)> {
    TrocadorGateway::from_env().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GiftCardErrorResponse::new("TROCADOR_API_KEY not set")),
        )
    })
}

fn giftcard_service(state: &Arc<AppState>) -> GiftCardService {
    GiftCardService::new(state.clone())
}

fn map_trocador_error(error: TrocadorError) -> (StatusCode, Json<GiftCardErrorResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(GiftCardErrorResponse::new(error.to_string())),
    )
}

fn map_service_error(error: GiftCardServiceError) -> (StatusCode, Json<GiftCardErrorResponse>) {
    let status = match error {
        GiftCardServiceError::Validation(_) => StatusCode::BAD_REQUEST,
        GiftCardServiceError::NotFound | GiftCardServiceError::Forbidden => StatusCode::NOT_FOUND,
        GiftCardServiceError::Config(_) | GiftCardServiceError::Database(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        GiftCardServiceError::External(_) => StatusCode::BAD_GATEWAY,
    };

    (status, Json(GiftCardErrorResponse::new(error.to_string())))
}

fn parse_trocador_webhook_payload(body: &str) -> Result<TrocadorWebhookPayload, String> {
    serde_json::from_str(body).or_else(|json_error| {
        parse_trocador_webhook_form(body).map_err(|form_error| {
            format!(
                "Invalid webhook payload. JSON parse error: {}; form parse error: {}",
                json_error, form_error
            )
        })
    })
}

fn parse_trocador_webhook_form(body: &str) -> Result<TrocadorWebhookPayload, String> {
    let mut map = JsonMap::new();

    for pair in body.split('&') {
        if pair.is_empty() {
            continue;
        }

        let mut parts = pair.splitn(2, '=');
        let raw_key = parts.next().unwrap_or_default();
        let raw_value = parts.next().unwrap_or_default();
        let key = decode_form_component(raw_key)?;
        let value = decode_form_component(raw_value)?;
        map.insert(key.clone(), coerce_form_value(&key, value));
    }

    serde_json::from_value(JsonValue::Object(map)).map_err(|error| error.to_string())
}

fn decode_form_component(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(input.len());
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let hex = &input[index + 1..index + 3];
                let value = u8::from_str_radix(hex, 16)
                    .map_err(|_| format!("Invalid percent-encoding: %{}", hex))?;
                out.push(value);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(out).map_err(|error| format!("Invalid UTF-8 in form payload: {}", error))
}

fn coerce_form_value(key: &str, value: String) -> JsonValue {
    if matches!(key, "amount_from" | "amount_to") {
        return value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::String(value));
    }

    if matches!(key, "payment" | "fixed") {
        match value.to_ascii_lowercase().as_str() {
            "true" => return JsonValue::Bool(true),
            "false" => return JsonValue::Bool(false),
            _ => {}
        }
    }

    JsonValue::String(value)
}

#[utoipa::path(
    get,
    path = "/giftcards/prepaid",
    tag = "Gift Cards",
    responses(
        (status = 200, description = "Available prepaid card products", body = PrepaidCardsResponse),
        (status = 500, description = "Server configuration error", body = GiftCardErrorResponse),
        (status = 502, description = "Upstream provider error", body = GiftCardErrorResponse)
    )
)]
pub async fn get_prepaid_cards(
    State(_state): State<Arc<AppState>>,
) -> Result<Json<PrepaidCardsResponse>, (StatusCode, Json<GiftCardErrorResponse>)> {
    let gateway = gateway()?;
    let cards = gateway
        .fetch_prepaid_cards()
        .await
        .map_err(map_trocador_error)?;

    Ok(Json(PrepaidCardsResponse {
        cards: cards.into_iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/giftcards",
    tag = "Gift Cards",
    params(GiftCardCatalogQuery),
    responses(
        (status = 200, description = "Available gift card catalog", body = GiftCardCatalogResponse),
        (status = 500, description = "Server configuration error", body = GiftCardErrorResponse),
        (status = 502, description = "Upstream provider error", body = GiftCardErrorResponse)
    )
)]
pub async fn get_giftcard_catalog(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<GiftCardCatalogQuery>,
) -> Result<Json<GiftCardCatalogResponse>, (StatusCode, Json<GiftCardErrorResponse>)> {
    let gateway = gateway()?;
    let fallback_cards = fallback_catalog(query.country.as_deref());
    let country_candidates = giftcard_country_candidates(query.country.as_deref());
    let mut last_error = None;
    let mut cards = Vec::new();

    if country_candidates.is_empty() {
        match gateway.fetch_giftcards(None).await {
            Ok(result) => cards = result.into_iter().map(Into::into).collect(),
            Err(error) => last_error = Some(error),
        }
    } else {
        for country in country_candidates {
            match gateway.fetch_giftcards(Some(country.as_str())).await {
                Ok(result) if !result.is_empty() => {
                    cards = result.into_iter().map(Into::into).collect();
                    break;
                }
                Ok(_) => {}
                Err(error) => last_error = Some(error),
            }
        }
    }

    if cards.is_empty() && !fallback_cards.is_empty() {
        cards = fallback_cards;
    }

    if cards.is_empty() {
        if let Some(error) = last_error {
            return Err(map_trocador_error(error));
        }
    }

    Ok(Json(GiftCardCatalogResponse {
        country: query.country,
        cards,
    }))
}

#[utoipa::path(
    post,
    path = "/giftcards/order",
    tag = "Gift Cards",
    request_body = CreateGiftCardOrderRequest,
    responses(
        (status = 200, description = "Existing recent duplicate order returned", body = CardOrderResponse),
        (status = 201, description = "Gift card order created", body = CardOrderResponse),
        (status = 202, description = "Gift card order accepted and queued for retry/reconciliation", body = CardOrderResponse),
        (status = 400, description = "Invalid gift card order request", body = GiftCardErrorResponse),
        (status = 500, description = "Server configuration error", body = GiftCardErrorResponse),
        (status = 502, description = "Upstream provider error", body = GiftCardErrorResponse)
    )
)]
pub async fn order_giftcard(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    client_id: AnonymousClientId,
    Json(req): Json<CreateGiftCardOrderRequest>,
) -> Result<(StatusCode, Json<CardOrderResponse>), (StatusCode, Json<GiftCardErrorResponse>)> {
    if let Err(error) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(GiftCardErrorResponse::new(error.to_string())),
        ));
    }

    let service = giftcard_service(&state);
    let (status, response) = service
        .create_giftcard_order(
            user.0.as_ref().map(|value| value.id.as_str()),
            client_id.as_str(),
            &req,
        )
        .await
        .map_err(map_service_error)?;

    Ok((
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        Json(response),
    ))
}

#[utoipa::path(
    post,
    path = "/giftcards/prepaid/order",
    tag = "Gift Cards",
    request_body = CreatePrepaidCardOrderRequest,
    responses(
        (status = 200, description = "Existing recent duplicate prepaid order returned", body = CardOrderResponse),
        (status = 201, description = "Prepaid card order created", body = CardOrderResponse),
        (status = 202, description = "Prepaid card order accepted and queued for retry/reconciliation", body = CardOrderResponse),
        (status = 400, description = "Invalid prepaid card order request", body = GiftCardErrorResponse),
        (status = 500, description = "Server configuration error", body = GiftCardErrorResponse),
        (status = 502, description = "Upstream provider error", body = GiftCardErrorResponse)
    )
)]
pub async fn order_prepaid_card(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    client_id: AnonymousClientId,
    Json(req): Json<CreatePrepaidCardOrderRequest>,
) -> Result<(StatusCode, Json<CardOrderResponse>), (StatusCode, Json<GiftCardErrorResponse>)> {
    if let Err(error) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(GiftCardErrorResponse::new(error.to_string())),
        ));
    }

    let service = giftcard_service(&state);
    let (status, response) = service
        .create_prepaid_order(
            user.0.as_ref().map(|value| value.id.as_str()),
            client_id.as_str(),
            &req,
        )
        .await
        .map_err(map_service_error)?;

    Ok((
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        Json(response),
    ))
}

#[utoipa::path(
    get,
    path = "/giftcards/orders/{trade_id}",
    tag = "Gift Cards",
    params(
        ("trade_id" = String, Path, description = "Local order id or upstream Trocador trade id")
    ),
    responses(
        (status = 200, description = "Current card order status", body = CardOrderResponse),
        (status = 404, description = "Order not found", body = GiftCardErrorResponse),
        (status = 500, description = "Server configuration error", body = GiftCardErrorResponse),
        (status = 502, description = "Upstream provider error", body = GiftCardErrorResponse)
    )
)]
pub async fn get_order_status(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    client_id: AnonymousClientId,
    Path(order_ref): Path<String>,
) -> Result<Json<CardOrderResponse>, (StatusCode, Json<GiftCardErrorResponse>)> {
    let service = giftcard_service(&state);
    let response = service
        .get_order_for_requester(
            &order_ref,
            user.0.as_ref().map(|value| value.id.as_str()),
            client_id.as_str(),
        )
        .await
        .map_err(map_service_error)?;

    Ok(Json(response))
}

pub async fn trocador_webhook(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<StatusCode, (StatusCode, Json<GiftCardErrorResponse>)> {
    let expected_webhook_key = std::env::var("TROCADOR_WEBHOOK_KEY").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(GiftCardErrorResponse::new(
                "Trocador gift card webhook is not configured".to_string(),
            )),
        )
    })?;

    let payload = parse_trocador_webhook_payload(&body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(GiftCardErrorResponse::new(format!(
                "Failed to parse Trocador webhook payload: {}",
                error
            ))),
        )
    })?;

    if payload.provided_webhook_key() != Some(expected_webhook_key.as_str()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(GiftCardErrorResponse::new(
                "Invalid Trocador webhook key".to_string(),
            )),
        ));
    }

    let service = giftcard_service(&state);
    service
        .handle_trocador_webhook(&payload.trade)
        .await
        .map_err(map_service_error)?;

    Ok(StatusCode::OK)
}
