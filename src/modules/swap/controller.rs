use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::sync::Arc;

use super::crud::{CurrenciesResult, SwapCrud};
use super::schema::{
    ClientHistoryResponse, CreateDonationSwapRequest, CreateSwapRequest, CreateSwapResponse,
    CurrenciesQuery, DonationRatesQuery, DonationTargetResponse, HistoryQuery, HistoryResponse,
    ProvidersQuery, SwapErrorResponse, SwapOpsActionResponse, SwapStatusResponse,
    SwapTimelineResponse, ValidateAddressRequest, ValidateAddressResponse,
};
use super::service::SwapService;
use crate::middleware::admin::Admin;
use crate::middleware::client_identity::AnonymousClientId;
use crate::middleware::user::{OptionalUser, User};
use crate::services::trocador::HostedSwapRecipientConfig;
use crate::AppState;

fn swap_crud(state: &Arc<AppState>) -> SwapCrud {
    SwapCrud::new(
        state.db.clone(),
        None,
        state.wallet_mnemonic.clone(),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
}

fn swap_service(state: &Arc<AppState>) -> SwapService {
    SwapService::new(
        state.db.clone(),
        None,
        state.wallet_mnemonic.clone(),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
}

fn load_donation_target() -> Result<HostedSwapRecipientConfig, (StatusCode, Json<SwapErrorResponse>)>
{
    match HostedSwapRecipientConfig::from_env() {
        Ok(Some(config)) => Ok(config),
        Ok(None) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SwapErrorResponse::new(
                "Hosted donation flow is not configured".to_string(),
            )),
        )),
        Err(error) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(SwapErrorResponse::new(error)),
        )),
    }
}

#[derive(Debug, Deserialize)]
struct TrocadorWebhookPayload {
    #[serde(flatten)]
    trade: super::schema::TrocadorTradeResponse,
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

    serde_json::from_value(JsonValue::Object(map)).map_err(|e| e.to_string())
}

fn decode_form_component(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = &input[i + 1..i + 3];
                let value = u8::from_str_radix(hex, 16)
                    .map_err(|_| format!("Invalid percent-encoding: %{}", hex))?;
                out.push(value);
                i += 3;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }

    String::from_utf8(out).map_err(|e| format!("Invalid UTF-8 in form payload: {}", e))
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

// ... (existing handlers)

// =============================================================================
// POST /swap/create - Create a new swap
// =============================================================================

#[utoipa::path(
    post,
    path = "/swap/create",
    tag = "Swap",
    request_body = CreateSwapRequest,
    responses(
        (status = 201, description = "Swap created successfully", body = CreateSwapResponse),
        (status = 400, description = "Invalid swap request", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn create_swap(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    client_id: AnonymousClientId,
    Json(payload): Json<CreateSwapRequest>,
) -> Result<(StatusCode, Json<CreateSwapResponse>), (StatusCode, Json<SwapErrorResponse>)> {
    let service = swap_service(&state);
    let user_id = user.0.map(|u| u.id);
    let anonymous_client_id = if user_id.is_none() {
        Some(client_id.0)
    } else {
        None
    };

    let response = service
        .create_swap(&payload, user_id, anonymous_client_id)
        .await
        .map_err(|e| {
            let status = match e {
                super::crud::SwapError::AmountOutOfRange { .. } => StatusCode::BAD_REQUEST,
                super::crud::SwapError::InvalidAddress => StatusCode::BAD_REQUEST,
                super::crud::SwapError::ValidationError(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(SwapErrorResponse::new(e.to_string())))
        })?;

    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/swap/donation/target",
    tag = "Swap",
    responses(
        (status = 200, description = "Server-controlled donation target metadata", body = DonationTargetResponse),
        (status = 503, description = "Donation flow is not configured", body = SwapErrorResponse)
    )
)]
pub async fn get_donation_target(
) -> Result<Json<DonationTargetResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let config = load_donation_target()?;
    Ok(Json(DonationTargetResponse::from_config(&config)))
}

#[utoipa::path(
    get,
    path = "/swap/donation/rates",
    tag = "Swap",
    params(DonationRatesQuery),
    responses(
        (status = 200, description = "Rates for the configured donation target", body = super::schema::RatesResponse),
        (status = 502, description = "Upstream provider error", body = SwapErrorResponse),
        (status = 503, description = "Donation flow is not configured", body = SwapErrorResponse)
    )
)]
pub async fn get_donation_rates(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DonationRatesQuery>,
) -> Result<Json<super::schema::RatesResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let config = load_donation_target()?;
    let rates_query = query.into_rates_query(&config);
    let crud = swap_crud(&state);

    let response = crud
        .get_provider_managed_rates(&rates_query)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                Json(SwapErrorResponse::new(e.to_string())),
            )
        })?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/swap/donation/create",
    tag = "Swap",
    request_body = CreateDonationSwapRequest,
    responses(
        (status = 201, description = "Donation swap created successfully", body = CreateSwapResponse),
        (status = 400, description = "Invalid donation swap request", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 503, description = "Donation flow is not configured", body = SwapErrorResponse)
    )
)]
pub async fn create_donation_swap(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    client_id: AnonymousClientId,
    Json(payload): Json<CreateDonationSwapRequest>,
) -> Result<(StatusCode, Json<CreateSwapResponse>), (StatusCode, Json<SwapErrorResponse>)> {
    let config = load_donation_target()?;
    let request = payload.into_create_swap_request(&config);
    let service = swap_service(&state);
    let user_id = user.0.map(|u| u.id);
    let anonymous_client_id = if user_id.is_none() {
        Some(client_id.0)
    } else {
        None
    };

    let response = service
        .create_provider_managed_swap(&request, user_id, anonymous_client_id)
        .await
        .map_err(|e| {
            let status = match e {
                super::crud::SwapError::AmountOutOfRange { .. } => StatusCode::BAD_REQUEST,
                super::crud::SwapError::InvalidAddress => StatusCode::BAD_REQUEST,
                super::crud::SwapError::ValidationError(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(SwapErrorResponse::new(e.to_string())))
        })?;

    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    post,
    path = "/swap/webhooks/trocador",
    tag = "Swap",
    request_body = String,
    responses(
        (status = 200, description = "Trocador swap webhook accepted"),
        (status = 400, description = "Invalid webhook payload", body = SwapErrorResponse),
        (status = 401, description = "Invalid webhook key", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 503, description = "Webhook not configured", body = SwapErrorResponse)
    )
)]
pub async fn trocador_webhook(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    body: String,
) -> Result<StatusCode, (StatusCode, Json<SwapErrorResponse>)> {
    let expected_webhook_key = std::env::var("TROCADOR_WEBHOOK_KEY").map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(SwapErrorResponse::new(
                "Trocador webhook is not configured".to_string(),
            )),
        )
    })?;

    let payload = parse_trocador_webhook_payload(&body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(SwapErrorResponse::new(format!(
                "Failed to parse Trocador webhook payload: {}",
                e
            ))),
        )
    })?;

    if payload.provided_webhook_key() != Some(expected_webhook_key.as_str()) {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(SwapErrorResponse::new(
                "Invalid Trocador webhook key".to_string(),
            )),
        ));
    }

    let service = swap_service(&state);
    service
        .handle_trocador_trade_webhook(&payload.trade)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SwapErrorResponse::new(e.to_string())),
            )
        })?;

    Ok(StatusCode::OK)
}

#[utoipa::path(
    get,
    path = "/swap/currencies",
    tag = "Swap",
    params(CurrenciesQuery),
    responses(
        (status = 200, description = "List of supported currencies", body = [super::schema::CurrencyResponse]),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_currencies(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CurrenciesQuery>,
) -> Result<Response, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    // The CRUD layer now handles caching, pagination, raw JSON, and background synchronization
    let result = crud.get_currencies_optimized(query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(SwapErrorResponse::new(e.to_string())),
        )
    })?;

    match result {
        CurrenciesResult::Structured(responses) => {
            // Standard JSON response
            Ok(Json(responses).into_response())
        }
        CurrenciesResult::RawJson(json_string) => {
            // Optimized raw JSON response (avoids serialization overhead)
            let response = Response::builder()
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json_string))
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(SwapErrorResponse::new(e.to_string())),
                    )
                })?;
            Ok(response)
        }
    }
}

// =============================================================================
// GET /swap/providers - List all exchange providers
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/providers",
    tag = "Swap",
    params(ProvidersQuery),
    responses(
        (status = 200, description = "List of exchange providers", body = [super::schema::ProviderResponse]),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_providers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ProvidersQuery>,
) -> Result<Response, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    // The CRUD layer now handles caching, optimized filtering, and background synchronization
    let result = crud.get_providers_optimized(query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(SwapErrorResponse::new(e.to_string())),
        )
    })?;

    match result {
        super::crud::ProvidersResult::Structured(responses) => {
            // Standard JSON response
            Ok(Json(responses).into_response())
        }
        super::crud::ProvidersResult::RawJson(json_string) => {
            // Optimized raw JSON response (avoids serialization overhead)
            let response = Response::builder()
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json_string))
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(SwapErrorResponse::new(e.to_string())),
                    )
                })?;
            Ok(response)
        }
    }
}

// =============================================================================
// GET /swap/rates - Get live rates from all providers
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/rates",
    tag = "Swap",
    params(super::schema::RatesQuery),
    responses(
        (status = 200, description = "Current swap rates", body = super::schema::RatesResponse),
        (status = 404, description = "Trading pair not available", body = super::schema::SwapErrorResponse),
        (status = 502, description = "Upstream provider error", body = super::schema::SwapErrorResponse)
    )
)]
pub async fn get_rates(
    State(state): State<Arc<AppState>>,
    Query(query): Query<super::schema::RatesQuery>,
) -> Result<Json<super::schema::RatesResponse>, (StatusCode, Json<super::schema::SwapErrorResponse>)>
{
    let crud = swap_crud(&state);

    let response = crud.get_rates_optimized(&query).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::PairNotAvailable => StatusCode::NOT_FOUND,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(super::schema::SwapErrorResponse::new(e.to_string())),
        )
    })?;

    Ok(Json(response))
}

// =============================================================================
// GET /swap/:id - Get swap status by ID
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/{id}",
    tag = "Swap",
    params(
        ("id" = String, Path, description = "Internal swap id")
    ),
    responses(
        (status = 200, description = "Swap status", body = SwapStatusResponse),
        (status = 404, description = "Swap not found", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Upstream provider error", body = SwapErrorResponse)
    )
)]
pub async fn get_swap_status(
    State(state): State<Arc<AppState>>,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapStatusResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let service = swap_service(&state);

    let response = service.get_swap_status(&swap_id).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::SwapNotFound => StatusCode::NOT_FOUND,
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/swap/ops/{id}",
    tag = "Swap Ops",
    params(
        ("id" = String, Path, description = "Internal swap id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin swap detail", body = SwapStatusResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Swap not found", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Upstream provider error", body = SwapErrorResponse)
    )
)]
pub async fn get_admin_swap_status(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapStatusResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    get_swap_status(State(state), Path(swap_id)).await
}

#[utoipa::path(
    get,
    path = "/swap/ops/{id}/timeline",
    tag = "Swap Ops",
    params(
        ("id" = String, Path, description = "Internal swap id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin swap status timeline", body = SwapTimelineResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Swap not found", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_admin_swap_timeline(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapTimelineResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let service = swap_service(&state);
    let response = service.get_swap_timeline(&swap_id).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::SwapNotFound => StatusCode::NOT_FOUND,
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/swap/ops/{id}/refresh",
    tag = "Swap Ops",
    params(
        ("id" = String, Path, description = "Internal swap id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Provider status refresh attempted", body = SwapOpsActionResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Swap not found", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Provider status error", body = SwapErrorResponse)
    )
)]
pub async fn refresh_admin_swap_status(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapOpsActionResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let service = swap_service(&state);
    let status_response = service.get_swap_status(&swap_id).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::SwapNotFound => StatusCode::NOT_FOUND,
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(SwapOpsActionResponse {
        action: "refresh".to_string(),
        message: "Provider status refresh attempted".to_string(),
        status: status_response,
    }))
}

#[utoipa::path(
    post,
    path = "/swap/ops/{id}/reconcile",
    tag = "Swap Ops",
    params(
        ("id" = String, Path, description = "Internal swap id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Swap reconciled against provider state", body = SwapOpsActionResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Swap not found", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Provider reconciliation error", body = SwapErrorResponse)
    )
)]
pub async fn reconcile_admin_swap(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapOpsActionResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let service = swap_service(&state);
    let status_response = service.get_swap_status(&swap_id).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::SwapNotFound => StatusCode::NOT_FOUND,
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(SwapOpsActionResponse {
        action: "reconcile".to_string(),
        message: "Swap reconciled against provider status where available".to_string(),
        status: status_response,
    }))
}

// =============================================================================
// POST /swap/validate-address - Validate cryptocurrency address
// =============================================================================

#[utoipa::path(
    post,
    path = "/swap/validate-address",
    tag = "Swap",
    request_body = ValidateAddressRequest,
    responses(
        (status = 200, description = "Address validation result", body = ValidateAddressResponse),
        (status = 400, description = "Invalid address input", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Upstream provider error", body = SwapErrorResponse)
    )
)]
pub async fn validate_address(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ValidateAddressRequest>,
) -> Result<Json<ValidateAddressResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    let response = crud.validate_address(&payload).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::InvalidAddress => StatusCode::BAD_REQUEST,
            super::crud::SwapError::ValidationError(_) => StatusCode::BAD_REQUEST,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

// =============================================================================
// GET /swap/history - Get authenticated user's swap history
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/history",
    tag = "Swap",
    params(HistoryQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Authenticated user's swap history", body = HistoryResponse),
        (status = 400, description = "Invalid history query", body = SwapErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = String),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_swap_history(
    State(state): State<Arc<AppState>>,
    user: User, // Requires authentication
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    let response = crud
        .get_swap_history(&user.0.id, query)
        .await
        .map_err(|e| {
            let status = match e {
                super::crud::SwapError::InvalidCursor(_) => StatusCode::BAD_REQUEST,
                super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::BAD_REQUEST,
            };
            (status, Json(SwapErrorResponse::new(e.to_string())))
        })?;

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/swap/ops",
    tag = "Swap Ops",
    params(HistoryQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin swap history", body = HistoryResponse),
        (status = 400, description = "Invalid history query", body = SwapErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_admin_swap_history(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    let response = crud.get_admin_swap_history(query).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::InvalidCursor(_) => StatusCode::BAD_REQUEST,
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

// =============================================================================
// GET /swap/history/client - Get anonymous client's swap history
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/history/client",
    tag = "Swap",
    params(HistoryQuery),
    responses(
        (
            status = 200,
            description = "Anonymous client's swap history",
            body = ClientHistoryResponse,
            headers(
                ("x-client-id" = String, description = "Stable anonymous client identifier returned in both the response header and body")
            )
        ),
        (status = 400, description = "Invalid history query", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_client_swap_history(
    State(state): State<Arc<AppState>>,
    client_id: AnonymousClientId,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<ClientHistoryResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    let response = crud
        .get_swap_history_for_client(client_id.as_str(), query)
        .await
        .map_err(|e| {
            let status = match e {
                super::crud::SwapError::InvalidCursor(_) => StatusCode::BAD_REQUEST,
                super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
                _ => StatusCode::BAD_REQUEST,
            };
            (status, Json(SwapErrorResponse::new(e.to_string())))
        })?;

    Ok(Json(ClientHistoryResponse {
        client_id: client_id.0,
        swaps: response.swaps,
        pagination: response.pagination,
        filters_applied: response.filters_applied,
    }))
}

// =============================================================================
// GET /swap/pairs - List available trading pairs
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/pairs",
    tag = "Swap",
    params(super::schema::PairsQuery),
    responses(
        (status = 200, description = "Available trading pairs", body = super::schema::PairsResponse),
        (status = 400, description = "Invalid pairs query", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse)
    )
)]
pub async fn get_pairs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<super::schema::PairsQuery>,
) -> Result<Json<super::schema::PairsResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = swap_crud(&state);

    let response = crud.get_pairs(query).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

// =============================================================================
// GET /swap/estimate - Quick rate preview without creating swap
// =============================================================================

#[utoipa::path(
    get,
    path = "/swap/estimate",
    tag = "Swap",
    params(super::schema::EstimateQuery),
    responses(
        (status = 200, description = "Estimated swap result", body = super::schema::EstimateResponse),
        (status = 400, description = "Invalid estimate query", body = SwapErrorResponse),
        (status = 404, description = "Trading pair not available", body = SwapErrorResponse),
        (status = 500, description = "Server error", body = SwapErrorResponse),
        (status = 502, description = "Upstream provider error", body = SwapErrorResponse)
    )
)]
pub async fn get_estimate(
    State(state): State<Arc<AppState>>,
    Query(query): Query<super::schema::EstimateQuery>,
) -> Result<Json<super::schema::EstimateResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    use validator::Validate;

    // Validate query parameters
    if let Err(e) = query.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(SwapErrorResponse::new(e.to_string())),
        ));
    }

    let crud = swap_crud(&state);

    let response = crud.get_estimate_optimized(&query).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::PairNotAvailable => StatusCode::NOT_FOUND,
            super::crud::SwapError::AmountOutOfRange { .. } => StatusCode::BAD_REQUEST,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::{parse_trocador_webhook_payload, TrocadorWebhookPayload};

    #[test]
    fn parses_trocador_webhook_json_payload() {
        let body = r#"{
            "trade_id":"trade_123",
            "status":"finished",
            "ticker_from":"btc",
            "network_from":"Mainnet",
            "ticker_to":"eth",
            "network_to":"ERC20",
            "coin_from":"Bitcoin",
            "coin_to":"Ethereum",
            "amount_from":0.1,
            "amount_to":1.5,
            "provider":"provider_x",
            "address_provider":"provider_address",
            "address_provider_memo":null,
            "address_user":"user_address",
            "address_user_memo":null,
            "refund_address":"refund_address",
            "refund_address_memo":null,
            "id_provider":"provider_trade_id",
            "date":"2026-03-27T14:00:00Z",
            "payment":false,
            "webhook_key":"secret_123",
            "details":{"hashout":"tx_hash_out"}
        }"#;

        let payload: TrocadorWebhookPayload =
            parse_trocador_webhook_payload(body).expect("json webhook payload should parse");

        assert_eq!(payload.trade.trade_id, "trade_123");
        assert_eq!(payload.trade.status, "finished");
        assert_eq!(payload.provided_webhook_key(), Some("secret_123"));
        assert_eq!(
            payload
                .trade
                .details
                .as_ref()
                .and_then(|details| details.hashout.as_deref()),
            Some("tx_hash_out")
        );
    }

    #[test]
    fn parses_trocador_webhook_form_payload() {
        let body = "trade_id=trade_456&status=finished&ticker_from=btc&network_from=Mainnet&ticker_to=eth&network_to=ERC20&coin_from=Bitcoin&coin_to=Ethereum&amount_from=0.1&amount_to=1.5&provider=provider_x&address_provider=provider_address&address_user=user_address&refund_address=refund_address&payment=False&webhook_key=secret_456";

        let payload: TrocadorWebhookPayload =
            parse_trocador_webhook_payload(body).expect("form webhook payload should parse");

        assert_eq!(payload.trade.trade_id, "trade_456");
        assert_eq!(payload.trade.amount_to, 1.5);
        assert_eq!(payload.trade.payment, Some(false));
        assert_eq!(payload.provided_webhook_key(), Some("secret_456"));
    }
}
