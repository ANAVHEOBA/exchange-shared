use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::sync::Arc;

use super::crud::{CurrenciesResult, SwapCrud};
use super::schema::{
    ClientHistoryResponse, CreateSwapRequest, CreateSwapResponse, CurrenciesQuery, HistoryQuery,
    HistoryResponse, ProvidersQuery, SwapErrorResponse, SwapStatusResponse, ValidateAddressRequest,
    ValidateAddressResponse,
};
use super::service::SwapService;
use crate::middleware::client_identity::AnonymousClientId;
use crate::middleware::user::{OptionalUser, User};
use crate::AppState;

fn swap_crud(state: &Arc<AppState>) -> SwapCrud {
    SwapCrud::new(
        state.db.clone(),
        Some(state.redis.clone()),
        Some(state.wallet_mnemonic.clone()),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
}

fn swap_service(state: &Arc<AppState>) -> SwapService {
    SwapService::new(
        state.db.clone(),
        Some(state.redis.clone()),
        Some(state.wallet_mnemonic.clone()),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
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
        (
            StatusCode::BAD_GATEWAY,
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
