use axum::{
    extract::{Query, State, Path},
    http::StatusCode,
    response::{Response, IntoResponse},
    Json,
};
use std::sync::Arc;

use crate::AppState;
use super::crud::{SwapCrud, CurrenciesResult};
use super::schema::{
    CurrenciesQuery, CurrencyResponse, ProvidersQuery, ProviderResponse, SwapErrorResponse,
    CreateSwapRequest, CreateSwapResponse, SwapStatusResponse, ValidateAddressRequest, ValidateAddressResponse,
};
use crate::services::trocador::TrocadorClient;
use crate::modules::auth::interface::OptionalUser;

// ... (existing handlers)

// =============================================================================
// POST /swap/create - Create a new swap
// =============================================================================

pub async fn create_swap(
    State(state): State<Arc<AppState>>,
    user: OptionalUser,
    Json(payload): Json<CreateSwapRequest>,
) -> Result<(StatusCode, Json<CreateSwapResponse>), (StatusCode, Json<SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

    let response = crud.create_swap(&payload, user.0.map(|u| u.id)).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::AmountOutOfRange { .. } => StatusCode::BAD_REQUEST,
            super::crud::SwapError::InvalidAddress => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok((StatusCode::CREATED, Json(response)))
}

pub async fn get_currencies(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CurrenciesQuery>,
) -> Result<Response, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

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
        },
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

pub async fn get_providers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ProvidersQuery>,
) -> Result<Json<Vec<ProviderResponse>>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

    // Check if we need to sync from Trocador
    let should_sync = crud.should_sync_providers().await.unwrap_or(true);

    if should_sync {
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        
        if !api_key.is_empty() {
            let trocador_client = TrocadorClient::new(api_key);
            
            if let Err(e) = crud.sync_providers_from_trocador(&trocador_client).await {
                tracing::warn!("Failed to sync providers from Trocador: {}", e);
            }
        }
    }

    // Get providers from database (cache)
    let providers = crud.get_providers(query).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(SwapErrorResponse::new(e.to_string())),
        )
    })?;

    // Convert to response format
    let responses: Vec<ProviderResponse> = providers
        .into_iter()
        .map(|p| p.into())
        .collect();

    Ok(Json(responses))
}

// =============================================================================
// GET /swap/rates - Get live rates from all providers
// =============================================================================

pub async fn get_rates(
    State(state): State<Arc<AppState>>,
    Query(query): Query<super::schema::RatesQuery>,
) -> Result<Json<super::schema::RatesResponse>, (StatusCode, Json<super::schema::SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

    let response = crud.get_rates(&query).await.map_err(|e| {
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

pub async fn get_swap_status(
    State(state): State<Arc<AppState>>,
    Path(swap_id): Path<String>,
) -> Result<Json<SwapStatusResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

    let response = crud.get_swap_status(&swap_id).await.map_err(|e| {
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

pub async fn validate_address(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ValidateAddressRequest>,
) -> Result<Json<ValidateAddressResponse>, (StatusCode, Json<SwapErrorResponse>)> {
    let crud = SwapCrud::new(state.db.clone(), Some(state.redis.clone()));

    let response = crud.validate_address(&payload).await.map_err(|e| {
        let status = match e {
            super::crud::SwapError::InvalidAddress => StatusCode::BAD_REQUEST,
            super::crud::SwapError::ExternalApiError(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(SwapErrorResponse::new(e.to_string())))
    })?;

    Ok(Json(response))
}