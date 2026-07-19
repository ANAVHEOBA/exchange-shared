use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::Response,
    Json,
};
use chrono::Utc;
use validator::Validate;

use super::{
    crud::{AdminCrud, AdminError},
    schema::{
        AdminErrorResponse, AdminLoginRequest, AdminLoginResponse, AdminOverviewResponse,
        AdminSwapExportQuery, OpsAssetDetailQuery, OpsAssetDetailResponse, OpsAssetListResponse,
        OpsAssetQuery, OpsAssetValidateRequest, OpsAssetValidateResponse, OpsCreateNoteRequest,
        OpsDashboardResponse, OpsFinanceQuery, OpsFinanceResponse, OpsGiftCardCatalogDetailQuery,
        OpsGiftCardCatalogDetailResponse, OpsGiftCardCatalogQuery, OpsGiftCardCatalogResponse,
        OpsHealthResponse, OpsNoteResponse, OpsProviderDetailResponse, OpsProviderListQuery,
        OpsProviderListResponse, OpsSearchQuery, OpsSearchResponse, OpsSettingsDiagnosticsResponse,
        OpsSettingsResponse, OpsSyncResponse, OpsWebhookDetailResponse, OpsWebhookMonitorResponse,
        OpsWebhookQuery,
    },
};
use crate::middleware::admin::Admin;
use crate::modules::swap::{crud::SwapCrud, schema::ValidateAddressRequest};
use crate::services::trocador::TrocadorGateway;
use crate::AppState;

fn swap_crud(state: &Arc<AppState>) -> SwapCrud {
    SwapCrud::new(
        state.db.clone(),
        state.redis.clone(),
        state.wallet_mnemonic.clone(),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
}

#[utoipa::path(
    post,
    path = "/ops/login",
    tag = "Admin",
    request_body = AdminLoginRequest,
    responses(
        (status = 200, description = "Admin login succeeded", body = AdminLoginResponse),
        (status = 400, description = "Validation error", body = AdminErrorResponse),
        (status = 401, description = "Invalid credentials", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AdminLoginRequest>,
) -> Result<(StatusCode, Json<AdminLoginResponse>), (StatusCode, Json<AdminErrorResponse>)> {
    if let Err(error) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorResponse::new(error.to_string())),
        ));
    }

    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .login(&req.email, &req.password)
        .await
        .map_err(map_admin_error)?;

    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/ops/overview",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin overview metrics", body = AdminOverviewResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn overview(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<AdminOverviewResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.overview().await.map_err(map_admin_error)?;

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/dashboard",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Overview snapshot for the admin dashboard", body = OpsDashboardResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn dashboard(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsDashboardResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.dashboard().await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/assets",
    tag = "Operations",
    params(OpsAssetQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Supported assets for the admin dashboard", body = OpsAssetListResponse),
        (status = 400, description = "Invalid asset query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn list_assets(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsAssetQuery>,
) -> Result<Json<OpsAssetListResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.list_assets(&query).await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/assets/{ticker}",
    tag = "Operations",
    params(
        ("ticker" = String, Path, description = "Asset ticker"),
        OpsAssetDetailQuery
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Asset detail for the admin drawer", body = OpsAssetDetailResponse),
        (status = 400, description = "Invalid asset lookup", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 404, description = "Asset not found", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn get_asset_detail(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(ticker): Path<String>,
    Query(query): Query<OpsAssetDetailQuery>,
) -> Result<Json<OpsAssetDetailResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .get_asset_detail(&ticker, &query)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/ops/assets/sync",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Currencies synced from Trocador", body = OpsSyncResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse),
        (status = 502, description = "Trocador error", body = AdminErrorResponse)
    )
)]
pub async fn sync_assets(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsSyncResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let gateway = TrocadorGateway::from_env()
        .map_err(|_| map_admin_error(AdminError::Config("TROCADOR_API_KEY not set".to_string())))?;
    let crud = swap_crud(&state);
    let synced_count = crud
        .sync_currencies_from_trocador(&gateway)
        .await
        .map_err(|error| map_admin_error(AdminError::External(error.to_string())))?;

    Ok(Json(OpsSyncResponse {
        generated_at: Utc::now().to_rfc3339(),
        synced_count,
        target: "assets".to_string(),
    }))
}

#[utoipa::path(
    post,
    path = "/ops/assets/validate-address",
    tag = "Operations",
    request_body = OpsAssetValidateRequest,
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Asset address validation result", body = OpsAssetValidateResponse),
        (status = 400, description = "Invalid address payload", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse),
        (status = 502, description = "Trocador error", body = AdminErrorResponse)
    )
)]
pub async fn validate_asset_address(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Json(req): Json<OpsAssetValidateRequest>,
) -> Result<Json<OpsAssetValidateResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    if let Err(error) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorResponse::new(error.to_string())),
        ));
    }

    let crud = swap_crud(&state);
    let response = crud
        .validate_address(&ValidateAddressRequest {
            ticker: req.ticker.clone(),
            network: req.network.clone(),
            address: req.address.clone(),
        })
        .await
        .map_err(|error| map_admin_error(AdminError::External(error.to_string())))?;

    Ok(Json(OpsAssetValidateResponse {
        valid: response.valid,
        ticker: response.ticker,
        network: response.network,
        address: response.address,
    }))
}

#[utoipa::path(
    get,
    path = "/ops/catalog/giftcards",
    tag = "Operations",
    params(OpsGiftCardCatalogQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Gift card catalog for the admin dashboard", body = OpsGiftCardCatalogResponse),
        (status = 400, description = "Invalid catalog query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse),
        (status = 502, description = "Trocador error", body = AdminErrorResponse)
    )
)]
pub async fn list_giftcard_catalog(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsGiftCardCatalogQuery>,
) -> Result<Json<OpsGiftCardCatalogResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .list_giftcard_catalog(&query)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/catalog/giftcards/{product_id}",
    tag = "Operations",
    params(
        ("product_id" = String, Path, description = "Gift card product id"),
        OpsGiftCardCatalogDetailQuery
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Gift card catalog item detail", body = OpsGiftCardCatalogDetailResponse),
        (status = 400, description = "Invalid catalog item query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 404, description = "Product not found", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse),
        (status = 502, description = "Trocador error", body = AdminErrorResponse)
    )
)]
pub async fn get_giftcard_catalog_item(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(product_id): Path<String>,
    Query(query): Query<OpsGiftCardCatalogDetailQuery>,
) -> Result<Json<OpsGiftCardCatalogDetailResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .get_giftcard_catalog_item(&product_id, &query)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/providers",
    tag = "Operations",
    params(OpsProviderListQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Provider list for the admin dashboard", body = OpsProviderListResponse),
        (status = 400, description = "Invalid provider query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn list_providers(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsProviderListQuery>,
) -> Result<Json<OpsProviderListResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.list_providers(&query).await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/providers/{provider_id}",
    tag = "Operations",
    params(
        ("provider_id" = String, Path, description = "Provider id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Provider detail for the admin drawer", body = OpsProviderDetailResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 404, description = "Provider not found", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn get_provider_detail(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(provider_id): Path<String>,
) -> Result<Json<OpsProviderDetailResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .get_provider_detail(&provider_id)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/ops/providers/sync",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Providers synced from Trocador", body = OpsSyncResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse),
        (status = 502, description = "Trocador error", body = AdminErrorResponse)
    )
)]
pub async fn sync_providers(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsSyncResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let gateway = TrocadorGateway::from_env()
        .map_err(|_| map_admin_error(AdminError::Config("TROCADOR_API_KEY not set".to_string())))?;
    let crud = swap_crud(&state);
    let synced_count = crud
        .sync_providers_from_trocador(&gateway)
        .await
        .map_err(|error| map_admin_error(AdminError::External(error.to_string())))?;

    Ok(Json(OpsSyncResponse {
        generated_at: Utc::now().to_rfc3339(),
        synced_count,
        target: "providers".to_string(),
    }))
}

#[utoipa::path(
    get,
    path = "/ops/settings",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Runtime settings for the admin dashboard", body = OpsSettingsResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn settings(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsSettingsResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.settings().await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/settings/diagnostics",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Runtime integration diagnostics", body = OpsSettingsDiagnosticsResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn settings_diagnostics(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsSettingsDiagnosticsResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.settings_diagnostics().await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/swaps/export",
    tag = "Operations",
    params(AdminSwapExportQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Swap transaction history export as CSV", content_type = "text/csv", body = String),
        (status = 400, description = "Invalid export query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn export_swaps_csv(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<AdminSwapExportQuery>,
) -> Result<Response, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let csv_bytes = crud
        .export_swaps_csv(&query)
        .await
        .map_err(map_admin_error)?;

    let filename = format!("swaps_export_{}.csv", Utc::now().format("%Y%m%dT%H%M%SZ"));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(csv_bytes))
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AdminErrorResponse::new(error.to_string())),
            )
        })
}

#[utoipa::path(
    get,
    path = "/ops/search",
    tag = "Operations",
    params(OpsSearchQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Search swaps, gift card orders, and support conversations", body = OpsSearchResponse),
        (status = 400, description = "Invalid search query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn global_search(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsSearchQuery>,
) -> Result<Json<OpsSearchResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.global_search(&query).await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/health",
    tag = "Operations",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Provider, worker, queue, and risk health overview", body = OpsHealthResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn ops_health(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
) -> Result<Json<OpsHealthResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.ops_health().await.map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/finance/summary",
    tag = "Operations",
    params(OpsFinanceQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Finance and reporting summary", body = OpsFinanceResponse),
        (status = 400, description = "Invalid finance query", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn finance_summary(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsFinanceQuery>,
) -> Result<Json<OpsFinanceResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .finance_summary(&query)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/webhooks",
    tag = "Operations",
    params(OpsWebhookQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Webhook delivery monitor and retry backlog", body = OpsWebhookMonitorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn webhook_monitor(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<OpsWebhookQuery>,
) -> Result<Json<OpsWebhookMonitorResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .webhook_monitor(&query)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/ops/webhooks/{delivery_id}",
    tag = "Operations",
    params(
        ("delivery_id" = String, Path, description = "Webhook delivery id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Webhook delivery detail", body = OpsWebhookDetailResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 404, description = "Webhook delivery not found", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn webhook_detail(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(delivery_id): Path<String>,
) -> Result<Json<OpsWebhookDetailResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .webhook_detail(&delivery_id)
        .await
        .map_err(map_admin_error)?;
    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/ops/notes",
    tag = "Operations",
    request_body = OpsCreateNoteRequest,
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 201, description = "Support or operations note created", body = OpsNoteResponse),
        (status = 400, description = "Invalid note payload", body = AdminErrorResponse),
        (status = 401, description = "Missing or invalid admin token", body = AdminErrorResponse),
        (status = 403, description = "Admin access required", body = AdminErrorResponse),
        (status = 500, description = "Server error", body = AdminErrorResponse)
    )
)]
pub async fn create_note(
    State(state): State<Arc<AppState>>,
    admin: Admin,
    Json(req): Json<OpsCreateNoteRequest>,
) -> Result<(StatusCode, Json<OpsNoteResponse>), (StatusCode, Json<AdminErrorResponse>)> {
    if let Err(error) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorResponse::new(error.to_string())),
        ));
    }

    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud
        .create_note(&admin.id, &admin.email, &req)
        .await
        .map_err(map_admin_error)?;

    Ok((StatusCode::CREATED, Json(response)))
}

fn map_admin_error(error: AdminError) -> (StatusCode, Json<AdminErrorResponse>) {
    let status = match error {
        AdminError::InvalidCredentials => StatusCode::UNAUTHORIZED,
        AdminError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        AdminError::NotFound(_) => StatusCode::NOT_FOUND,
        AdminError::TokenCreation(_)
        | AdminError::Config(_)
        | AdminError::Database(_)
        | AdminError::Csv(_) => StatusCode::INTERNAL_SERVER_ERROR,
        AdminError::External(_) => StatusCode::BAD_GATEWAY,
    };
    (status, Json(AdminErrorResponse::new(error.to_string())))
}
