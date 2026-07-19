use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Query, State},
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
        AdminSwapExportQuery, OpsCreateNoteRequest, OpsDashboardResponse, OpsFinanceQuery,
        OpsFinanceResponse, OpsHealthResponse, OpsNoteResponse, OpsSearchQuery, OpsSearchResponse,
        OpsWebhookMonitorResponse,
    },
};
use crate::middleware::admin::Admin;
use crate::AppState;

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
        .map_err(|error| {
            let status = match error {
                AdminError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
                AdminError::InvalidCredentials => StatusCode::UNAUTHORIZED,
                AdminError::TokenCreation(_) | AdminError::Database(_) | AdminError::Csv(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            (status, Json(AdminErrorResponse::new(error.to_string())))
        })?;

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
    let response = crud.overview().await.map_err(|error| {
        let status = match error {
            AdminError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AdminError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            AdminError::TokenCreation(_) | AdminError::Database(_) | AdminError::Csv(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(AdminErrorResponse::new(error.to_string())))
    })?;

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
    let csv_bytes = crud.export_swaps_csv(&query).await.map_err(|error| {
        let status = match error {
            AdminError::InvalidCredentials => StatusCode::UNAUTHORIZED,
            AdminError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            AdminError::TokenCreation(_) | AdminError::Database(_) | AdminError::Csv(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(AdminErrorResponse::new(error.to_string())))
    })?;

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
) -> Result<Json<OpsWebhookMonitorResponse>, (StatusCode, Json<AdminErrorResponse>)> {
    let crud = AdminCrud::new(state.db.clone(), &state.jwt_service);
    let response = crud.webhook_monitor().await.map_err(map_admin_error)?;
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
        AdminError::TokenCreation(_) | AdminError::Database(_) | AdminError::Csv(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    (status, Json(AdminErrorResponse::new(error.to_string())))
}
