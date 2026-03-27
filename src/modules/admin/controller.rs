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
    schema::{AdminErrorResponse, AdminLoginRequest, AdminLoginResponse, AdminSwapExportQuery},
};
use crate::middleware::admin::Admin;
use crate::AppState;

#[utoipa::path(
    post,
    path = "/admin/login",
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
    path = "/admin/swaps/export",
    tag = "Admin",
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
