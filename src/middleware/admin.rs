use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    Json,
};
use std::sync::Arc;

use crate::{
    modules::admin::{model::AdminAccount, schema::AdminErrorResponse},
    AppState,
};

#[derive(Debug, Clone)]
pub struct Admin {
    pub id: String,
    pub email: String,
}

impl<S> FromRequestParts<S> for Admin
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, Json<AdminErrorResponse>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = Arc::from_ref(state);
        let auth_header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or((
                StatusCode::UNAUTHORIZED,
                Json(AdminErrorResponse::new("Missing authorization header")),
            ))?;

        let token = auth_header.strip_prefix("Bearer ").ok_or((
            StatusCode::UNAUTHORIZED,
            Json(AdminErrorResponse::new(
                "Invalid authorization header format",
            )),
        ))?;

        let claims = state.jwt_service.verify_access_token(token).map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                Json(AdminErrorResponse::new("Invalid or expired token")),
            )
        })?;

        let account = AdminAccount::from_env();
        if claims.claims.sub != account.id
            || !claims.claims.email.eq_ignore_ascii_case(&account.email)
        {
            return Err((
                StatusCode::FORBIDDEN,
                Json(AdminErrorResponse::new("Admin access required")),
            ));
        }

        Ok(Admin {
            id: account.id,
            email: account.email,
        })
    }
}
