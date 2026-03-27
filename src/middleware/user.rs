use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
};
use std::sync::Arc;

use crate::{
    modules::auth::{crud::UserCrud, model::User as UserModel},
    AppState,
};

pub struct OptionalUser(pub Option<UserModel>);

impl<S> FromRequestParts<S> for OptionalUser
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = Arc::from_ref(state);
        let auth_header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok());

        if let Some(auth_header) = auth_header {
            if let Some(token) = auth_header.strip_prefix("Bearer ") {
                if let Ok(claims) = state.jwt_service.verify_access_token(token) {
                    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
                    if let Ok(user) = crud.find_by_id(&claims.claims.sub).await {
                        return Ok(OptionalUser(user));
                    }
                }
            }
        }

        Ok(OptionalUser(None))
    }
}

pub struct User(pub UserModel);

impl<S> FromRequestParts<S> for User
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let state = Arc::from_ref(state);
        let auth_header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or((StatusCode::UNAUTHORIZED, "Missing authorization header"))?;

        let token = auth_header.strip_prefix("Bearer ").ok_or((
            StatusCode::UNAUTHORIZED,
            "Invalid authorization header format",
        ))?;

        let claims = state
            .jwt_service
            .verify_access_token(token)
            .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid or expired token"))?;

        let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
        let user = crud
            .find_by_id(&claims.claims.sub)
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Failed to fetch user"))?
            .ok_or((StatusCode::UNAUTHORIZED, "User not found"))?;

        Ok(User(user))
    }
}
