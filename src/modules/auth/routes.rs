use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::controller;
use crate::{middleware::user::User as AuthenticatedUser, AppState};

pub fn auth_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let protected_routes = Router::new()
        .route("/logout", post(controller::logout))
        .route("/me", get(controller::me))
        .route_layer(middleware::from_extractor_with_state::<
            AuthenticatedUser,
            Arc<AppState>,
        >(state.clone()));

    Router::new()
        .route("/register", post(controller::register))
        .route("/login", post(controller::login))
        .route("/refresh", post(controller::refresh))
        .route("/forgot-password", post(controller::forgot_password))
        .route("/reset-password", post(controller::reset_password))
        .route("/request-verification", post(controller::request_verification))
        .route(
            "/verify-email",
            get(controller::verify_email_get).post(controller::verify_email),
        )
        .merge(protected_routes)
}
