use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::controller;
use crate::AppState;

pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/register", post(controller::register))
        .route("/login", post(controller::login))
        .route(
            "/verify-email",
            get(controller::verify_email_get).post(controller::verify_email),
        )
}
