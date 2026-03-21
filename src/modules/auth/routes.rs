use axum::{routing::{post, get}, Router};
use std::sync::Arc;

use crate::AppState;
use super::controller;

pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/register", post(controller::register))
        .route("/login", post(controller::login))
        .route("/verify-email", get(controller::verify_email))
}
