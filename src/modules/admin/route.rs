use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use super::controller;
use crate::AppState;

pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/login", post(controller::login))
        .route("/swaps/export", get(controller::export_swaps_csv))
}
