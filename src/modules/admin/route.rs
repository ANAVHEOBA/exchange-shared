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
        .route("/overview", get(controller::overview))
        .route("/swaps/export", get(controller::export_swaps_csv))
        .route("/search", get(controller::global_search))
        .route("/health", get(controller::ops_health))
        .route("/finance/summary", get(controller::finance_summary))
        .route("/webhooks", get(controller::webhook_monitor))
        .route("/notes", post(controller::create_note))
}
