use axum::{routing::{get, post}, Router};
use std::sync::Arc;

use crate::AppState;
use super::controller::{get_currencies, get_providers, get_rates, create_swap, get_swap_status, validate_address, get_swap_history, get_estimate, get_pairs};

pub fn swap_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/currencies", get(get_currencies))
        .route("/providers", get(get_providers))
        .route("/pairs", get(get_pairs))
        .route("/rates", get(get_rates))
        .route("/estimate", get(get_estimate))
        .route("/create", post(create_swap))
        .route("/history", get(get_swap_history))
        .route("/{id}", get(get_swap_status))
        .route("/validate-address", post(validate_address))
}
