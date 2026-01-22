use axum::{routing::{get, post}, Router};
use std::sync::Arc;

use crate::AppState;
use super::controller::{get_currencies, get_providers, get_rates, create_swap};

pub fn swap_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/currencies", get(get_currencies))
        .route("/providers", get(get_providers))
        .route("/rates", get(get_rates))
        .route("/create", post(create_swap))
        // Other routes to be added later...
}
