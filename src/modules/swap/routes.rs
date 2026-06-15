use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::controller::{
    create_donation_swap, create_swap, get_client_swap_history, get_currencies, get_donation_rates,
    get_donation_target, get_estimate, get_pairs, get_providers, get_rates, get_swap_history,
    get_swap_status, trocador_webhook, validate_address,
};
use crate::AppState;

pub fn swap_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/currencies", get(get_currencies))
        .route("/providers", get(get_providers))
        .route("/pairs", get(get_pairs))
        .route("/rates", get(get_rates))
        .route("/estimate", get(get_estimate))
        .route("/create", post(create_swap))
        .route("/donation/target", get(get_donation_target))
        .route("/donation/rates", get(get_donation_rates))
        .route("/donation/create", post(create_donation_swap))
        .route("/webhooks/trocador", post(trocador_webhook))
        .route("/history", get(get_swap_history))
        .route("/history/client", get(get_client_swap_history))
        .route("/{id}", get(get_swap_status))
        .route("/validate-address", post(validate_address))
}
