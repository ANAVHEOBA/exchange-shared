use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::controller::{
    get_giftcard_catalog, get_order_status, get_prepaid_cards, order_giftcard, order_prepaid_card,
    trocador_webhook,
};
use crate::AppState;

pub fn giftcard_public_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(get_giftcard_catalog))
        .route("/order", post(order_giftcard))
        .route("/orders/{trade_id}", get(get_order_status))
        .route("/prepaid", get(get_prepaid_cards))
        .route("/prepaid/order", post(order_prepaid_card))
}

pub fn giftcard_webhook_routes() -> Router<Arc<AppState>> {
    Router::new().route("/webhooks/trocador", post(trocador_webhook))
}
