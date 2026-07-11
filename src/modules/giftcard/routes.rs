use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;

use super::controller::{
    admin_get_order, admin_list_orders, admin_reconcile_order, admin_retry_order,
    admin_reveal_order, get_giftcard_catalog, get_order_status, get_prepaid_cards, order_giftcard,
    order_prepaid_card, trocador_webhook,
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

pub fn giftcard_admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ops/orders", get(admin_list_orders))
        .route("/ops/orders/{order_ref}", get(admin_get_order))
        .route("/ops/orders/{order_ref}/retry", post(admin_retry_order))
        .route(
            "/ops/orders/{order_ref}/reconcile",
            post(admin_reconcile_order),
        )
        .route("/ops/orders/{order_ref}/reveal", post(admin_reveal_order))
}
