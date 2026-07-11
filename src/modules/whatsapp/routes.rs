use axum::{routing::get, Router};
use std::sync::Arc;

use super::controller::{
    get_admin_conversation, list_admin_conversations, receive_webhook, update_admin_conversation,
    verify_webhook,
};
use crate::AppState;

pub fn whatsapp_routes() -> Router<Arc<AppState>> {
    Router::new().route("/webhook", get(verify_webhook).post(receive_webhook))
}

pub fn whatsapp_admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ops/conversations", get(list_admin_conversations))
        .route(
            "/ops/conversations/{wa_id}",
            get(get_admin_conversation).patch(update_admin_conversation),
        )
}
