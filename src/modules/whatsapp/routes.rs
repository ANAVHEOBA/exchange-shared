use axum::{routing::get, Router};
use std::sync::Arc;

use super::controller::{receive_webhook, verify_webhook};
use crate::AppState;

pub fn whatsapp_routes() -> Router<Arc<AppState>> {
    Router::new().route("/webhook", get(verify_webhook).post(receive_webhook))
}
