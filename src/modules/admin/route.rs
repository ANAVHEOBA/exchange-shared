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
        .route("/dashboard", get(controller::dashboard))
        .route("/overview", get(controller::overview))
        .route("/assets", get(controller::list_assets))
        .route("/assets/sync", post(controller::sync_assets))
        .route(
            "/assets/validate-address",
            post(controller::validate_asset_address),
        )
        .route("/assets/{ticker}", get(controller::get_asset_detail))
        .route("/catalog/giftcards", get(controller::list_giftcard_catalog))
        .route(
            "/catalog/giftcards/{product_id}",
            get(controller::get_giftcard_catalog_item),
        )
        .route("/providers", get(controller::list_providers))
        .route("/providers/sync", post(controller::sync_providers))
        .route(
            "/providers/{provider_id}",
            get(controller::get_provider_detail),
        )
        .route("/settings", get(controller::settings))
        .route(
            "/settings/diagnostics",
            get(controller::settings_diagnostics),
        )
        .route("/swaps/export", get(controller::export_swaps_csv))
        .route("/search", get(controller::global_search))
        .route("/health", get(controller::ops_health))
        .route("/finance/summary", get(controller::finance_summary))
        .route("/webhooks", get(controller::webhook_monitor))
        .route("/webhooks/{delivery_id}", get(controller::webhook_detail))
        .route("/notes", post(controller::create_note))
}
