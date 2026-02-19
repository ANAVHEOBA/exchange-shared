use axum::{
    routing::get,
    Router,
};
use std::sync::Arc;

use crate::services::metrics::MetricsRegistry;
use super::controller::{get_metrics, health_check};

pub fn metrics_routes(metrics: Arc<MetricsRegistry>) -> Router {
    Router::new()
        .route("/metrics", get(get_metrics))
        .route("/health", get(health_check))
        .with_state(metrics)
}
