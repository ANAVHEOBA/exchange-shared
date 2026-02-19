use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::services::metrics::MetricsRegistry;

/// Handler for GET /metrics endpoint
/// Returns Prometheus metrics in text format
pub async fn get_metrics(
    State(metrics): State<Arc<MetricsRegistry>>,
) -> Response {
    match metrics.export() {
        Ok(output) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; version=0.0.4")],
            output,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to export metrics: {}", e),
        )
            .into_response(),
    }
}

/// Handler for GET /health endpoint
/// Returns basic health check
pub async fn health_check() -> Response {
    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        r#"{"status":"healthy"}"#,
    )
        .into_response()
}
