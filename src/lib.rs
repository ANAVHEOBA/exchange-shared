pub mod config;
pub mod modules;
pub mod services;

use axum::{middleware, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};

use config::DbPool;
use modules::auth::auth_routes;
use modules::swap::swap_routes;
use services::jwt::JwtService;
use services::rate_limit::{create_rate_limiter, RateLimitLayer};
use services::security::security_headers;
use services::redis_cache::RedisService;

pub struct AppState {
    pub db: DbPool,
    pub redis: RedisService, // Changed from redis::Client
    pub http_client: reqwest::Client,
    pub jwt_service: JwtService,
}

pub async fn create_app(db: DbPool, redis: RedisService, jwt_service: JwtService) -> Router {
    let state = Arc::new(AppState {
        db,
        redis,
        http_client: reqwest::Client::new(),
        jwt_service,
    });

    // Rate limit: burst of 10, then 1 per minute
    let rate_limiter = create_rate_limiter(10);

    Router::new()
        .route("/", get(root))
        .route("/health", get(health_check))
        .nest("/auth", auth_routes())
        .nest("/swap", swap_routes())
        .layer(middleware::from_fn(security_headers))
        .layer(RequestBodyLimitLayer::new(1024 * 100)) // 100KB max body
        .layer(RateLimitLayer::new(rate_limiter))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn root() -> &'static str {
    "Exchange Platform API"
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}
