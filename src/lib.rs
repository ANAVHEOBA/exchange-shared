pub mod config;
pub mod docs;
pub mod middleware;
pub mod modules;
pub mod services;

use axum::{
    extract::State,
    http::{header, HeaderName, HeaderValue, Method},
    middleware as axum_middleware,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use config::DbPool;
use docs::ApiDoc;
use modules::admin::admin_routes;
use modules::auth::auth_routes;
use modules::giftcard::{
    giftcard_admin_routes, giftcard_public_routes, giftcard_webhook_routes,
    worker::run_giftcard_worker,
};
use modules::swap::{swap_admin_routes, swap_routes};
use modules::whatsapp::{whatsapp_admin_routes, whatsapp_routes, worker::run_whatsapp_worker};
use services::jwt::JwtService;
use services::kimi::KimiClient;
use services::payout_policy::PayoutPolicyConfig;
use services::rate_limit::{create_rate_limiter, RateLimitLayer};
use services::redis_cache::RedisService;
use services::rpc::{RpcHealthOverview, RpcManager};
use services::security::security_headers;
use services::trocador::TrocadorGateway;
use services::whatsapp::WhatsAppService;

pub struct AppState {
    pub db: DbPool,
    pub redis: Option<RedisService>,
    pub http_client: reqwest::Client,
    pub jwt_service: JwtService,
    pub wallet_mnemonic: Option<String>,
    pub email_service: Option<services::email::EmailService>,
    pub whatsapp_service: Option<Arc<WhatsAppService>>,
    pub rpc_manager: Arc<RpcManager>,
    pub payout_policy: PayoutPolicyConfig,
    pub kimi_client: Option<Arc<KimiClient>>,
}

const DEFAULT_CORS_ORIGINS: &[&str] = &[
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:5174",
    "https://exchange-frontend-sand.vercel.app",
    "https://exchangefrontend-admin.vercel.app",
];

pub async fn create_app(
    db: DbPool,
    redis: Option<RedisService>,
    jwt_service: JwtService,
    wallet_mnemonic: Option<String>,
    rpc_manager: Arc<RpcManager>,
    payout_policy: PayoutPolicyConfig,
) -> Router {
    // Initialize email service (optional, won't fail if not configured)
    let email_service = services::email::EmailService::from_env().ok();
    if email_service.is_none() {
        tracing::warn!("⚠️  Email service not configured - email verification will be disabled");
    } else {
        tracing::info!(
            "✉️  Email service configured successfully (provider={})",
            email_service.as_ref().unwrap().provider_name()
        );
    }

    let http_client = reqwest::Client::new();
    let whatsapp_service = match WhatsAppService::from_env(http_client.clone()) {
        Ok(service) => {
            if let Some(service) = service.as_ref() {
                let webhook_url = service
                    .config()
                    .webhook_url()
                    .unwrap_or_else(|| "<public URL not configured>".to_string());
                tracing::info!(
                    "📲 WhatsApp service configured (phone_number_id={}, webhook_url={})",
                    service.config().phone_number_id,
                    webhook_url
                );
                if webhook_url.starts_with("http://") {
                    tracing::warn!(
                        "⚠️  WhatsApp webhook URL is not HTTPS; Meta webhook verification will fail until PUBLIC_BACKEND_URL/RENDER_EXTERNAL_URL/API_BASE_URL is switched to https://"
                    );
                }
            } else {
                tracing::info!("WhatsApp service not configured");
            }
            service.map(Arc::new)
        }
        Err(error) => {
            tracing::warn!("⚠️  WhatsApp service disabled: {}", error);
            None
        }
    };

    let kimi_client = KimiClient::from_env().map(Arc::new);
    if kimi_client.is_some() {
        tracing::info!("🤖 Kimi conversational layer configured for WhatsApp");
    } else {
        tracing::info!("Kimi conversational layer not configured - WhatsApp falls back to the deterministic menu only");
    }

    let state = Arc::new(AppState {
        db,
        redis,
        http_client,
        jwt_service,
        wallet_mnemonic,
        email_service,
        whatsapp_service,
        kimi_client,
        rpc_manager,
        payout_policy,
    });

    {
        let worker_state = state.clone();
        tokio::spawn(async move {
            run_giftcard_worker(worker_state).await;
        });
        tracing::info!("Started gift card worker");
    }

    if state.whatsapp_service.is_some() {
        let worker_state = state.clone();
        tokio::spawn(async move {
            run_whatsapp_worker(worker_state).await;
        });
        tracing::info!("Started WhatsApp worker");
    }

    let public_rate_limiter = create_rate_limiter(100);
    let giftcard_rate_limiter = create_rate_limiter(20);
    let giftcard_webhook_rate_limiter = create_rate_limiter(120);
    let whatsapp_rate_limiter = create_rate_limiter(300);

    let giftcard_routes = Router::new()
        .nest(
            "/giftcards",
            giftcard_public_routes().layer(RateLimitLayer::new(giftcard_rate_limiter)),
        )
        .nest(
            "/giftcards",
            giftcard_webhook_routes().layer(RateLimitLayer::new(giftcard_webhook_rate_limiter)),
        );

    let public_routes = Router::new()
        .route("/", get(root))
        .route("/ping", get(ping))
        .route("/branding/assetar-logo.jpg", get(assetar_logo_legacy))
        .route("/branding/assetar-logo.png", get(assetar_logo))
        .route("/health", get(health_check))
        .nest("/ops", admin_routes())
        .nest("/swap", swap_admin_routes())
        .nest("/giftcards", giftcard_admin_routes())
        .nest("/whatsapp", whatsapp_admin_routes())
        .nest("/auth", auth_routes(state.clone()))
        .nest("/swap", swap_routes())
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(RateLimitLayer::new(public_rate_limiter));

    let whatsapp_routes = Router::new()
        .nest("/whatsapp", whatsapp_routes())
        .layer(RateLimitLayer::new(whatsapp_rate_limiter));

    let app = Router::new()
        .merge(public_routes)
        .merge(giftcard_routes)
        .merge(whatsapp_routes)
        .layer(axum_middleware::from_fn(
            crate::middleware::client_identity::attach_client_identity,
        ))
        .layer(axum_middleware::from_fn(security_headers))
        .layer(RequestBodyLimitLayer::new(1024 * 100)) // 100KB max body
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    match configured_cors_layer().expect("Failed to parse CORS_ORIGINS") {
        Some(cors_layer) => app.layer(cors_layer),
        None => app,
    }
}

#[utoipa::path(
    get,
    path = "/",
    tag = "System",
    responses(
        (status = 200, description = "API root", body = String, example = "Exchange Platform API")
    )
)]
async fn root() -> &'static str {
    "Exchange Platform API"
}

#[utoipa::path(
    get,
    path = "/ping",
    tag = "System",
    responses(
        (status = 200, description = "Lightweight keep-alive endpoint", body = String, example = "pong")
    )
)]
async fn ping() -> &'static str {
    "pong"
}

#[utoipa::path(
    get,
    path = "/branding/assetar-logo.png",
    tag = "System",
    responses(
        (status = 200, description = "Assetar PNG logo", content_type = "image/png", body = String)
    )
)]
async fn assetar_logo() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        include_bytes!("../assetar-logo-rgb.png").as_slice(),
    )
}

#[utoipa::path(
    get,
    path = "/branding/assetar-logo.jpg",
    tag = "System",
    responses(
        (status = 200, description = "Legacy Assetar JPEG logo", content_type = "image/jpeg", body = String)
    )
)]
async fn assetar_logo_legacy() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/jpeg"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        include_bytes!("../assetar logo with name 2.jpg").as_slice(),
    )
}

#[derive(Serialize, ToSchema)]
struct HealthResponse {
    status: String,
    version: &'static str,
    checks: HealthChecks,
}

#[derive(Serialize, ToSchema)]
struct HealthChecks {
    database: DependencyHealth,
    redis: DependencyHealth,
    trocador: DependencyHealth,
    rpc: RpcDependencyHealth,
}

#[derive(Serialize, ToSchema)]
struct DependencyHealth {
    status: String,
    latency_ms: u128,
    details: Option<String>,
}

#[derive(Serialize, ToSchema)]
struct RpcDependencyHealth {
    status: String,
    configured_chains: usize,
    total_endpoints: usize,
    sampled_endpoints: usize,
    healthy_endpoints: usize,
    details: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

impl HealthState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "ok",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

#[utoipa::path(
    get,
    path = "/health",
    tag = "System",
    responses(
        (status = 200, description = "Health check status", body = HealthResponse),
        (status = 503, description = "Health check status with one or more failing dependencies", body = HealthResponse)
    )
)]
async fn health_check(
    State(state): State<Arc<AppState>>,
) -> (axum::http::StatusCode, Json<HealthResponse>) {
    let (database, redis, trocador, rpc) = tokio::join!(
        check_database(&state),
        check_redis(&state),
        check_trocador(),
        check_rpc(&state),
    );

    let overall = overall_health_status(
        &database.status,
        &redis.status,
        &trocador.status,
        &rpc.status,
    );
    let http_status = if overall == HealthState::Unhealthy {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    } else {
        axum::http::StatusCode::OK
    };

    (
        http_status,
        Json(HealthResponse {
            status: overall.as_str().to_string(),
            version: env!("CARGO_PKG_VERSION"),
            checks: HealthChecks {
                database,
                redis,
                trocador,
                rpc,
            },
        }),
    )
}

async fn check_database(state: &AppState) -> DependencyHealth {
    let start = tokio::time::Instant::now();
    let result = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, async {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&state.db)
            .await
            .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(_)) => DependencyHealth {
            status: HealthState::Healthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: None,
        },
        Ok(Err(err)) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some(err),
        },
        Err(_) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some("Timed out while checking MySQL".to_string()),
        },
    }
}

async fn check_redis(state: &AppState) -> DependencyHealth {
    let Some(redis) = &state.redis else {
        return DependencyHealth {
            status: "disabled".to_string(),
            latency_ms: 0,
            details: Some("Redis is not configured".to_string()),
        };
    };

    let start = tokio::time::Instant::now();
    let result = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, async {
        let mut conn = redis
            .get_client()
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| e.to_string())?;

        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e: redis::RedisError| e.to_string())?;

        if pong.eq_ignore_ascii_case("PONG") {
            Ok(())
        } else {
            Err(format!("Unexpected Redis PING response: {}", pong))
        }
    })
    .await;

    match result {
        Ok(Ok(())) => DependencyHealth {
            status: HealthState::Healthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: None,
        },
        Ok(Err(err)) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some(err),
        },
        Err(_) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some("Timed out while checking Redis".to_string()),
        },
    }
}

async fn check_trocador() -> DependencyHealth {
    let start = tokio::time::Instant::now();
    let result = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, async {
        let gateway =
            TrocadorGateway::from_env().map_err(|_| "TROCADOR_API_KEY not set".to_string())?;
        let providers = gateway.fetch_providers().await.map_err(|e| e.to_string())?;

        if providers.is_empty() {
            Err("Trocador provider list was empty".to_string())
        } else {
            Ok(())
        }
    })
    .await;

    match result {
        Ok(Ok(())) => DependencyHealth {
            status: HealthState::Healthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: None,
        },
        Ok(Err(err)) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some(err),
        },
        Err(_) => DependencyHealth {
            status: HealthState::Unhealthy.as_str().to_string(),
            latency_ms: start.elapsed().as_millis(),
            details: Some("Timed out while checking Trocador".to_string()),
        },
    }
}

async fn check_rpc(state: &AppState) -> RpcDependencyHealth {
    let overview = state.rpc_manager.health_overview().await;
    let status = rpc_health_state(&overview);
    let details = match status {
        HealthState::Healthy => None,
        HealthState::Degraded => Some(
            "Some RPC endpoints are healthy, but coverage is partial or still warming up"
                .to_string(),
        ),
        HealthState::Unhealthy => {
            Some("No healthy RPC endpoints are currently available".to_string())
        }
    };

    RpcDependencyHealth {
        status: status.as_str().to_string(),
        configured_chains: overview.configured_chains,
        total_endpoints: overview.total_endpoints,
        sampled_endpoints: overview.sampled_endpoints,
        healthy_endpoints: overview.healthy_endpoints,
        details,
    }
}

fn rpc_health_state(overview: &RpcHealthOverview) -> HealthState {
    if overview.total_endpoints == 0 {
        HealthState::Unhealthy
    } else if overview.sampled_endpoints == 0 {
        HealthState::Degraded
    } else if overview.healthy_endpoints == 0 {
        HealthState::Unhealthy
    } else if overview.healthy_endpoints < overview.total_endpoints {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    }
}

fn overall_health_status(database: &str, redis: &str, trocador: &str, rpc: &str) -> HealthState {
    let states = [database, trocador, rpc];
    if states.contains(&HealthState::Unhealthy.as_str()) {
        HealthState::Unhealthy
    } else if states.contains(&HealthState::Degraded.as_str())
        || redis == HealthState::Degraded.as_str()
    {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    }
}

fn configured_cors_layer() -> Result<Option<CorsLayer>, String> {
    let configured_origins = std::env::var("CORS_ORIGINS").unwrap_or_default();
    let raw_origins = if configured_origins.trim().is_empty() {
        DEFAULT_CORS_ORIGINS.join(",")
    } else {
        format!(
            "{},{}",
            DEFAULT_CORS_ORIGINS.join(","),
            configured_origins.trim()
        )
    };
    let origins = parse_cors_origins(&raw_origins)?;

    if origins.is_empty() {
        tracing::warn!(
            "No valid CORS origins were configured. Cross-origin browser access is disabled."
        );
        return Ok(None);
    }

    tracing::info!("Configured CORS allowlist with {} origin(s)", origins.len());

    Ok(Some(
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([
                header::AUTHORIZATION,
                header::CONTENT_TYPE,
                header::ACCEPT,
                HeaderName::from_static("x-client-id"),
            ])
            .expose_headers([HeaderName::from_static("x-client-id")]),
    ))
}

fn parse_cors_origins(raw: &str) -> Result<Vec<HeaderValue>, String> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::new();

    for origin in raw
        .split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
    {
        if !seen.insert(origin.to_string()) {
            continue;
        }

        parsed.push(
            HeaderValue::from_str(origin)
                .map_err(|e| format!("Invalid CORS origin '{}': {}", origin, e))?,
        );
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::{overall_health_status, parse_cors_origins, rpc_health_state, HealthState};
    use crate::services::rpc::RpcHealthOverview;

    #[test]
    fn parses_comma_separated_cors_origins() {
        let origins = parse_cors_origins("https://app.example.com, http://localhost:5173")
            .expect("Origins should parse");

        assert_eq!(origins.len(), 2);
    }

    #[test]
    fn ignores_empty_cors_entries() {
        let origins =
            parse_cors_origins(" , https://app.example.com ,, ").expect("Origins should parse");

        assert_eq!(origins.len(), 1);
    }

    #[test]
    fn de_duplicates_repeated_cors_entries() {
        let origins = parse_cors_origins(
            "https://exchangefrontend-admin.vercel.app, https://exchangefrontend-admin.vercel.app",
        )
        .expect("Origins should parse");

        assert_eq!(origins.len(), 1);
    }

    #[test]
    fn rejects_invalid_cors_origin_values() {
        let err = parse_cors_origins("https://good.example.com,\ninvalid")
            .expect_err("Invalid header value should fail");

        assert!(err.contains("Invalid CORS origin"));
    }

    #[test]
    fn rpc_health_is_degraded_when_no_samples_exist_yet() {
        let overview = RpcHealthOverview {
            configured_chains: 3,
            total_endpoints: 6,
            sampled_endpoints: 0,
            healthy_endpoints: 0,
        };

        assert_eq!(rpc_health_state(&overview), HealthState::Degraded);
    }

    #[test]
    fn overall_health_is_unhealthy_when_any_required_dependency_fails() {
        assert_eq!(
            overall_health_status("ok", "ok", "unhealthy", "degraded"),
            HealthState::Unhealthy
        );
    }
}
