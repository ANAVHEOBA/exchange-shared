use exchange_shared::config::{environment::Config, init_db};
use exchange_shared::services::{jwt::JwtService, redis_cache::RedisService};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "exchange_shared=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = Config::from_env().expect("Failed to load environment configuration");

    let db = init_db().await;
    tracing::info!("Connected to MySQL");

    // Initialize Redis Service
    let redis_service = RedisService::new(&config.redis_url);
    tracing::info!("Connected to Redis");

    let jwt_service = JwtService::new(config.jwt_secret);

    let app = exchange_shared::create_app(db, redis_service, jwt_service).await;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("Server running on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}
