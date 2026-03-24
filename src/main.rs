use exchange_shared::config::{environment::Config, init_db};
use exchange_shared::services::{
    blockchain::BlockchainListener,
    jwt::JwtService,
    monitor::MonitorEngine,
    redis_cache::RedisService,
    rpc::{build_default_rpc_configs, RpcManager},
};
use std::sync::Arc;
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

    // Initialize production RPC Manager with circuit breaker and health checks
    let rpc_configs = build_default_rpc_configs();
    let rpc_manager = Arc::new(RpcManager::new(rpc_configs));
    tracing::info!("Initialized production RPC Manager with circuit breaker");

    // Start RPC health check loop in background
    let health_check_manager = rpc_manager.clone();
    tokio::spawn(async move {
        health_check_manager.health_check_loop().await;
    });
    tracing::info!("Started RPC health check loop");

    // Start blockchain listener in background (PRIMARY: Direct blockchain monitoring)
    let listener_db = db.clone();
    let listener_mnemonic = config.wallet_mnemonic.clone();
    let listener_rpc = rpc_manager.clone();
    tokio::spawn(async move {
        let listener = BlockchainListener::new(listener_db, listener_rpc)
            .with_wallet_mnemonic(listener_mnemonic);
        listener.run().await;
    });
    tracing::info!("Blockchain listener started (primary fund detection)");

    // Start monitor engine in background (FALLBACK: Trocador polling + blockchain verification)
    let monitor_db = db.clone();
    let monitor_redis = redis_service.clone();
    let monitor_mnemonic = config.wallet_mnemonic.clone();
    let monitor_rpc = rpc_manager.clone();
    tokio::spawn(async move {
        let monitor = MonitorEngine::new(monitor_db, monitor_redis, monitor_mnemonic, monitor_rpc);
        monitor.run().await;
    });
    tracing::info!("Monitor engine started (adaptive polling with mathematical optimization)");

    let app = exchange_shared::create_app(
        db,
        redis_service,
        jwt_service,
        config.wallet_mnemonic,
        rpc_manager,
    )
    .await;

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    tracing::info!("Server running on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}
