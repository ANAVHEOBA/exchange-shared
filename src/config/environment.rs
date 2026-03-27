use std::env;

use crate::services::payout_policy::PayoutPolicyConfig;

/// Environment configuration
/// Loads and validates environment variables
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub port: u16,
    pub jwt_secret: String,
    pub trocador_api_key: String,
    pub wallet_mnemonic: String,

    // RPC Configuration
    pub alchemy_api_key: Option<String>,
    pub infura_api_key: Option<String>,
    pub quicknode_api_key: Option<String>,
    pub blockfrost_api_key: Option<String>,
    pub drpc_api_key: Option<String>,

    // RPC Performance Settings
    pub rpc_timeout_ms: u64,
    pub rpc_retry_attempts: u32,
    pub rpc_retry_delay_ms: u64,
    pub rpc_max_concurrent: usize,
    pub rpc_cache_enabled: bool,
    pub rpc_cache_ttl_seconds: u64,
    pub rpc_log_enabled: bool,
    pub payout_policy: PayoutPolicyConfig,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        dotenvy::dotenv().ok();

        let database_url =
            env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set".to_string())?;

        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

        let port = env::var("PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3000);

        let jwt_secret =
            env::var("JWT_SECRET").map_err(|_| "JWT_SECRET must be set".to_string())?;

        let trocador_api_key =
            env::var("TROCADOR_API_KEY").map_err(|_| "TROCADOR_API_KEY must be set".to_string())?;

        let wallet_mnemonic =
            env::var("WALLET_MNEMONIC").map_err(|_| "WALLET_MNEMONIC must be set".to_string())?;

        // RPC API Keys (all optional)
        let alchemy_api_key = env::var("ALCHEMY_API_KEY").ok();
        let infura_api_key = env::var("INFURA_API_KEY").ok();
        let quicknode_api_key = env::var("QUICKNODE_API_KEY").ok();
        let blockfrost_api_key = env::var("BLOCKFROST_API_KEY").ok();
        let drpc_api_key = env::var("DRPC_API_KEY").ok();

        // RPC Performance Settings with defaults
        let rpc_timeout_ms = env::var("RPC_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10000);

        let rpc_retry_attempts = env::var("RPC_RETRY_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);

        let rpc_retry_delay_ms = env::var("RPC_RETRY_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);

        let rpc_max_concurrent = env::var("RPC_MAX_CONCURRENT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);

        let rpc_cache_enabled = env::var("RPC_CACHE_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);

        let rpc_cache_ttl_seconds = env::var("RPC_CACHE_TTL_SECONDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);

        let rpc_log_enabled = env::var("RPC_LOG_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(false);
        let payout_policy = PayoutPolicyConfig::from_env();

        // Log RPC configuration status
        if alchemy_api_key.is_some() {
            tracing::info!("✅ Alchemy API key detected - 70+ chains will auto-configure");
        } else {
            tracing::warn!(
                "⚠️  No Alchemy API key - using public endpoints (slower, rate limited)"
            );
            tracing::warn!("   Get free API key at: https://www.alchemy.com");
        }

        if infura_api_key.is_some() {
            tracing::info!("✅ Infura API key detected - available as fallback");
        }

        if drpc_api_key.is_some() {
            tracing::info!("✅ dRPC API key detected - available as fallback");
        }

        tracing::info!(
            "🧭 Payout policy loaded: local_certified={} trocador_only_overrides={}",
            payout_policy.local_certified_chain_keys().join(","),
            payout_policy.trocador_only_chain_keys().join(",")
        );

        Ok(Self {
            database_url,
            redis_url,
            port,
            jwt_secret,
            trocador_api_key,
            wallet_mnemonic,
            alchemy_api_key,
            infura_api_key,
            quicknode_api_key,
            blockfrost_api_key,
            drpc_api_key,
            rpc_timeout_ms,
            rpc_retry_attempts,
            rpc_retry_delay_ms,
            rpc_max_concurrent,
            rpc_cache_enabled,
            rpc_cache_ttl_seconds,
            rpc_log_enabled,
            payout_policy,
        })
    }

    pub fn trocador_api_key(&self) -> &str {
        &self.trocador_api_key
    }
}
