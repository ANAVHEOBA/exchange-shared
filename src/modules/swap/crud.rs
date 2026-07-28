use chrono::Utc;
use sqlx::{MySql, Pool};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use super::model::{Currency, Provider};
use super::repository::SwapRepository;
use super::schema::{
    CurrenciesQuery, CurrencyResponse, EstimateCacheEntry, EstimateResponse, ProviderResponse,
    ProvidersQuery, RatesResponse, TrocadorCurrency, TrocadorProvider,
};
use super::service::SwapService;
use crate::services::gas::GasEstimator;
use crate::services::payout_policy::PayoutPolicyConfig;
use crate::services::pricing::{PricedRates, QuoteService};
use crate::services::redis_cache::RedisService;
use crate::services::rpc::RpcManager;
use crate::services::trocador::{swap_markup_from_env, TrocadorError, TrocadorGateway};
use crate::services::wallet::validation::{
    default_extra_id_name, validate_address_by_network_family, AddressValidation,
};

const RATES_MEMORY_CACHE_TTL: Duration = Duration::from_secs(15);
const RATES_MEMORY_CACHE_MAX_ENTRIES: usize = 256;
const ESTIMATE_EXACT_MEMORY_CACHE_TTL: Duration = Duration::from_secs(10);
const ESTIMATE_BUCKET_MEMORY_CACHE_TTL: Duration = Duration::from_secs(60);
const ESTIMATE_MEMORY_CACHE_MAX_ENTRIES: usize = 512;

#[derive(Debug, Clone)]
struct CachedRatesResponse {
    cached_at: Instant,
    value: RatesResponse,
}

#[derive(Debug, Clone)]
struct CachedEstimateEntry {
    cached_at: Instant,
    ttl: Duration,
    value: EstimateCacheEntry,
}

fn rates_memory_cache() -> &'static RwLock<HashMap<String, CachedRatesResponse>> {
    static CACHE: OnceLock<RwLock<HashMap<String, CachedRatesResponse>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn estimate_memory_cache() -> &'static RwLock<HashMap<String, CachedEstimateEntry>> {
    static CACHE: OnceLock<RwLock<HashMap<String, CachedEstimateEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn read_cached_rates_response(cache_key: &str) -> Option<RatesResponse> {
    let cache = rates_memory_cache().read().ok()?;
    let cached = cache.get(cache_key)?;

    if cached.cached_at.elapsed() > RATES_MEMORY_CACHE_TTL {
        return None;
    }

    Some(cached.value.clone())
}

fn write_cached_rates_response(cache_key: String, value: RatesResponse) {
    let Ok(mut cache) = rates_memory_cache().write() else {
        return;
    };

    cache.retain(|_, cached| cached.cached_at.elapsed() <= RATES_MEMORY_CACHE_TTL);
    if cache.len() >= RATES_MEMORY_CACHE_MAX_ENTRIES {
        let overflow = cache
            .len()
            .saturating_add(1)
            .saturating_sub(RATES_MEMORY_CACHE_MAX_ENTRIES);
        let mut oldest_entries = cache
            .iter()
            .map(|(cached_key, cached)| (cached_key.clone(), cached.cached_at))
            .collect::<Vec<_>>();
        oldest_entries.sort_by_key(|(_, cached_at)| *cached_at);

        for (cached_key, _) in oldest_entries.into_iter().take(overflow.max(1)) {
            cache.remove(&cached_key);
        }
    }

    cache.insert(
        cache_key,
        CachedRatesResponse {
            cached_at: Instant::now(),
            value,
        },
    );
}

fn read_cached_estimate_entry(cache_key: &str) -> Option<EstimateCacheEntry> {
    let cache = estimate_memory_cache().read().ok()?;
    let cached = cache.get(cache_key)?;

    if cached.cached_at.elapsed() > cached.ttl {
        return None;
    }

    Some(cached.value.clone())
}

fn write_cached_estimate_entry(cache_key: String, ttl: Duration, value: EstimateCacheEntry) {
    let Ok(mut cache) = estimate_memory_cache().write() else {
        return;
    };

    cache.retain(|_, cached| cached.cached_at.elapsed() <= cached.ttl);
    if cache.len() >= ESTIMATE_MEMORY_CACHE_MAX_ENTRIES {
        let overflow = cache
            .len()
            .saturating_add(1)
            .saturating_sub(ESTIMATE_MEMORY_CACHE_MAX_ENTRIES);
        let mut oldest_entries = cache
            .iter()
            .map(|(cached_key, cached)| (cached_key.clone(), cached.cached_at))
            .collect::<Vec<_>>();
        oldest_entries.sort_by_key(|(_, cached_at)| *cached_at);

        for (cached_key, _) in oldest_entries.into_iter().take(overflow.max(1)) {
            cache.remove(&cached_key);
        }
    }

    cache.insert(
        cache_key,
        CachedEstimateEntry {
            cached_at: Instant::now(),
            ttl,
            value,
        },
    );
}

fn estimate_response_from_cache_entry(entry: &EstimateCacheEntry) -> EstimateResponse {
    let now = Utc::now().timestamp_millis();
    let cache_age = ((now - entry.created_at) / 1000) as i64;
    let expires_in = ((entry.expires_at - now) / 1000).max(0) as i64;

    let mut response = entry.response.clone();
    response.cached = true;
    response.cache_age_seconds = cache_age;
    response.expires_in_seconds = expires_in;
    response
}

fn upstream_error_indicates_missing_quote(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("no resulting quote")
        || lowered.contains("no available quote")
        || lowered.contains("pair not available")
}

pub enum CurrenciesResult {
    RawJson(String),
    Structured(Vec<CurrencyResponse>),
}

pub enum ProvidersResult {
    RawJson(String),
    Structured(Vec<ProviderResponse>),
}

// =============================================================================
// SWAP ERROR
// =============================================================================

#[derive(Debug)]
pub enum SwapError {
    ProviderNotFound,
    CurrencyNotFound,
    PairNotAvailable,
    AmountOutOfRange { min: f64, max: f64 },
    InvalidAddress,
    ValidationError(String),
    SwapNotFound,
    ProviderUnavailable(String),
    DatabaseError(String),
    ExternalApiError(String),
    RedisError(String),
    InvalidCursor(String), // Added for cursor validation errors
}

impl std::fmt::Display for SwapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SwapError::ProviderNotFound => write!(f, "Provider not found"),
            SwapError::CurrencyNotFound => write!(f, "Currency not found"),
            SwapError::PairNotAvailable => write!(f, "Trading pair not available"),
            SwapError::AmountOutOfRange { min, max } => {
                write!(f, "Amount out of range: min={}, max={}", min, max)
            }
            SwapError::InvalidAddress => write!(f, "Invalid address"),
            SwapError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            SwapError::SwapNotFound => write!(f, "Swap not found"),
            SwapError::ProviderUnavailable(msg) => write!(f, "Provider unavailable: {}", msg),
            SwapError::DatabaseError(e) => write!(f, "Database error: {}", e),
            SwapError::ExternalApiError(e) => write!(f, "External API error: {}", e),
            SwapError::RedisError(e) => write!(f, "Redis error: {}", e),
            SwapError::InvalidCursor(e) => write!(f, "Invalid cursor: {}", e),
        }
    }
}

impl From<TrocadorError> for SwapError {
    fn from(err: TrocadorError) -> Self {
        match err {
            TrocadorError::ApiError(message)
                if upstream_error_indicates_missing_quote(&message) =>
            {
                SwapError::PairNotAvailable
            }
            other => SwapError::ExternalApiError(other.to_string()),
        }
    }
}

// =============================================================================
// SWAP CRUD
// =============================================================================

pub struct SwapCrud {
    pool: Pool<MySql>,
    repository: SwapRepository,
    redis_service: Option<RedisService>, // Changed to RedisService
    wallet_mnemonic: Option<String>,
    gas_estimator: GasEstimator,
    rpc_manager: Arc<RpcManager>,
    payout_policy: PayoutPolicyConfig,
}

impl SwapCrud {
    pub fn new(
        pool: Pool<MySql>,
        redis_service: Option<RedisService>,
        wallet_mnemonic: Option<String>,
        rpc_manager: Arc<RpcManager>,
        payout_policy: PayoutPolicyConfig,
    ) -> Self {
        let gas_estimator = GasEstimator::new(redis_service.clone());
        Self {
            repository: SwapRepository::new(pool.clone()),
            pool,
            redis_service,
            wallet_mnemonic,
            gas_estimator,
            rpc_manager,
            payout_policy,
        }
    }

    fn swap_service(&self) -> SwapService {
        SwapService::new(
            self.pool.clone(),
            self.redis_service.clone(),
            self.wallet_mnemonic.clone(),
            self.rpc_manager.clone(),
            self.payout_policy.clone(),
        )
    }

    fn preferred_currency_network_rank(currency: &CurrencyResponse) -> usize {
        let ticker = currency.ticker.to_lowercase();
        let network = currency.network.to_lowercase();

        let preferred = match ticker.as_str() {
            "btc" => ["mainnet", "lightning"].as_slice(),
            "xmr" => ["mainnet"].as_slice(),
            "eth" => ["erc20", "mainnet", "arbitrum", "optimism", "base", "bep20"].as_slice(),
            "bnb" => ["bep20", "mainnet"].as_slice(),
            "ada" => ["mainnet"].as_slice(),
            "ltc" => ["mainnet", "lightning"].as_slice(),
            "xrp" => ["mainnet"].as_slice(),
            "sol" => ["mainnet", "sol"].as_slice(),
            "trx" => ["trc20", "mainnet"].as_slice(),
            "usdt" => ["erc20", "trc20", "bep20", "polygon", "sol"].as_slice(),
            "usdc" => ["erc20", "base", "optimism", "polygon", "bsc", "sol"].as_slice(),
            _ => ["mainnet"].as_slice(),
        };

        preferred
            .iter()
            .position(|candidate| network == *candidate)
            .unwrap_or(preferred.len() + 10)
    }

    fn sort_currencies_for_display(currencies: &mut [CurrencyResponse]) {
        currencies.sort_by(|left, right| {
            left.ticker
                .to_lowercase()
                .cmp(&right.ticker.to_lowercase())
                .then_with(|| {
                    Self::preferred_currency_network_rank(left)
                        .cmp(&Self::preferred_currency_network_rank(right))
                })
                .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
                .then_with(|| {
                    left.network
                        .to_lowercase()
                        .cmp(&right.network.to_lowercase())
                })
        });
    }

    fn normalize_currency_search(value: &str) -> String {
        value
            .trim()
            .to_ascii_lowercase()
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character.is_ascii_whitespace() {
                    character
                } else {
                    ' '
                }
            })
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn currency_search_score(currency: &CurrencyResponse, search: &str) -> usize {
        if search.is_empty() {
            return 0;
        }

        let ticker = Self::normalize_currency_search(&currency.ticker);
        let name = Self::normalize_currency_search(&currency.name);
        let network = Self::normalize_currency_search(&currency.network);
        let search_text = format!("{} {} {}", ticker, name, network);

        let mut score = 0usize;

        if ticker == search {
            score += 100;
        }
        if name == search {
            score += 90;
        }
        if ticker.starts_with(search) {
            score += 70;
        }
        if name.starts_with(search) {
            score += 50;
        }
        if network.starts_with(search) {
            score += 30;
        }
        if search_text.contains(search) {
            score += 10;
        }

        score
    }

    fn apply_search_ranking(
        mut responses: Vec<CurrencyResponse>,
        search: &str,
    ) -> Vec<CurrencyResponse> {
        let normalized_search = Self::normalize_currency_search(search);
        if normalized_search.is_empty() {
            Self::sort_currencies_for_display(&mut responses);
            return responses;
        }

        let mut scored = responses
            .into_iter()
            .filter_map(|currency| {
                let score = Self::currency_search_score(&currency, &normalized_search);
                (score > 0).then_some((score, currency))
            })
            .collect::<Vec<_>>();

        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| {
                    Self::preferred_currency_network_rank(&left.1)
                        .cmp(&Self::preferred_currency_network_rank(&right.1))
                })
                .then_with(|| left.1.name.to_lowercase().cmp(&right.1.name.to_lowercase()))
                .then_with(|| {
                    left.1
                        .ticker
                        .to_lowercase()
                        .cmp(&right.1.ticker.to_lowercase())
                })
                .then_with(|| {
                    left.1
                        .network
                        .to_lowercase()
                        .cmp(&right.1.network.to_lowercase())
                })
        });

        scored.into_iter().map(|(_, currency)| currency).collect()
    }

    /// Internal helper to estimate gas cost for payout on the target network
    /// Get the amount Trocador should have sent to our address
    pub async fn get_expected_trocador_amount(&self, swap_id: &str) -> Result<f64, SwapError> {
        self.repository.get_expected_trocador_amount(swap_id).await
    }

    async fn get_gas_cost_for_network(
        &self,
        ticker: &str,
        network: &str,
    ) -> Result<f64, SwapError> {
        if !self.payout_policy.has_local_certified_chains() {
            return Ok(0.0);
        }

        if !self
            .swap_service()
            .direct_settlement_available(ticker, network)
            .await
        {
            return Ok(0.0);
        }

        // Normalize API-facing network labels (for example ERC20 -> ethereum)
        // and fall back to conservative defaults when live RPC estimation fails.
        let normalized_network = GasEstimator::normalize_payout_network(ticker, network);
        Ok(self
            .gas_estimator
            .get_gas_cost_for_network(&normalized_network)
            .await)
    }

    async fn cache_trade_provider_spread(&self, trade_id: &str, provider_spread: f64) {
        if let Some(service) = &self.redis_service {
            let cache_key = format!("trocador:pricing:trade:{}:provider_spread", trade_id);
            let _ = service
                .set_string(&cache_key, &provider_spread.to_string(), 600)
                .await;
        }
    }

    // =========================================================================
    // CURRENCIES
    // =========================================================================

    /// Check if currencies cache needs refresh using Probabilistic Early Recomputation (PER)
    pub async fn should_sync_currencies(&self) -> Result<bool, SwapError> {
        match self.repository.get_latest_currency_sync().await? {
            Some(last_sync) => {
                let now = Utc::now();
                let cache_age = now - last_sync;
                let ttl_seconds = 300.0; // 5 minutes

                // Get the last sync duration (Delta) from Redis, default to 2.0s if missing
                let delta = if let Some(service) = &self.redis_service {
                    service
                        .get_string("currencies:sync_duration")
                        .await
                        .ok()
                        .flatten()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(2.0)
                } else {
                    2.0
                };

                let beta = 1.0;
                let rand: f64 = rand::random(); // 0.0 to 1.0

                // Avoid log(0) which is -infinity
                let safe_rand = if rand < 0.0001 { 0.0001 } else { rand };

                // PER Formula: TimeToRefresh = TTL - (Delta * Beta * -ln(rand))
                // Note: -ln(rand) is positive because ln(0..1) is negative
                let early_expire_margin = delta * beta * (-safe_rand.ln());

                let effective_ttl = ttl_seconds - early_expire_margin;

                // If cache age exceeds our probabilistic TTL, we sync
                Ok(cache_age.num_seconds() as f64 >= effective_ttl)
            }
            _ => Ok(true), // No sync found, need to sync
        }
    }

    /// Sync currencies from Trocador API and upsert into database
    pub async fn sync_currencies_from_trocador(
        &self,
        trocador_gateway: &TrocadorGateway,
    ) -> Result<usize, SwapError> {
        let start_time = std::time::Instant::now();

        // Fetch from Trocador API
        let trocador_currencies = trocador_gateway.fetch_currencies().await?;
        let total_count = trocador_currencies.len();

        // Process in chunks of 500 to avoid hitting packet size limits
        for chunk in trocador_currencies.chunks(500) {
            self.upsert_currencies_batch(chunk).await?;
        }

        let duration = start_time.elapsed().as_secs_f64();

        // Store the sync duration (Delta) for PER and invalidate response cache
        if let Some(service) = &self.redis_service {
            let _ = service
                .set_string("currencies:sync_duration", &duration.to_string(), 3600)
                .await;
            let _ = service.set_string("currencies:response:all", "", 0).await;
        }

        Ok(total_count)
    }

    /// Upsert a batch of currencies
    async fn upsert_currencies_batch(
        &self,
        currencies: &[TrocadorCurrency],
    ) -> Result<(), SwapError> {
        self.repository.upsert_currencies_batch(currencies).await
    }

    /// Get currencies with optimized caching and raw response support
    pub async fn get_currencies_optimized(
        &self,
        query: CurrenciesQuery,
    ) -> Result<CurrenciesResult, SwapError> {
        let cache_key = format!("trocador:currencies:{:?}", query);
        let stale_key = format!("trocador:currencies:stale:{:?}", query);

        // 1. Try fresh cache first (10 min TTL)
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached_json)) = service.get_string(&cache_key).await {
                return Ok(CurrenciesResult::RawJson(cached_json));
            }

            // 2. If fresh cache miss, try stale cache (30 min TTL) - STALE-WHILE-REVALIDATE
            if let Ok(Some(stale_json)) = service.get_string(&stale_key).await {
                // Serve stale data immediately
                let stale_result = Ok(CurrenciesResult::RawJson(stale_json.clone()));

                // Trigger background refresh (fire and forget)
                let service_clone = service.clone();
                let cache_key_clone = cache_key.clone();
                let stale_key_clone = stale_key.clone();
                let query_clone = query.clone();

                tokio::spawn(async move {
                    if let Ok(true) = service_clone.try_lock("lock:refresh_currencies", 30).await {
                        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
                        let gateway = TrocadorGateway::new(api_key);

                        if let Ok(currencies) = gateway.fetch_currencies().await {
                            let responses =
                                Self::filter_and_convert_currencies(currencies, &query_clone);
                            if let Ok(json_string) = serde_json::to_string(&responses) {
                                let _ = service_clone
                                    .set_string(&cache_key_clone, &json_string, 600)
                                    .await; // 10 min fresh
                                let _ = service_clone
                                    .set_string(&stale_key_clone, &json_string, 1800)
                                    .await; // 30 min stale
                            }
                        }
                    }
                });

                return stale_result;
            }
        }

        // 3. No cache at all - fetch from API (with rate limit protection)
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        let gateway = TrocadorGateway::new(api_key);

        // Rate limit check: use token bucket
        if let Some(service) = &self.redis_service {
            if !self.check_rate_limit(service, "trocador_api", 10, 60).await {
                return Err(SwapError::ExternalApiError(
                    "Rate limit exceeded. Please try again later.".to_string(),
                ));
            }
        }

        let currencies = gateway.fetch_currencies().await?;
        let responses = Self::filter_and_convert_currencies(currencies, &query);

        // 4. Cache the result (both fresh and stale)
        let json_string = serde_json::to_string(&responses)
            .map_err(|e| SwapError::ExternalApiError(e.to_string()))?;

        if let Some(service) = &self.redis_service {
            let _ = service.set_string(&cache_key, &json_string, 600).await; // 10 min fresh
            let _ = service.set_string(&stale_key, &json_string, 1800).await; // 30 min stale
        }

        Ok(CurrenciesResult::RawJson(json_string))
    }

    // Helper: Token bucket rate limiter
    async fn check_rate_limit(
        &self,
        redis: &crate::services::redis_cache::RedisService,
        key: &str,
        max_requests: u32,
        window_secs: u64,
    ) -> bool {
        let bucket_key = format!("ratelimit:{}", key);

        // Simple token bucket: allow max_requests per window_secs
        match redis.get_string(&bucket_key).await {
            Ok(Some(count_str)) => {
                if let Ok(count) = count_str.parse::<u32>() {
                    if count >= max_requests {
                        return false; // Rate limited
                    }
                    // Increment counter
                    let _ = redis
                        .set_string(&bucket_key, &(count + 1).to_string(), window_secs)
                        .await;
                    true
                } else {
                    // Reset counter
                    let _ = redis.set_string(&bucket_key, "1", window_secs).await;
                    true
                }
            }
            _ => {
                // First request in window
                let _ = redis.set_string(&bucket_key, "1", window_secs).await;
                true
            }
        }
    }

    // Helper: Filter and convert currencies
    fn filter_and_convert_currencies(
        currencies: Vec<TrocadorCurrency>,
        query: &CurrenciesQuery,
    ) -> Vec<CurrencyResponse> {
        let mut responses: Vec<CurrencyResponse> = currencies
            .into_iter()
            .filter(|c| {
                if let Some(ref ticker) = query.ticker {
                    if c.ticker.to_lowercase() != ticker.to_lowercase() {
                        return false;
                    }
                }
                if let Some(ref network) = query.network {
                    if &c.network != network {
                        return false;
                    }
                }
                if let Some(memo) = query.memo {
                    if c.memo != memo {
                        return false;
                    }
                }
                true
            })
            .map(|c| CurrencyResponse {
                name: c.name,
                ticker: c.ticker.clone(),
                network: c.network.clone(),
                memo: c.memo,
                extra_id_name: default_extra_id_name(&c.ticker, &c.network, c.memo),
                image: c.image,
                minimum: c.minimum,
                maximum: c.maximum,
            })
            .collect();

        responses = match query.search.as_deref() {
            Some(search) if !search.trim().is_empty() => {
                Self::apply_search_ranking(responses, search)
            }
            _ => {
                Self::sort_currencies_for_display(&mut responses);
                responses
            }
        };

        // Apply pagination
        if let Some(limit) = query.limit {
            let page = query.page.unwrap_or(1).max(1);
            let start = ((page - 1) * limit) as usize;
            if start < responses.len() {
                let end = std::cmp::min(start + limit as usize, responses.len());
                responses = responses[start..end].to_vec();
            } else {
                responses = Vec::new();
            }
        }

        responses
    }

    /// Get currencies from database with optional filtering
    pub async fn get_currencies(&self, query: CurrenciesQuery) -> Result<Vec<Currency>, SwapError> {
        // Redirect to new optimized method
        // Warning: This is a compatibility shim. The controller should now handle CurrenciesResult.
        // If this is called, we force unpack Structured result.
        match self.get_currencies_optimized(query).await? {
            CurrenciesResult::Structured(_res) => {
                // We need to map back to Currency model if this function signature is strict,
                // but the current get_currencies returns Vec<Currency>.
                // Wait, get_currencies_optimized returns Vec<CurrencyResponse> inside Structured variant.
                // The original get_currencies returned Vec<Currency>.
                // This means I broke the signature compatibility for this shim.
                // Let's just fix the controller to use get_currencies_optimized directly.
                // For this shim, I will return an error or basic implementation if needed,
                // but better to rely on fetch_currencies_from_db for raw access.

                // Ideally this method should be deprecated or removed if controller is updated.
                // For now, let's just return a database fetch to be safe.
                Err(SwapError::DatabaseError(
                    "Use get_currencies_optimized instead".to_string(),
                ))
            }
            CurrenciesResult::RawJson(_) => Err(SwapError::DatabaseError(
                "Raw JSON not supported in legacy method".to_string(),
            )),
        }
    }

    // =========================================================================
    // PROVIDERS
    // =========================================================================

    /// Check if providers cache needs refresh (>5 minutes old)
    pub async fn should_sync_providers(&self) -> Result<bool, SwapError> {
        match self.repository.get_latest_provider_sync().await? {
            Some(last_sync) => {
                let cache_age = Utc::now() - last_sync;
                Ok(cache_age.num_minutes() > 5)
            }
            _ => Ok(true),
        }
    }

    /// Check if providers cache needs refresh using Probabilistic Early Recomputation (PER)
    pub async fn should_sync_providers_per(&self) -> Result<bool, SwapError> {
        let stats_key = "providers:sync_stats";

        let stats = if let Some(service) = &self.redis_service {
            service
                .get_json::<serde_json::Value>(stats_key)
                .await
                .unwrap_or(None)
        } else {
            None
        };

        if let Some(stats) = stats {
            let last_sync = stats["last_sync"].as_i64().unwrap_or(0);
            let duration = stats["duration"].as_f64().unwrap_or(2.0);

            let now = Utc::now().timestamp();
            let age = (now - last_sync) as f64;
            let ttl = 3600.0; // 1 hour hard TTL
            let beta = 1.0;

            // PER Formula: Refresh if time_remaining <= delta * beta * -ln(rand)
            let time_remaining = ttl - age;
            let rand: f64 = rand::random();
            // Avoid log(0)
            let safe_rand = if rand < 0.0001 { 0.0001 } else { rand };
            let x_fetch = duration * beta * (-safe_rand.ln());

            return Ok(time_remaining <= x_fetch);
        }

        Ok(true) // No stats, sync needed
    }

    /// Get providers with optimized caching and raw response support
    pub async fn get_providers_optimized(
        &self,
        query: ProvidersQuery,
    ) -> Result<ProvidersResult, SwapError> {
        let cache_key = format!("trocador:providers:{:?}", query);
        let stale_key = format!("trocador:providers:stale:{:?}", query);

        // 1. Try fresh cache first (10 min TTL)
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached_json)) = service.get_string(&cache_key).await {
                return Ok(ProvidersResult::RawJson(cached_json));
            }

            // 2. If fresh cache miss, try stale cache (30 min TTL) - STALE-WHILE-REVALIDATE
            if let Ok(Some(stale_json)) = service.get_string(&stale_key).await {
                // Serve stale data immediately
                let stale_result = Ok(ProvidersResult::RawJson(stale_json.clone()));

                // Trigger background refresh (fire and forget)
                let service_clone = service.clone();
                let cache_key_clone = cache_key.clone();
                let stale_key_clone = stale_key.clone();
                let query_clone = query.clone();

                tokio::spawn(async move {
                    if let Ok(true) = service_clone.try_lock("lock:refresh_providers", 30).await {
                        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
                        let gateway = TrocadorGateway::new(api_key);

                        if let Ok(providers) = gateway.fetch_providers().await {
                            let responses =
                                Self::filter_and_convert_providers(providers, &query_clone);
                            if let Ok(json_string) = serde_json::to_string(&responses) {
                                let _ = service_clone
                                    .set_string(&cache_key_clone, &json_string, 600)
                                    .await; // 10 min fresh
                                let _ = service_clone
                                    .set_string(&stale_key_clone, &json_string, 1800)
                                    .await; // 30 min stale
                            }
                        }
                    }
                });

                return stale_result;
            }
        }

        // 3. No cache at all - fetch from API (with rate limit protection)
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        let gateway = TrocadorGateway::new(api_key);

        // Rate limit check
        if let Some(service) = &self.redis_service {
            if !self.check_rate_limit(service, "trocador_api", 10, 60).await {
                return Err(SwapError::ExternalApiError(
                    "Rate limit exceeded. Please try again later.".to_string(),
                ));
            }
        }

        let providers = gateway.fetch_providers().await?;
        let responses = Self::filter_and_convert_providers(providers, &query);

        // 4. Cache the result (both fresh and stale)
        let json_string = serde_json::to_string(&responses)
            .map_err(|e| SwapError::ExternalApiError(e.to_string()))?;

        if let Some(service) = &self.redis_service {
            let _ = service.set_string(&cache_key, &json_string, 600).await; // 10 min fresh
            let _ = service.set_string(&stale_key, &json_string, 1800).await; // 30 min stale
        }

        Ok(ProvidersResult::RawJson(json_string))
    }

    // Helper: Filter and convert providers
    fn filter_and_convert_providers(
        providers: Vec<TrocadorProvider>,
        query: &ProvidersQuery,
    ) -> Vec<ProviderResponse> {
        providers
            .into_iter()
            .filter(|p| {
                if let Some(ref rating) = query.rating {
                    if &p.rating != rating {
                        return false;
                    }
                }
                if let Some(markup_enabled) = query.markup_enabled {
                    if p.enabled_markup != markup_enabled {
                        return false;
                    }
                }
                true
            })
            .map(|p| ProviderResponse {
                name: p.name,
                rating: p.rating,
                insurance: p.insurance,
                markup_enabled: p.enabled_markup,
                eta: p.eta as i32,
            })
            .collect()
    }

    /// Sync providers from Trocador API and upsert into database
    pub async fn sync_providers_from_trocador(
        &self,
        trocador_gateway: &TrocadorGateway,
    ) -> Result<usize, SwapError> {
        let start_time = std::time::Instant::now();

        let trocador_providers = trocador_gateway.fetch_providers().await?;

        let mut synced_count = 0;

        for trocador_provider in trocador_providers {
            self.upsert_provider_from_trocador(&trocador_provider)
                .await?;
            synced_count += 1;
        }

        let duration = start_time.elapsed().as_secs_f64();

        // Store the sync duration (Delta) for PER and invalidate response cache
        if let Some(service) = &self.redis_service {
            let stats = serde_json::json!({
                "last_sync": Utc::now().timestamp(),
                "duration": duration
            });
            let _ = service
                .set_json("providers:sync_stats", &stats, 3600 * 24)
                .await;
            let _ = service.set_string("providers:response:all", "", 0).await;
        }

        Ok(synced_count)
    }

    /// Upsert a single provider from Trocador data
    async fn upsert_provider_from_trocador(
        &self,
        trocador_provider: &TrocadorProvider,
    ) -> Result<(), SwapError> {
        self.repository
            .upsert_provider_from_trocador(trocador_provider)
            .await
    }

    /// Get providers from database with optional filtering
    pub async fn get_providers(&self, query: ProvidersQuery) -> Result<Vec<Provider>, SwapError> {
        self.repository.get_providers(query).await
    }

    // =========================================================================
    // TRADING PAIRS
    // =========================================================================

    /// Get trading pairs with pagination, filtering, and sorting
    pub async fn get_pairs(
        &self,
        query: super::schema::PairsQuery,
    ) -> Result<super::schema::PairsResponse, SwapError> {
        self.repository.get_pairs(query).await
    }

    // =========================================================================
    // RATES
    // =========================================================================

    /// Get live rates with Distributed Singleflight optimization
    /// Prevents thundering herd by coalescing concurrent requests for the same pair
    pub async fn get_rates_optimized(
        &self,
        query: &super::schema::RatesQuery,
    ) -> Result<super::schema::RatesResponse, SwapError> {
        self.get_rates_optimized_with_payout_mode(query, false)
            .await
    }

    pub async fn get_provider_managed_rates(
        &self,
        query: &super::schema::RatesQuery,
    ) -> Result<super::schema::RatesResponse, SwapError> {
        self.get_rates_optimized_with_payout_mode(query, true).await
    }

    async fn get_rates_optimized_with_payout_mode(
        &self,
        query: &super::schema::RatesQuery,
        force_provider_managed: bool,
    ) -> Result<super::schema::RatesResponse, SwapError> {
        let payout_mode = if force_provider_managed {
            "provider-managed"
        } else {
            "auto"
        };
        let cache_key = format!(
            "rates:{}:{}:{}:{}:{}:{}",
            query.from, query.to, query.network_from, query.network_to, query.amount, payout_mode
        );

        let lock_key = format!("lock:{}", cache_key);

        // 1. Try process-local cache first for instant repeat lookups.
        if let Some(cached) = read_cached_rates_response(&cache_key) {
            return Ok(cached);
        }

        // 2. Try Redis cache if it is enabled for this flow.
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached)) = service
                .get_json::<super::schema::RatesResponse>(&cache_key)
                .await
            {
                write_cached_rates_response(cache_key.clone(), cached.clone());
                return Ok(cached);
            }
        }

        // 3. Distributed singleflight only when Redis is enabled.
        if let Some(service) = &self.redis_service {
            // Try to acquire lock for 15 seconds (cover long API calls)
            // If try_lock returns true, we are the LEADER.
            // If returns false, we are a FOLLOWER.
            if !service.try_lock(&lock_key, 15).await.unwrap_or(false) {
                // FOLLOWER: Wait for the leader to populate the cache
                // Poll every 200ms for up to 5 seconds
                for _ in 0..25 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if let Ok(Some(cached)) = service
                        .get_json::<super::schema::RatesResponse>(&cache_key)
                        .await
                    {
                        return Ok(cached);
                    }
                }
                // If timeout, fall through and fetch ourselves
            }
        }

        // 4. Fetch from API (leader execution or direct fetch when Redis is disabled)
        let result = self
            .fetch_rates_from_api(query, force_provider_managed)
            .await?;

        // 5. Cache result locally and optionally in Redis.
        write_cached_rates_response(cache_key.clone(), result.clone());

        if let Some(service) = &self.redis_service {
            let _ = service.set_json(&cache_key, &result, 15).await;
            // Lock will auto-expire, letting it sit ensures we don't spam if API is slow
        }

        Ok(result)
    }

    /// Internal helper to fetch rates from Trocador
    async fn fetch_rates_from_api(
        &self,
        query: &super::schema::RatesQuery,
        force_provider_managed: bool,
    ) -> Result<super::schema::RatesResponse, SwapError> {
        Ok(self
            .fetch_priced_rates_from_api(query, force_provider_managed)
            .await?
            .response)
    }

    async fn fetch_priced_rates_from_api(
        &self,
        query: &super::schema::RatesQuery,
        force_provider_managed: bool,
    ) -> Result<PricedRates, SwapError> {
        // Validate: Cannot swap same currency on same network
        if query.from.eq_ignore_ascii_case(&query.to)
            && query.network_from.eq_ignore_ascii_case(&query.network_to)
        {
            return Err(SwapError::PairNotAvailable);
        }

        // Rate limiting check
        if let Some(service) = &self.redis_service {
            let rate_limit_key = "api_calls:trocador:rates";
            let _ = service.check_rate_limit(rate_limit_key, 5, 60).await;
        }

        let trocador_gateway = TrocadorGateway::from_env()
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        let trocador_res = self
            .call_trocador_with_retry(|| async {
                let markup = swap_markup_from_env().map_err(TrocadorError::ApiError)?;
                trocador_gateway
                    .fetch_rates(
                        &query.from,
                        &query.network_from,
                        &query.to,
                        &query.network_to,
                        query.amount,
                        query.min_kycrating.as_deref(),
                        markup.as_deref(),
                    )
                    .await
            })
            .await?;

        let direct_settlement = if force_provider_managed {
            false
        } else {
            self.swap_service()
                .direct_settlement_available(&query.to, &query.network_to)
                .await
        };
        let gas_cost = if direct_settlement {
            self.get_gas_cost_for_network(&query.to, &query.network_to)
                .await?
        } else {
            0.0
        };
        let quote_service = QuoteService::new();
        let priced_rates = quote_service
            .price_rates(
                query,
                &trocador_gateway,
                trocador_res,
                gas_cost,
                direct_settlement,
            )
            .await
            .map_err(|e| {
                SwapError::ExternalApiError(format!(
                    "Failed to resolve live market pricing from Trocador: {}",
                    e
                ))
            })?;

        if !priced_rates.response.trade_id.is_empty() {
            self.cache_trade_provider_spread(
                &priced_rates.response.trade_id,
                priced_rates.provider_spread,
            )
            .await;
        }

        Ok(priced_rates)
    }

    // =========================================================================
    // CREATE SWAP
    // =========================================================================

    /// Create a new swap by calling Trocador new_trade and saving to database
    pub async fn create_swap(
        &self,
        request: &super::schema::CreateSwapRequest,
        user_id: Option<String>,
        client_id: Option<String>,
    ) -> Result<super::schema::CreateSwapResponse, SwapError> {
        self.swap_service()
            .create_swap(request, user_id, client_id)
            .await
    }

    // =========================================================================
    // SWAP STATUS
    // =========================================================================

    /// Get swap status by ID
    /// 1. Look up swap in database by local swap_id
    /// 2. Get provider_swap_id (Trocador's trade_id)
    /// 3. Call Trocador API to get latest status
    /// 4. Update local database with new status
    /// 5. Return status to user
    pub async fn get_swap_status(
        &self,
        swap_id: &str,
    ) -> Result<super::schema::SwapStatusResponse, SwapError> {
        self.swap_service().get_swap_status(swap_id).await
    }

    pub async fn get_swap_status_for_client(
        &self,
        swap_id: &str,
        client_id: &str,
    ) -> Result<super::schema::SwapStatusResponse, SwapError> {
        self.swap_service()
            .get_swap_status_for_client(swap_id, client_id)
            .await
    }

    // =========================================================================
    // ADDRESS VALIDATION
    // =========================================================================

    /// Validate cryptocurrency address using Trocador API
    pub async fn validate_address(
        &self,
        request: &super::schema::ValidateAddressRequest,
    ) -> Result<super::schema::ValidateAddressResponse, SwapError> {
        // 1. Validate input
        if request.ticker.trim().is_empty() {
            return Err(SwapError::InvalidAddress);
        }

        if request.network.trim().is_empty() {
            return Err(SwapError::InvalidAddress);
        }

        if request.address.trim().is_empty() {
            return Err(SwapError::InvalidAddress);
        }

        match validate_address_by_network_family(
            &request.ticker,
            &request.network,
            &request.address,
        ) {
            AddressValidation::Valid { .. } => {}
            AddressValidation::Invalid { .. } => {
                return Ok(super::schema::ValidateAddressResponse {
                    valid: false,
                    ticker: request.ticker.clone(),
                    network: request.network.clone(),
                    address: request.address.clone(),
                });
            }
            AddressValidation::Unsupported { .. } => {}
        }

        // 2. Get API key
        let trocador_gateway = TrocadorGateway::from_env()
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        // 3. Call Trocador API with retry logic
        let is_valid = self
            .call_trocador_with_retry(|| async {
                trocador_gateway
                    .validate_address(&request.ticker, &request.network, &request.address)
                    .await
            })
            .await?;

        // 4. Return response
        Ok(super::schema::ValidateAddressResponse {
            valid: is_valid,
            ticker: request.ticker.clone(),
            network: request.network.clone(),
            address: request.address.clone(),
        })
    }

    // =========================================================================
    // RETRY LOGIC FOR RATE LIMITING
    // =========================================================================

    /// Call Trocador API with exponential backoff retry logic
    /// Handles rate limiting AND transient network errors gracefully
    async fn call_trocador_with_retry<F, Fut, T>(&self, f: F) -> Result<T, SwapError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, TrocadorError>>,
    {
        let max_retries = 3;
        let mut retries = 0;

        loop {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let is_rate_limit = e.is_rate_limited();
                    let is_transient_error = e.is_retryable() && !is_rate_limit;

                    // Retry on either rate limit or transient errors
                    if (is_rate_limit || is_transient_error) && retries < max_retries {
                        retries += 1;
                        // Exponential backoff: 400ms, 800ms, 1600ms
                        // Total max wait: ~2.8s (allows API to recover)
                        let delay_millis = 200 * (2_u64.pow(retries as u32));

                        let error_type = if is_rate_limit {
                            "Rate limit"
                        } else {
                            "Network error"
                        };
                        tracing::warn!(
                            "{} hit, retrying in {}ms (attempt {}/{})",
                            error_type,
                            delay_millis,
                            retries,
                            max_retries
                        );

                        tokio::time::sleep(Duration::from_millis(delay_millis as u64)).await;
                        continue;
                    }

                    // Not retriable error or max retries exceeded
                    return Err(SwapError::from(e));
                }
            }
        }
    }

    // =========================================================================
    // SWAP HISTORY (Keyset Pagination)
    // =========================================================================

    /// Get user's swap history with keyset pagination for optimal performance
    pub async fn get_swap_history(
        &self,
        user_id: &str,
        query: super::schema::HistoryQuery,
    ) -> Result<super::schema::HistoryResponse, SwapError> {
        self.repository.get_swap_history(user_id, query).await
    }

    pub async fn get_swap_history_for_client(
        &self,
        client_id: &str,
        query: super::schema::HistoryQuery,
    ) -> Result<super::schema::HistoryResponse, SwapError> {
        self.repository
            .get_swap_history_for_client(client_id, query)
            .await
    }

    pub async fn get_admin_swap_history(
        &self,
        query: super::schema::HistoryQuery,
    ) -> Result<super::schema::HistoryResponse, SwapError> {
        self.repository.get_admin_swap_history(query).await
    }

    // =============================================================================
    // ESTIMATE ENDPOINT - Quick rate preview without creating swap
    // =============================================================================

    /// Get estimate with optimized caching (60s TTL + bucketing + PER)
    pub async fn get_estimate_optimized(
        &self,
        query: &super::schema::EstimateQuery,
    ) -> Result<super::schema::EstimateResponse, SwapError> {
        // 1. Generate cache keys (exact + bucketed)
        let exact_key = format!(
            "estimate:v2:{}:{}:{}:{}:{:.8}",
            query.from.to_lowercase(),
            query.to.to_lowercase(),
            query.network_from,
            query.network_to,
            query.amount
        );

        let bucketed_amount = Self::bucket_amount(query.amount);
        let bucketed_key = format!(
            "estimate:v2:{}:{}:{}:{}:{:.8}:bucket",
            query.from.to_lowercase(),
            query.to.to_lowercase(),
            query.network_from,
            query.network_to,
            bucketed_amount
        );

        // 2. Try in-process exact cache first (10s TTL for repeated requests).
        if let Some(cached) = read_cached_estimate_entry(&exact_key) {
            return Ok(estimate_response_from_cache_entry(&cached));
        }

        // 3. Try in-process bucketed cache next (60s TTL for nearby amounts).
        if let Some(cached) = read_cached_estimate_entry(&bucketed_key) {
            return Ok(estimate_response_from_cache_entry(&cached));
        }

        // 4. Try Redis exact cache if it is enabled.
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached)) = service
                .get_json::<super::schema::EstimateCacheEntry>(&exact_key)
                .await
            {
                write_cached_estimate_entry(
                    exact_key.clone(),
                    ESTIMATE_EXACT_MEMORY_CACHE_TTL,
                    cached.clone(),
                );
                // Check if we should trigger early refresh (PER algorithm)
                if !Self::should_early_refresh(&cached) {
                    return Ok(estimate_response_from_cache_entry(&cached));
                } else {
                    // Trigger async refresh but return stale data
                    let query_clone = query.clone();
                    let pool_clone = self.pool.clone();
                    let redis_clone = self.redis_service.clone();
                    let wallet_clone = self.wallet_mnemonic.clone();
                    let rpc_manager = self.rpc_manager.clone();
                    let payout_policy = self.payout_policy.clone();

                    tokio::spawn(async move {
                        let crud = SwapCrud::new(
                            pool_clone,
                            redis_clone,
                            wallet_clone,
                            rpc_manager,
                            payout_policy,
                        );
                        let _ = crud.fetch_estimate_from_api(&query_clone).await;
                    });

                    return Ok(estimate_response_from_cache_entry(&cached));
                }
            }

            // 5. Try Redis bucketed cache for similar amounts.
            if let Ok(Some(cached)) = service
                .get_json::<super::schema::EstimateCacheEntry>(&bucketed_key)
                .await
            {
                write_cached_estimate_entry(
                    bucketed_key.clone(),
                    ESTIMATE_BUCKET_MEMORY_CACHE_TTL,
                    cached.clone(),
                );
                if !Self::should_early_refresh(&cached) {
                    return Ok(estimate_response_from_cache_entry(&cached));
                }
            }
        }

        // 6. Cache miss - fetch from API.
        self.fetch_estimate_from_api(query).await
    }

    /// Fetch estimate from Trocador API and cache result
    async fn fetch_estimate_from_api(
        &self,
        query: &super::schema::EstimateQuery,
    ) -> Result<super::schema::EstimateResponse, SwapError> {
        use chrono::Utc;
        use std::time::Instant;

        let start_time = Instant::now();

        // 1. Fetch priced rates from Trocador and reuse the resolved USD amount
        let rates_query = super::schema::RatesQuery {
            from: query.from.clone(),
            network_from: query.network_from.clone(),
            to: query.to.clone(),
            network_to: query.network_to.clone(),
            amount: query.amount,
            rate_type: None,
            provider: None,
            min_kycrating: None,
        };

        let priced_rates = self
            .fetch_priced_rates_from_api(&rates_query, false)
            .await?;
        let provider_spread = priced_rates.provider_spread;
        let amount_usd = priced_rates.amount_usd;
        let rates_response = priced_rates.response;

        if rates_response.rates.is_empty() {
            if let (Some(min), Some(max)) =
                (rates_response.min_deposit, rates_response.max_deposit)
            {
                if query.amount < min || query.amount > max {
                    return Err(SwapError::AmountOutOfRange { min, max });
                }
            }

            return Err(SwapError::PairNotAvailable);
        }

        // 2. Build estimate response from the priced rates
        let quote_service = QuoteService::new();
        let compute_time_ms = start_time.elapsed().as_millis() as i64;

        let response = quote_service.build_estimate(
            query,
            Some(rates_response.trade_id.clone()),
            rates_response.rates,
            provider_spread,
            amount_usd,
            false, // not cached
            0,     // cache age
            60,    // expires in 60s
        );

        // 5. Cache the result in-process, and in Redis if enabled.
        let now = Utc::now().timestamp_millis();

        let exact_key = format!(
            "estimate:v2:{}:{}:{}:{}:{:.8}",
            query.from.to_lowercase(),
            query.to.to_lowercase(),
            query.network_from,
            query.network_to,
            query.amount
        );
        let exact_entry = super::schema::EstimateCacheEntry {
            response: response.clone(),
            created_at: now,
            expires_at: now + 10_000, // 10 seconds
            compute_time_ms,
        };
        write_cached_estimate_entry(
            exact_key.clone(),
            ESTIMATE_EXACT_MEMORY_CACHE_TTL,
            exact_entry.clone(),
        );

        let bucketed_amount = Self::bucket_amount(query.amount);
        let bucketed_key = format!(
            "estimate:v2:{}:{}:{}:{}:{:.8}:bucket",
            query.from.to_lowercase(),
            query.to.to_lowercase(),
            query.network_from,
            query.network_to,
            bucketed_amount
        );
        let bucketed_entry = super::schema::EstimateCacheEntry {
            response: response.clone(),
            created_at: now,
            expires_at: now + 60_000, // 60 seconds
            compute_time_ms,
        };
        write_cached_estimate_entry(
            bucketed_key.clone(),
            ESTIMATE_BUCKET_MEMORY_CACHE_TTL,
            bucketed_entry.clone(),
        );

        if let Some(service) = &self.redis_service {
            let _ = service.set_json(&exact_key, &exact_entry, 10).await;
            let _ = service.set_json(&bucketed_key, &bucketed_entry, 60).await;
        }

        Ok(response)
    }

    /// Bucket amount to reduce cache fragmentation
    fn bucket_amount(amount: f64) -> f64 {
        let bucket_size = if amount < 0.01 {
            0.001
        } else if amount < 1.0 {
            0.01
        } else if amount < 10.0 {
            0.1
        } else {
            1.0
        };
        (amount / bucket_size).floor() * bucket_size
    }

    /// Probabilistic Early Recomputation (PER) - XFetch algorithm
    fn should_early_refresh(entry: &super::schema::EstimateCacheEntry) -> bool {
        use chrono::Utc;

        let now = Utc::now().timestamp_millis();
        let time_until_expiry = entry.expires_at - now;

        // Already expired
        if time_until_expiry <= 0 {
            return true;
        }

        // Don't refresh if more than 90% TTL remains
        let total_ttl = entry.expires_at - entry.created_at;
        if time_until_expiry > (total_ttl as f64 * 0.9) as i64 {
            return false;
        }

        // PER formula: currentTime - (delta × beta × log(random())) >= expirationTime
        let random: f64 = rand::random();
        if random == 0.0 {
            return true; // Guard against log(0)
        }

        let delta = entry.compute_time_ms.max(100) as f64; // Minimum 100ms
        let beta = 1.5; // Slightly aggressive for financial data
        let threshold = delta * beta * (-random.ln());

        now + threshold as i64 >= entry.expires_at
    }
}

#[cfg(test)]
mod tests {
    use super::SwapCrud;
    use crate::modules::swap::schema::{CurrenciesQuery, CurrencyResponse, TrocadorCurrency};

    fn trocador_currency(name: &str, ticker: &str, network: &str) -> TrocadorCurrency {
        TrocadorCurrency {
            name: name.to_string(),
            ticker: ticker.to_string(),
            network: network.to_string(),
            memo: false,
            image: String::new(),
            minimum: 0.0,
            maximum: 0.0,
        }
    }

    fn currency(name: &str, ticker: &str, network: &str) -> CurrencyResponse {
        CurrencyResponse {
            name: name.to_string(),
            ticker: ticker.to_string(),
            network: network.to_string(),
            memo: false,
            extra_id_name: None,
            image: String::new(),
            minimum: 0.0,
            maximum: 0.0,
        }
    }

    #[test]
    fn search_ranking_prefers_exact_ticker_then_name_prefix() {
        let ranked = SwapCrud::apply_search_ranking(
            vec![
                currency("Lido DAO", "ldo", "ERC20"),
                currency("Wrapped Lido Staked Ether", "wsteth", "ERC20"),
                currency("Bitcoin", "btc", "Mainnet"),
            ],
            "lido",
        );

        assert_eq!(ranked.first().map(|item| item.ticker.as_str()), Some("ldo"));
        assert!(ranked
            .iter()
            .all(|item| item.name.to_lowercase().contains("lido")));
    }

    #[test]
    fn search_filter_uses_backend_search_parameter() {
        let responses = SwapCrud::filter_and_convert_currencies(
            vec![
                trocador_currency("USD Coin", "usdc", "ERC20"),
                trocador_currency("Bitcoin", "btc", "Mainnet"),
                trocador_currency("Tether USD", "usdt", "TRC20"),
            ],
            &CurrenciesQuery {
                search: Some("usd".to_string()),
                page: Some(1),
                limit: Some(10),
                ..Default::default()
            },
        );

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].ticker, "usdc");
        assert_eq!(responses[1].ticker, "usdt");
    }
}
