use chrono::{Utc, DateTime};
use sqlx::{MySql, Pool};
use std::time::Duration;

use super::model::{Currency, Provider};
use super::schema::{CurrenciesQuery, ProvidersQuery, TrocadorCurrency, TrocadorProvider, CurrencyResponse, ProviderResponse};
use crate::services::trocador::{TrocadorClient, TrocadorError};
use crate::services::redis_cache::RedisService;
use crate::services::pricing::PricingEngine;
use crate::services::gas::GasEstimator;

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
        SwapError::ExternalApiError(err.to_string())
    }
}

// =============================================================================
// SWAP CRUD
// =============================================================================

pub struct SwapCrud {
    pool: Pool<MySql>,
    redis_service: Option<RedisService>, // Changed to RedisService
    wallet_mnemonic: Option<String>,
    gas_estimator: GasEstimator,
}

impl SwapCrud {
    pub fn new(pool: Pool<MySql>, redis_service: Option<RedisService>, wallet_mnemonic: Option<String>) -> Self {
        let gas_estimator = GasEstimator::new(redis_service.clone());
        Self { pool, redis_service, wallet_mnemonic, gas_estimator }
    }

    /// Normalize provider name from Trocador API to database ID format
    fn normalize_provider_id(provider_name: &str) -> String {
        // Trocador returns names like "ChangeNOW", "FixedFloat", "Changelly"
        // Database uses lowercase slugs like "changenow", "fixedfloat", "changelly"
        provider_name.to_lowercase().replace(" ", "").replace("-", "")
    }

    /// Internal helper to estimate gas cost for payout on the target network
    /// Get the amount Trocador should have sent to our address
    pub async fn get_expected_trocador_amount(&self, swap_id: &str) -> Result<f64, SwapError> {
        let swap: (f64, f64) = sqlx::query_as(
            "SELECT estimated_receive, platform_fee FROM swaps WHERE id = ?"
        )
        .bind(swap_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        
        // Trocador sends us: user_amount + our_commission
        // Because we told them to send to OUR address
        Ok(swap.0 + swap.1)
    }

    async fn get_gas_cost_for_network(&self, network: &str) -> f64 {
        // Use real-time gas price estimation with multi-tier caching and EMA smoothing
        self.gas_estimator.get_gas_cost_for_network(network).await
    }

    // =========================================================================
    // CURRENCIES
    // =========================================================================

    /// Check if currencies cache needs refresh using Probabilistic Early Recomputation (PER)
    pub async fn should_sync_currencies(&self) -> Result<bool, SwapError> {
        let result: Option<(Option<chrono::DateTime<Utc>>,)> = sqlx::query_as(
            "SELECT MAX(last_synced_at) FROM currencies"
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        match result {
            Some((Some(last_sync),)) => {
                let now = Utc::now();
                let cache_age = now - last_sync;
                let ttl_seconds = 300.0; // 5 minutes

                // Get the last sync duration (Delta) from Redis, default to 2.0s if missing
                let delta = if let Some(service) = &self.redis_service {
                    service.get_string("currencies:sync_duration")
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
        trocador_client: &TrocadorClient,
    ) -> Result<usize, SwapError> {
        let start_time = std::time::Instant::now();

        // Fetch from Trocador API
        let trocador_currencies = trocador_client.get_currencies().await?;
        let total_count = trocador_currencies.len();

        // Process in chunks of 500 to avoid hitting packet size limits
        for chunk in trocador_currencies.chunks(500) {
            self.upsert_currencies_batch(chunk).await?;
        }

        let duration = start_time.elapsed().as_secs_f64();
        
        // Store the sync duration (Delta) for PER and invalidate response cache
        if let Some(service) = &self.redis_service {
            let _ = service.set_string("currencies:sync_duration", &duration.to_string(), 3600).await;
            let _ = service.set_string("currencies:response:all", "", 0).await;
        }

        Ok(total_count)
    }

    /// Upsert a batch of currencies
    async fn upsert_currencies_batch(
        &self,
        currencies: &[TrocadorCurrency],
    ) -> Result<(), SwapError> {
        if currencies.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO currencies (
                symbol, name, network, is_active, logo_url,
                requires_extra_id, min_amount, max_amount, last_synced_at
            ) "
        );

        query_builder.push_values(currencies, |mut b, currency| {
            b.push_bind(&currency.ticker)
             .push_bind(&currency.name)
             .push_bind(&currency.network)
             .push("TRUE") // is_active
             .push_bind(&currency.image)
             .push_bind(currency.memo)
             .push_bind(currency.minimum)
             .push_bind(currency.maximum)
             .push("NOW()");
        });

        query_builder.push(
            " ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                logo_url = VALUES(logo_url),
                min_amount = VALUES(min_amount),
                max_amount = VALUES(max_amount),
                last_synced_at = VALUES(last_synced_at)"
        );

        let query = query_builder.build();
        query.execute(&self.pool).await.map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
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
                        let client = TrocadorClient::new(api_key);

                        if let Ok(currencies) = client.get_currencies().await {
                            let responses = Self::filter_and_convert_currencies(currencies, &query_clone);
                            if let Ok(json_string) = serde_json::to_string(&responses) {
                                let _ = service_clone.set_string(&cache_key_clone, &json_string, 600).await; // 10 min fresh
                                let _ = service_clone.set_string(&stale_key_clone, &json_string, 1800).await; // 30 min stale
                            }
                        }
                    }
                });

                return stale_result;
            }
        }

        // 3. No cache at all - fetch from API (with rate limit protection)
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        let client = TrocadorClient::new(api_key);

        // Rate limit check: use token bucket
        if let Some(service) = &self.redis_service {
            if !self.check_rate_limit(service, "trocador_api", 10, 60).await {
                return Err(SwapError::ExternalApiError(
                    "Rate limit exceeded. Please try again later.".to_string()
                ));
            }
        }

        let currencies = client.get_currencies().await?;
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
                    let _ = redis.set_string(&bucket_key, &(count + 1).to_string(), window_secs).await;
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
        let mut responses: Vec<CurrencyResponse> = currencies.into_iter()
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
                ticker: c.ticker,
                network: c.network,
                memo: c.memo,
                image: c.image,
                minimum: c.minimum,
                maximum: c.maximum,
            })
            .collect();

        // Apply pagination
        if let (Some(page), Some(limit)) = (query.page, query.limit) {
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
    pub async fn get_currencies(
        &self,
        query: CurrenciesQuery,
    ) -> Result<Vec<Currency>, SwapError> {
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
                Err(SwapError::DatabaseError("Use get_currencies_optimized instead".to_string()))
            }
            CurrenciesResult::RawJson(_) => Err(SwapError::DatabaseError("Raw JSON not supported in legacy method".to_string())),
        }
    }

    // =========================================================================
    // PROVIDERS
    // =========================================================================

    /// Check if providers cache needs refresh (>5 minutes old)
    pub async fn should_sync_providers(&self) -> Result<bool, SwapError> {
        let result: Option<(Option<chrono::DateTime<Utc>>,)> = sqlx::query_as(
            "SELECT MAX(last_synced_at) FROM providers"
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        match result {
            Some((Some(last_sync),)) => {
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
            service.get_json::<serde_json::Value>(stats_key).await.unwrap_or(None)
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
                        let client = TrocadorClient::new(api_key);

                        if let Ok(providers) = client.get_providers().await {
                            let responses = Self::filter_and_convert_providers(providers, &query_clone);
                            if let Ok(json_string) = serde_json::to_string(&responses) {
                                let _ = service_clone.set_string(&cache_key_clone, &json_string, 600).await; // 10 min fresh
                                let _ = service_clone.set_string(&stale_key_clone, &json_string, 1800).await; // 30 min stale
                            }
                        }
                    }
                });

                return stale_result;
            }
        }

        // 3. No cache at all - fetch from API (with rate limit protection)
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        let client = TrocadorClient::new(api_key);

        // Rate limit check
        if let Some(service) = &self.redis_service {
            if !self.check_rate_limit(service, "trocador_api", 10, 60).await {
                return Err(SwapError::ExternalApiError(
                    "Rate limit exceeded. Please try again later.".to_string()
                ));
            }
        }

        let providers = client.get_providers().await?;
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
        providers.into_iter()
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
        trocador_client: &TrocadorClient,
    ) -> Result<usize, SwapError> {
        let start_time = std::time::Instant::now();

        let trocador_providers = trocador_client.get_providers().await?;

        let mut synced_count = 0;

        for trocador_provider in trocador_providers {
            self.upsert_provider_from_trocador(&trocador_provider).await?;
            synced_count += 1;
        }

        let duration = start_time.elapsed().as_secs_f64();
        
        // Store the sync duration (Delta) for PER and invalidate response cache
        if let Some(service) = &self.redis_service {
            let stats = serde_json::json!({
                "last_sync": Utc::now().timestamp(),
                "duration": duration
            });
            let _ = service.set_json("providers:sync_stats", &stats, 3600 * 24).await;
            let _ = service.set_string("providers:response:all", "", 0).await;
        }

        Ok(synced_count)
    }

    /// Upsert a single provider from Trocador data
    async fn upsert_provider_from_trocador(
        &self,
        trocador_provider: &TrocadorProvider,
    ) -> Result<(), SwapError> {
        // Generate normalized ID from name (consistent with normalize_provider_id)
        let id = Self::normalize_provider_id(&trocador_provider.name);
        let slug = trocador_provider.name.to_lowercase().replace(" ", "-");

        // First, try to find existing provider by name (case-insensitive)
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT id FROM providers WHERE LOWER(name) = LOWER(?) LIMIT 1"
        )
        .bind(&trocador_provider.name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        if let Some((existing_id,)) = existing {
            // Update existing provider
            sqlx::query(
                r#"
                UPDATE providers SET
                    name = ?,
                    slug = ?,
                    kyc_rating = ?,
                    insurance_percentage = ?,
                    eta_minutes = ?,
                    markup_enabled = ?,
                    last_synced_at = NOW()
                WHERE id = ?
                "#
            )
            .bind(&trocador_provider.name)
            .bind(&slug)
            .bind(&trocador_provider.rating)
            .bind(trocador_provider.insurance)
            .bind(trocador_provider.eta as i32)  // Convert f64 to i32
            .bind(trocador_provider.enabled_markup)
            .bind(&existing_id)
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        } else {
            // Insert new provider
            sqlx::query(
                r#"
                INSERT INTO providers (
                    id, name, slug, is_active, kyc_rating,
                    insurance_percentage, eta_minutes, markup_enabled, last_synced_at
                )
                VALUES (?, ?, ?, TRUE, ?, ?, ?, ?, NOW())
                "#
            )
            .bind(&id)
            .bind(&trocador_provider.name)
            .bind(&slug)
            .bind(&trocador_provider.rating)
            .bind(trocador_provider.insurance)
            .bind(trocador_provider.eta as i32)  // Convert f64 to i32
            .bind(trocador_provider.enabled_markup)
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        }

        Ok(())
    }

    /// Get providers from database with optional filtering
    pub async fn get_providers(
        &self,
        query: ProvidersQuery,
    ) -> Result<Vec<Provider>, SwapError> {
        let mut sql = String::from(
            "SELECT id, name, slug, is_active, kyc_rating, insurance_percentage,
             eta_minutes, markup_enabled, api_url, logo_url, website_url,
             last_synced_at, created_at, updated_at
             FROM providers
             WHERE is_active = TRUE"
        );

        let mut sql_parts = Vec::new();

        if let Some(ref rating) = query.rating {
            sql_parts.push(format!("kyc_rating = '{}'", rating.replace("'", "''")));
        }

        if let Some(markup_enabled) = query.markup_enabled {
            sql_parts.push(format!("markup_enabled = {}", markup_enabled));
        }

        if !sql_parts.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&sql_parts.join(" AND "));
        }

        // Apply sorting
        match query.sort.as_deref() {
            Some("name") => sql.push_str(" ORDER BY name ASC"),
            Some("rating") => sql.push_str(" ORDER BY kyc_rating ASC, name ASC"),
            Some("eta") => sql.push_str(" ORDER BY eta_minutes ASC"),
            _ => sql.push_str(" ORDER BY name ASC"), // Default
        }

        let providers = sqlx::query_as::<_, Provider>(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(providers)
    }

    // =========================================================================
    // TRADING PAIRS
    // =========================================================================

    /// Get trading pairs with pagination, filtering, and sorting
    pub async fn get_pairs(
        &self,
        query: super::schema::PairsQuery,
    ) -> Result<super::schema::PairsResponse, SwapError> {
        // Build the base query with joins
        let mut count_sql = String::from(
            "SELECT COUNT(DISTINCT tp.id) 
             FROM trading_pairs tp
             INNER JOIN currencies c1 ON tp.from_currency_id = c1.id
             INNER JOIN currencies c2 ON tp.to_currency_id = c2.id
             WHERE 1=1"
        );

        let mut data_sql = String::from(
            "SELECT 
                tp.id,
                c1.symbol as base_currency,
                c1.network as base_network,
                c2.symbol as quote_currency,
                c2.network as quote_network,
                tp.is_active,
                LEAST(c1.min_amount, c2.min_amount) as min_amount,
                LEAST(c1.max_amount, c2.max_amount) as max_amount,
                tp.updated_at
             FROM trading_pairs tp
             INNER JOIN currencies c1 ON tp.from_currency_id = c1.id
             INNER JOIN currencies c2 ON tp.to_currency_id = c2.id
             WHERE 1=1"
        );

        let mut conditions = Vec::new();

        // Apply filters
        if let Some(ref base) = query.base_currency {
            conditions.push(format!("c1.symbol = '{}'", base.replace("'", "''")));
        }
        if let Some(ref quote) = query.quote_currency {
            conditions.push(format!("c2.symbol = '{}'", quote.replace("'", "''")));
        }
        if let Some(ref base_net) = query.base_network {
            conditions.push(format!("c1.network = '{}'", base_net.replace("'", "''")));
        }
        if let Some(ref quote_net) = query.quote_network {
            conditions.push(format!("c2.network = '{}'", quote_net.replace("'", "''")));
        }

        // Status filter
        match query.status.as_deref() {
            Some("active") => conditions.push("tp.is_active = TRUE".to_string()),
            Some("disabled") => conditions.push("tp.is_active = FALSE".to_string()),
            Some("all") | None => {}, // No filter
            _ => {}, // Invalid status, ignore
        }

        // Add conditions to both queries
        if !conditions.is_empty() {
            let condition_str = format!(" AND {}", conditions.join(" AND "));
            count_sql.push_str(&condition_str);
            data_sql.push_str(&condition_str);
        }

        // Get total count
        let total: (i64,) = sqlx::query_as(&count_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        let total_elements = total.0;

        // Apply sorting
        let order_clause = match query.order_by.as_deref() {
            Some(order) if order.contains("name") => {
                if order.contains("desc") {
                    " ORDER BY c1.symbol DESC, c2.symbol DESC"
                } else {
                    " ORDER BY c1.symbol ASC, c2.symbol ASC"
                }
            },
            Some(order) if order.contains("updated") => {
                if order.contains("desc") {
                    " ORDER BY tp.updated_at DESC"
                } else {
                    " ORDER BY tp.updated_at ASC"
                }
            },
            _ => " ORDER BY c1.symbol ASC, c2.symbol ASC", // Default
        };
        data_sql.push_str(order_clause);

        // Apply pagination
        let offset = query.page * query.size;
        data_sql.push_str(&format!(" LIMIT {} OFFSET {}", query.size, offset));

        // Fetch data
        let rows: Vec<(i64, String, String, String, String, bool, Option<f64>, Option<f64>, DateTime<Utc>)> = 
            sqlx::query_as(&data_sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        // Convert to response
        let pairs: Vec<super::schema::PairResponse> = rows.into_iter().map(|row| {
            super::schema::PairResponse {
                name: format!("{}/{}", row.1, row.3),
                base_currency: row.1,
                base_network: row.2,
                quote_currency: row.3,
                quote_network: row.4,
                status: if row.5 { "active".to_string() } else { "disabled".to_string() },
                min_amount: row.6,
                max_amount: row.7,
                last_updated: row.8,
            }
        }).collect();

        // Calculate pagination info
        let total_pages = ((total_elements as f64) / (query.size as f64)).ceil() as u32;
        let has_next = query.page + 1 < total_pages;
        let has_prev = query.page > 0;

        Ok(super::schema::PairsResponse {
            pairs,
            pagination: super::schema::PairsPaginationInfo {
                page: query.page,
                size: query.size,
                total_elements,
                total_pages,
                has_next,
                has_prev,
            },
        })
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
        let cache_key = format!(
            "rates:{}:{}:{}:{}:{}",
            query.from, query.to, query.network_from, query.network_to, query.amount
        );
        
        let lock_key = format!("lock:{}", cache_key);

        // 1. Try Cache First (Fast Path)
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached)) = service.get_json::<super::schema::RatesResponse>(&cache_key).await {
                return Ok(cached);
            }
        }

        // 2. Singleflight Logic (Coalescing)
        if let Some(service) = &self.redis_service {
            // Try to acquire lock for 15 seconds (cover long API calls)
            // If try_lock returns true, we are the LEADER.
            // If returns false, we are a FOLLOWER.
            if !service.try_lock(&lock_key, 15).await.unwrap_or(false) {
                // FOLLOWER: Wait for the leader to populate the cache
                // Poll every 200ms for up to 5 seconds
                for _ in 0..25 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if let Ok(Some(cached)) = service.get_json::<super::schema::RatesResponse>(&cache_key).await {
                        return Ok(cached);
                    }
                }
                // If timeout, fall through and fetch ourselves
            }
        }

        // 3. Fetch from API (Leader Execution)
        let result = self.fetch_rates_from_api(query).await?;

        // 4. Cache Result (Short TTL: 15s for volatility)
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
    ) -> Result<super::schema::RatesResponse, SwapError> {
        // Rate limiting check
        if let Some(service) = &self.redis_service {
            let rate_limit_key = "api_calls:trocador:rates";
            let _ = service.check_rate_limit(rate_limit_key, 5, 60).await;
        }

        let api_key = std::env::var("TROCADOR_API_KEY")
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        let trocador_client = TrocadorClient::new(api_key);

        let trocador_res = self.call_trocador_with_retry(|| async {
            trocador_client
                .get_rates(
                    &query.from,
                    &query.network_from,
                    &query.to,
                    &query.network_to,
                    query.amount,
                )
                .await
        })
        .await?;

        // ALGORITHMIC PRICING: Use PricingEngine to calculate optimal rates
        let pricing_engine = PricingEngine::new();
        let gas_cost = self.get_gas_cost_for_network(&query.network_to).await;
        
        let rates = pricing_engine.apply_optimal_markup(
            &trocador_res.quotes.quotes,
            query.amount,
            &query.from, // Changed from &query.network_to
            gas_cost,
        );

        Ok(super::schema::RatesResponse {
            trade_id: trocador_res.trade_id,
            from: query.from.clone(),
            network_from: query.network_from.clone(),
            to: query.to.clone(),
            network_to: query.network_to.clone(),
            amount: query.amount,
            rates,
        })
    }

    // =========================================================================
    // CREATE SWAP
    // =========================================================================

    /// Create a new swap by calling Trocador new_trade and saving to database
    pub async fn create_swap(
        &self,
        request: &super::schema::CreateSwapRequest,
        user_id: Option<String>,
    ) -> Result<super::schema::CreateSwapResponse, SwapError> {
        let api_key = std::env::var("TROCADOR_API_KEY")
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        let trocador_client = TrocadorClient::new(api_key);
        let swap_id = uuid::Uuid::new_v4().to_string();

        // MIDDLEMAN FLOW: 1. Generate our internal payout address (needed for Trocador call)
        let (internal_payout_address, address_index) = if let Some(mnemonic) = &self.wallet_mnemonic {
            let wallet_crud = crate::modules::wallet::crud::WalletCrud::new(self.pool.clone());
            
            // Get index FIRST
            let index = wallet_crud.get_next_index().await
                .map_err(|e| SwapError::DatabaseError(format!("Wallet error: {}", e)))?;

            let addr = crate::services::wallet::derivation::derive_address(mnemonic, &request.to, &request.network_to, index).await
                .map_err(|e| SwapError::DatabaseError(format!("Derivation error: {}", e)))?;
            
            tracing::info!("Generated internal payout address for {}: {}", request.to, addr);
            (addr, index)
        } else {
            return Err(SwapError::DatabaseError("Wallet mnemonic not configured".to_string()));
        };

        // 2. Call Trocador API with OUR address as the recipient
        let fixed = matches!(request.rate_type, super::schema::RateType::Fixed);

        let trocador_res = self.call_trocador_with_retry(|| async {
            let res = trocador_client
                .create_trade(
                    request.trade_id.as_deref(),
                    &request.from,
                    &request.network_from,
                    &request.to,
                    &request.network_to,
                    request.amount,
                    &internal_payout_address, // WE ARE THE RECIPIENT
                    request.refund_address.as_deref(),
                    &request.provider,
                    fixed,
                )
                .await;
            
            if let Err(ref e) = res {
                tracing::error!("Trocador create_trade failed: {}", e);
            }
            res
        })
        .await?;

        // ALGORITHMIC PRICING: Calculate fee for final swap creation (must match rate quote)
        let gas_cost = self.get_gas_cost_for_network(&request.network_to).await;
        
        // Transform single quote to mock list for engine compatibility or use engine logic directly
        // Better: We use the engine's internal math directly for a single value
        // Fee = Max( Gas_Cost * 1.5, Amount_To * Tier_Rate + Volatility_Premium )
        // Since create_swap uses a chosen provider, provider spread isn't relevant here, 
        // we use the tier-based rate.
        
        let trocador_amount = trocador_res.amount_to;
        let mut platform_fee = if request.amount < 200.0 {
            trocador_amount * 0.012
        } else if request.amount < 2000.0 {
            trocador_amount * 0.007
        } else {
            trocador_amount * 0.004
        };

        let gas_floor = gas_cost * 1.5;
        if platform_fee < gas_floor {
            platform_fee = gas_floor;
        }

        let estimated_user_receive = (trocador_amount - platform_fee).max(0.0);

        // 4. Map Trocador status to our internal SwapStatus
        let status = match trocador_res.status.as_str() {
            "new" | "waiting" => super::schema::SwapStatus::Waiting,
            "confirming" => super::schema::SwapStatus::Confirming,
            "sending" => super::schema::SwapStatus::Sending,
            "finished" => super::schema::SwapStatus::Completed,
            "failed" | "halted" => super::schema::SwapStatus::Failed,
            "refunded" => super::schema::SwapStatus::Refunded,
            "expired" => super::schema::SwapStatus::Expired,
            _ => super::schema::SwapStatus::Waiting,
        };

        // Normalize provider name to match database ID format
        let normalized_provider_id = Self::normalize_provider_id(&request.provider);

        // Ensure provider exists in database (auto-insert if missing)
        let provider_exists: Option<(i64,)> = sqlx::query_as(
            "SELECT COUNT(*) FROM providers WHERE id = ?"
        )
        .bind(&normalized_provider_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        if provider_exists.map(|(count,)| count).unwrap_or(0) == 0 {
            // Provider doesn't exist, insert a minimal record
            tracing::warn!("Provider '{}' not found in database, auto-inserting", normalized_provider_id);
            sqlx::query(
                r#"
                INSERT INTO providers (id, name, slug, is_active, kyc_rating, insurance_percentage, eta_minutes, markup_enabled)
                VALUES (?, ?, ?, TRUE, 'C', 0.015, 10, FALSE)
                ON DUPLICATE KEY UPDATE id = id
                "#
            )
            .bind(&normalized_provider_id)
            .bind(&request.provider)
            .bind(&normalized_provider_id)
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(format!("Failed to auto-insert provider: {}", e)))?;
        }

        // 5. Save to database - SWAPS table FIRST
        sqlx::query(
            r#"
            INSERT INTO swaps (
                id, user_id, provider_id, provider_swap_id,
                from_currency, from_network, to_currency, to_network,
                amount, estimated_receive, rate,
                deposit_address, deposit_extra_id,
                recipient_address, recipient_extra_id,
                refund_address, refund_extra_id,
                platform_fee, total_fee,
                status, rate_type, is_sandbox,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            "#
        )
        .bind(&swap_id)
        .bind(user_id)
        .bind(&normalized_provider_id)
        .bind(&trocador_res.trade_id)
        .bind(&request.from)
        .bind(&request.network_from)
        .bind(&request.to)
        .bind(&request.network_to)
        .bind(request.amount)
        .bind(estimated_user_receive)
        .bind(estimated_user_receive / request.amount) // rate
        .bind(&trocador_res.address_provider)
        .bind(&trocador_res.address_provider_memo)
        .bind(&request.recipient_address) // User's real address
        .bind(&request.recipient_extra_id)
        .bind(&request.refund_address)
        .bind(&request.refund_extra_id)
        .bind(platform_fee)
        .bind(platform_fee) // For now total platform fee is just our commission
        .bind(status.clone())
        .bind(&request.rate_type)
        .bind(request.sandbox)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        // 6. Save to swap_address_info - SECOND (Foreign Key now satisfied)
        let wallet_crud = crate::modules::wallet::crud::WalletCrud::new(self.pool.clone());
        wallet_crud.save_address_info(
            &swap_id,
            &internal_payout_address,
            address_index,
            &request.network_to,
            &request.recipient_address,
            request.recipient_extra_id.as_deref(),
        ).await
        .map_err(|e| SwapError::DatabaseError(format!("Failed to save address info: {}", e)))?;

        // 7. Transform to response
        Ok(super::schema::CreateSwapResponse {
            swap_id,
            provider: trocador_res.provider,
            from: request.from.clone(),
            to: request.to.clone(),
            deposit_address: trocador_res.address_provider,
            deposit_extra_id: trocador_res.address_provider_memo,
            deposit_amount: request.amount,
            recipient_address: request.recipient_address.clone(), // User sees THEIR address
            estimated_receive: estimated_user_receive,
            rate: estimated_user_receive / request.amount,
            status,
            rate_type: request.rate_type.clone(),
            is_sandbox: request.sandbox,
            expires_at: Utc::now() + chrono::Duration::minutes(60),
            created_at: Utc::now(),
        })
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
        // 1. Get swap from database - cast DECIMAL to DOUBLE for f64 compatibility
        let swap = sqlx::query!(
            r#"
            SELECT id, user_id, provider_id, provider_swap_id,
                   from_currency, from_network, to_currency, to_network,
                   CAST(amount AS DOUBLE) as "amount!: f64",
                   CAST(estimated_receive AS DOUBLE) as "estimated_receive!: f64",
                   CAST(actual_receive AS DOUBLE) as "actual_receive: f64",
                   CAST(rate AS DOUBLE) as "rate!: f64",
                   CAST(network_fee AS DOUBLE) as "network_fee!: f64",
                   CAST(provider_fee AS DOUBLE) as "provider_fee!: f64",
                   CAST(platform_fee AS DOUBLE) as "platform_fee!: f64",
                   CAST(total_fee AS DOUBLE) as "total_fee!: f64",
                   deposit_address, deposit_extra_id,
                   recipient_address, recipient_extra_id,
                   refund_address, refund_extra_id,
                   tx_hash_in, tx_hash_out,
                   status as "status!: super::schema::SwapStatus",
                   rate_type as "rate_type!: super::schema::RateType",
                   is_sandbox, error,
                   expires_at, completed_at, created_at, updated_at
            FROM swaps
            WHERE id = ?
            "#,
            swap_id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?
        .ok_or(SwapError::SwapNotFound)?;

        // 2. If we have a provider_swap_id, fetch latest status from Trocador
        if let Some(ref trocador_id) = swap.provider_swap_id {
            let api_key = std::env::var("TROCADOR_API_KEY")
                .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

            let trocador_client = TrocadorClient::new(api_key);

            // Call Trocador API with retry logic
            match self.call_trocador_with_retry(|| async {
                trocador_client.get_trade_status(trocador_id).await
            }).await {
                Ok(trocador_status) => {
                    // 3. Map Trocador status to our internal status
                    let new_status = self.map_trocador_status(&trocador_status.status);
                    
                    // 4. Update database if status changed
                    if new_status != swap.status {
                        self.update_swap_status(
                            swap_id,
                            &new_status,
                            trocador_status.amount_to,
                            None, // tx_hash_in from Trocador if available
                            None, // tx_hash_out from Trocador if available
                        ).await?;

                        // Log status change to history
                        self.log_status_change(swap_id, &new_status, None).await?;
                    }

                    // 5. Return updated status
                    return Ok(super::schema::SwapStatusResponse {
                        swap_id: swap.id.clone(),
                        provider: swap.provider_id.clone(),
                        provider_swap_id: swap.provider_swap_id.clone(),
                        status: new_status.clone(),
                        from: swap.from_currency.clone(),
                        to: swap.to_currency.clone(),
                        amount: swap.amount,
                        deposit_address: swap.deposit_address.clone(),
                        deposit_extra_id: swap.deposit_extra_id.clone(),
                        recipient_address: swap.recipient_address.clone(),
                        recipient_extra_id: swap.recipient_extra_id.clone(),
                        rate: swap.rate,
                        estimated_receive: swap.estimated_receive,
                        actual_receive: Some(trocador_status.amount_to),
                        network_fee: swap.network_fee,
                        total_fee: swap.total_fee,
                        rate_type: swap.rate_type.clone(),
                        is_sandbox: swap.is_sandbox != 0,
                        tx_hash_in: swap.tx_hash_in.clone(),
                        tx_hash_out: swap.tx_hash_out.clone(),
                        error: swap.error.clone(),
                        created_at: swap.created_at,
                        updated_at: Utc::now(),
                        expires_at: swap.expires_at,
                        completed_at: if new_status == super::schema::SwapStatus::Completed {
                            Some(Utc::now())
                        } else {
                            swap.completed_at
                        },
                    });
                }
                Err(e) => {
                    // If Trocador API fails, return cached status from database
                    tracing::warn!("Failed to fetch status from Trocador for swap {}: {}", swap_id, e);
                }
            }
        }

        // 6. Return status from database (if no provider_swap_id or Trocador call failed)
        Ok(super::schema::SwapStatusResponse {
            swap_id: swap.id,
            provider: swap.provider_id,
            provider_swap_id: swap.provider_swap_id,
            status: swap.status,
            from: swap.from_currency,
            to: swap.to_currency,
            amount: swap.amount,
            deposit_address: swap.deposit_address,
            deposit_extra_id: swap.deposit_extra_id,
            recipient_address: swap.recipient_address,
            recipient_extra_id: swap.recipient_extra_id,
            rate: swap.rate,
            estimated_receive: swap.estimated_receive,
            actual_receive: swap.actual_receive,
            network_fee: swap.network_fee,
            total_fee: swap.total_fee,
            rate_type: swap.rate_type,
            is_sandbox: swap.is_sandbox != 0,
            tx_hash_in: swap.tx_hash_in,
            tx_hash_out: swap.tx_hash_out,
            error: swap.error,
            created_at: swap.created_at,
            updated_at: swap.updated_at,
            expires_at: swap.expires_at,
            completed_at: swap.completed_at,
        })
    }

    /// Map Trocador status string to our SwapStatus enum
    fn map_trocador_status(&self, trocador_status: &str) -> super::schema::SwapStatus {
        match trocador_status {
            "new" | "waiting" => super::schema::SwapStatus::Waiting,
            "confirming" => super::schema::SwapStatus::Confirming,
            "exchanging" => super::schema::SwapStatus::Exchanging,
            "sending" => super::schema::SwapStatus::Sending,
            "finished" | "paid partially" => super::schema::SwapStatus::Completed,
            "failed" | "halted" => super::schema::SwapStatus::Failed,
            "refunded" => super::schema::SwapStatus::Refunded,
            "expired" => super::schema::SwapStatus::Expired,
            _ => super::schema::SwapStatus::Waiting,
        }
    }

    /// Update swap status in database
    async fn update_swap_status(
        &self,
        swap_id: &str,
        status: &super::schema::SwapStatus,
        actual_receive: f64,
        tx_hash_in: Option<String>,
        tx_hash_out: Option<String>,
    ) -> Result<(), SwapError> {
        let completed_at = if *status == super::schema::SwapStatus::Completed {
            Some(Utc::now())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE swaps
            SET status = ?,
                actual_receive = ?,
                tx_hash_in = COALESCE(?, tx_hash_in),
                tx_hash_out = COALESCE(?, tx_hash_out),
                completed_at = COALESCE(?, completed_at),
                updated_at = NOW()
            WHERE id = ?
            "#
        )
        .bind(status)
        .bind(actual_receive)
        .bind(tx_hash_in)
        .bind(tx_hash_out)
        .bind(completed_at)
        .bind(swap_id)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    /// Log status change to swap_status_history table
    async fn log_status_change(
        &self,
        swap_id: &str,
        status: &super::schema::SwapStatus,
        message: Option<String>,
    ) -> Result<(), SwapError> {
        sqlx::query(
            r#"
            INSERT INTO swap_status_history (swap_id, status, message, created_at)
            VALUES (?, ?, ?, NOW())
            "#
        )
        .bind(swap_id)
        .bind(status)
        .bind(message)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
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

        // 2. Get API key
        let api_key = std::env::var("TROCADOR_API_KEY")
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        let trocador_client = TrocadorClient::new(api_key);

        // 3. Call Trocador API with retry logic
        let is_valid = self.call_trocador_with_retry(|| async {
            trocador_client
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
    /// Handles rate limiting gracefully by retrying with increasing delays
    async fn call_trocador_with_retry<F, Fut, T>(
        &self,
        f: F,
    ) -> Result<T, SwapError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, TrocadorError>>,
    {
        let max_retries = 2; // Reduced from 5 to avoid long hangs
        let mut retries = 0;

        loop {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let error_msg = e.to_string();
                    
                    // Check if it's a rate limit error
                    let is_rate_limit = error_msg.contains("Rate limit")
                        || error_msg.contains("rate limit")
                        || error_msg.contains("429")
                        || error_msg.contains("Too Many Requests");

                    if is_rate_limit && retries < max_retries {
                        retries += 1;
                        // Linear backoff: 500ms, 1000ms
                        // Total max wait: ~1.5s
                        let delay_millis = retries * 500;
                        
                        tracing::warn!(
                            "Rate limit hit, retrying in {}ms (attempt {}/{})",
                            delay_millis,
                            retries,
                            max_retries
                        );
                        
                        tokio::time::sleep(Duration::from_millis(delay_millis as u64)).await;
                        continue;
                    }

                    // Not a rate limit error or max retries exceeded
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
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        
        // 1. Validate and parse cursor
        let cursor = if let Some(cursor_str) = &query.cursor {
            let bytes = URL_SAFE_NO_PAD.decode(cursor_str)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid base64: {}", e)))?;
            let json = String::from_utf8(bytes)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid UTF-8: {}", e)))?;
            let c: super::schema::HistoryCursor = serde_json::from_str(&json)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid JSON: {}", e)))?;
            Some(c)
        } else {
            None
        };
        
        // 2. Validate limit (min 1, max 100)
        let limit = query.limit.min(100).max(1);
        
        // 3. Parse date filters
        let date_from = query.date_from.as_ref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let date_to = query.date_to.as_ref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        
        // 4. Build dynamic SQL query with keyset pagination
        let mut sql = String::from(
            "SELECT 
                id, user_id, provider_id,
                CAST(status AS CHAR) as status,
                from_currency, from_network, to_currency, to_network,
                CAST(amount AS DOUBLE) as amount,
                CAST(estimated_receive AS DOUBLE) as estimated_receive,
                CAST(actual_receive AS DOUBLE) as actual_receive,
                CAST(rate AS DOUBLE) as rate,
                CAST(platform_fee AS DOUBLE) as platform_fee,
                CAST(total_fee AS DOUBLE) as total_fee,
                deposit_address, recipient_address,
                CAST(rate_type AS CHAR) as rate_type,
                is_sandbox,
                created_at, completed_at
            FROM swaps
            WHERE user_id = ?"
        );
        
        let mut bind_values: Vec<String> = vec![user_id.to_string()];
        
        // 5. Apply keyset cursor (CRITICAL for performance!)
        if let Some(ref c) = cursor {
            sql.push_str(" AND (created_at, id) < (?, ?)");
            bind_values.push(c.created_at.to_rfc3339());
            bind_values.push(c.id.clone());
        }
        
        // 6. Apply filters
        if let Some(ref status) = query.status {
            sql.push_str(" AND status = ?");
            bind_values.push(status.clone());
        }
        if let Some(ref from) = query.from_currency {
            sql.push_str(" AND from_currency = ?");
            bind_values.push(from.clone());
        }
        if let Some(ref to) = query.to_currency {
            sql.push_str(" AND to_currency = ?");
            bind_values.push(to.clone());
        }
        if let Some(ref provider) = query.provider {
            sql.push_str(" AND provider_id = ?");
            bind_values.push(provider.clone());
        }
        if let Some(dt) = date_from {
            sql.push_str(" AND created_at >= ?");
            bind_values.push(dt.to_rfc3339());
        }
        if let Some(dt) = date_to {
            sql.push_str(" AND created_at <= ?");
            bind_values.push(dt.to_rfc3339());
        }
        
        // 7. Apply sorting (default: created_at DESC, id DESC)
        let sort_by = query.sort_by.as_deref().unwrap_or("created_at");
        let sort_order = query.sort_order.as_deref().unwrap_or("desc").to_uppercase();
        sql.push_str(&format!(
            " ORDER BY {} {}, id {}",
            sort_by, sort_order, sort_order
        ));
        
        // 8. Fetch limit + 1 to detect "has_more"
        sql.push_str(&format!(" LIMIT {}", limit + 1));
        
        // 9. Build query with dynamic bindings
        let mut query_builder = sqlx::query(&sql);
        for value in &bind_values {
            query_builder = query_builder.bind(value);
        }
        
        // 10. Execute query
        let rows = query_builder
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        
        // 11. Process results
        let has_more = rows.len() > limit as usize;
        let swaps_data = if has_more {
            &rows[..limit as usize]
        } else {
            &rows[..]
        };
        
        // 12. Map to SwapSummary
        let swaps: Vec<super::schema::SwapSummary> = swaps_data.iter().map(|row| {
            use sqlx::Row;
            
            // Parse status string to enum
            let status_str: String = row.get("status");
            let status = match status_str.as_str() {
                "waiting" => super::schema::SwapStatus::Waiting,
                "confirming" => super::schema::SwapStatus::Confirming,
                "exchanging" => super::schema::SwapStatus::Exchanging,
                "sending" => super::schema::SwapStatus::Sending,
                "funds_received" => super::schema::SwapStatus::Completed, // Map to completed
                "completed" => super::schema::SwapStatus::Completed,
                "failed" => super::schema::SwapStatus::Failed,
                "refunded" => super::schema::SwapStatus::Refunded,
                "expired" => super::schema::SwapStatus::Expired,
                _ => super::schema::SwapStatus::Waiting,
            };
            
            // Parse rate_type string to enum
            let rate_type_str: String = row.get("rate_type");
            let rate_type = match rate_type_str.as_str() {
                "fixed" => super::schema::RateType::Fixed,
                _ => super::schema::RateType::Floating,
            };
            
            super::schema::SwapSummary {
                id: row.get("id"),
                status,
                from_currency: row.get("from_currency"),
                from_network: row.get("from_network"),
                to_currency: row.get("to_currency"),
                to_network: row.get("to_network"),
                amount: row.get("amount"),
                estimated_receive: row.get("estimated_receive"),
                actual_receive: row.try_get("actual_receive").ok(),
                rate: row.get("rate"),
                platform_fee: row.get("platform_fee"),
                total_fee: row.get("total_fee"),
                deposit_address: row.get("deposit_address"),
                recipient_address: row.get("recipient_address"),
                provider: row.get("provider_id"),
                rate_type,
                is_sandbox: row.get::<i8, _>("is_sandbox") != 0,
                created_at: row.get("created_at"),
                completed_at: row.try_get("completed_at").ok(),
            }
        }).collect();
        
        // 13. Generate next cursor
        let next_cursor = if has_more && !swaps.is_empty() {
            let last = &swaps[swaps.len() - 1];
            let cursor_obj = super::schema::HistoryCursor {
                created_at: last.created_at,
                id: last.id.clone(),
                status: query.status.clone(),
                from_currency: query.from_currency.clone(),
                to_currency: query.to_currency.clone(),
            };
            let json = serde_json::to_string(&cursor_obj).unwrap();
            Some(URL_SAFE_NO_PAD.encode(json.as_bytes()))
        } else {
            None
        };
        
        // 14. Build response
        Ok(super::schema::HistoryResponse {
            swaps,
            pagination: super::schema::PaginationInfo {
                limit,
                has_more,
                next_cursor,
            },
            filters_applied: super::schema::FiltersApplied {
                status: query.status,
                from_currency: query.from_currency,
                to_currency: query.to_currency,
                provider: query.provider,
                date_from: query.date_from,
                date_to: query.date_to,
            },
        })
    }

    // =============================================================================
    // ESTIMATE ENDPOINT - Quick rate preview without creating swap
    // =============================================================================
    
    /// Get estimate with optimized caching (60s TTL + bucketing + PER)
    pub async fn get_estimate_optimized(
        &self,
        query: &super::schema::EstimateQuery,
    ) -> Result<super::schema::EstimateResponse, SwapError> {
        use chrono::Utc;
        
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
        
        // 2. Try exact cache first (10s TTL for repeated requests)
        if let Some(service) = &self.redis_service {
            if let Ok(Some(cached)) = service.get_json::<super::schema::EstimateCacheEntry>(&exact_key).await {
                // Check if we should trigger early refresh (PER algorithm)
                if !Self::should_early_refresh(&cached) {
                    // Return cached response with updated metadata
                    let now = Utc::now().timestamp_millis();
                    let cache_age = ((now - cached.created_at) / 1000) as i64;
                    let expires_in = ((cached.expires_at - now) / 1000).max(0) as i64;
                    
                    let mut response = cached.response.clone();
                    response.cached = true;
                    response.cache_age_seconds = cache_age;
                    response.expires_in_seconds = expires_in;
                    
                    return Ok(response);
                } else {
                    // Trigger async refresh but return stale data
                    let query_clone = query.clone();
                    let pool_clone = self.pool.clone();
                    let redis_clone = self.redis_service.clone();
                    let wallet_clone = self.wallet_mnemonic.clone();
                    
                    tokio::spawn(async move {
                        let crud = SwapCrud::new(pool_clone, redis_clone, wallet_clone);
                        let _ = crud.fetch_estimate_from_api(&query_clone).await;
                    });
                    
                    // Return stale data immediately
                    let now = Utc::now().timestamp_millis();
                    let cache_age = ((now - cached.created_at) / 1000) as i64;
                    let mut response = cached.response.clone();
                    response.cached = true;
                    response.cache_age_seconds = cache_age;
                    response.expires_in_seconds = 0;
                    
                    return Ok(response);
                }
            }
            
            // 3. Try bucketed cache (60s TTL for similar amounts)
            if let Ok(Some(cached)) = service.get_json::<super::schema::EstimateCacheEntry>(&bucketed_key).await {
                if !Self::should_early_refresh(&cached) {
                    let now = Utc::now().timestamp_millis();
                    let cache_age = ((now - cached.created_at) / 1000) as i64;
                    let expires_in = ((cached.expires_at - now) / 1000).max(0) as i64;
                    
                    let mut response = cached.response.clone();
                    response.cached = true;
                    response.cache_age_seconds = cache_age;
                    response.expires_in_seconds = expires_in;
                    
                    return Ok(response);
                }
            }
        }
        
        // 4. Cache miss - fetch from API
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
        
        // 1. Fetch rates from Trocador (reuse existing logic)
        let rates_query = super::schema::RatesQuery {
            from: query.from.clone(),
            network_from: query.network_from.clone(),
            to: query.to.clone(),
            network_to: query.network_to.clone(),
            amount: query.amount,
            rate_type: None,
            provider: None,
        };
        
        let rates_response = self.fetch_rates_from_api(&rates_query).await?;
        
        if rates_response.rates.is_empty() {
            return Err(SwapError::PairNotAvailable);
        }
        
        // 2. Calculate provider spread (volatility indicator)
        let amounts: Vec<f64> = rates_response.rates.iter()
            .map(|r| r.estimated_amount)
            .collect();
        let max_amount = amounts.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_amount = amounts.iter().fold(f64::MAX, |a, &b| a.min(b));
        let provider_spread = if max_amount > 0.0 {
            (max_amount - min_amount) / max_amount
        } else {
            0.0
        };
        
        // 3. Estimate USD value (for slippage calculation)
        let usd_price = match query.from.to_lowercase().as_str() {
            "btc" => 60000.0,
            "eth" => 3000.0,
            "xmr" => 150.0,
            "usdt" | "usdc" | "dai" => 1.0,
            _ => 1.0,
        };
        let amount_usd = query.amount * usd_price;
        
        // 4. Build estimate response using pricing engine
        let pricing_engine = PricingEngine::new();
        let compute_time_ms = start_time.elapsed().as_millis() as i64;
        
        let response = pricing_engine.build_estimate_response(
            rates_response.rates,
            query,
            provider_spread,
            amount_usd,
            false, // not cached
            0,     // cache age
            60,    // expires in 60s
        );
        
        // 5. Cache the result
        if let Some(service) = &self.redis_service {
            let now = Utc::now().timestamp_millis();
            
            // Exact key cache (10s TTL)
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
            let _ = service.set_json(&exact_key, &exact_entry, 10).await;
            
            // Bucketed key cache (60s TTL)
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
