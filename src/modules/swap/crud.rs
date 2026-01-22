use chrono::Utc;
use sqlx::{MySql, Pool};
use std::time::Duration;

use super::model::{Currency, Provider};
use super::schema::{CurrenciesQuery, ProvidersQuery, TrocadorCurrency, TrocadorProvider};
use crate::services::trocador::{TrocadorClient, TrocadorError};
use crate::services::redis_cache::RedisService;

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
    RedisError(String), // Added RedisError
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
}

impl SwapCrud {
    pub fn new(pool: Pool<MySql>, redis_service: Option<RedisService>) -> Self {
        Self { pool, redis_service }
    }

    // =========================================================================
    // CURRENCIES
    // =========================================================================

    /// Check if currencies cache needs refresh (>5 minutes old)
    pub async fn should_sync_currencies(&self) -> Result<bool, SwapError> {
        let result: Option<(Option<chrono::DateTime<Utc>>,)> = sqlx::query_as(
            "SELECT MAX(last_synced_at) FROM currencies"
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        match result {
            Some((Some(last_sync),)) => {
                let cache_age = Utc::now() - last_sync;
                // Refresh if older than 5 minutes
                Ok(cache_age.num_minutes() > 5)
            }
            _ => Ok(true), // No sync found, need to sync
        }
    }

    /// Sync currencies from Trocador API and upsert into database
    pub async fn sync_currencies_from_trocador(
        &self,
        trocador_client: &TrocadorClient,
    ) -> Result<usize, SwapError> {
        // Fetch from Trocador API
        let trocador_currencies = trocador_client.get_currencies().await?;

        let mut synced_count = 0;

        // Upsert each currency
        for trocador_currency in trocador_currencies {
            self.upsert_currency_from_trocador(&trocador_currency).await?;
            synced_count += 1;
        }

        Ok(synced_count)
    }

    /// Upsert a single currency from Trocador data
    async fn upsert_currency_from_trocador(
        &self,
        trocador_currency: &TrocadorCurrency,
    ) -> Result<(), SwapError> {
        sqlx::query(
            r#"
            INSERT INTO currencies (
                symbol, name, network, is_active, logo_url,
                requires_extra_id, min_amount, max_amount, last_synced_at
            )
            VALUES (?, ?, ?, TRUE, ?, ?, ?, ?, NOW())
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                logo_url = VALUES(logo_url),
                min_amount = VALUES(min_amount),
                max_amount = VALUES(max_amount),
                last_synced_at = NOW()
            "#
        )
        .bind(&trocador_currency.ticker)
        .bind(&trocador_currency.name)
        .bind(&trocador_currency.network)
        .bind(&trocador_currency.image)
        .bind(trocador_currency.memo)
        .bind(trocador_currency.minimum)
        .bind(trocador_currency.maximum)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    /// Get currencies from database with optional filtering
    pub async fn get_currencies(
        &self,
        query: CurrenciesQuery,
    ) -> Result<Vec<Currency>, SwapError> {
        let mut sql = String::from(
            "SELECT id, symbol, name, network, is_active, logo_url, contract_address, 
             decimals, requires_extra_id, extra_id_name, min_amount, max_amount, 
             last_synced_at, created_at, updated_at 
             FROM currencies 
             WHERE is_active = TRUE"
        );

        // Build query based on filters
        let mut sql_parts = Vec::new();

        if let Some(ref ticker) = query.ticker {
            sql_parts.push(format!("LOWER(symbol) = LOWER('{}')", ticker.replace("'", "''")));
        }

        if let Some(ref network) = query.network {
            sql_parts.push(format!("network = '{}'", network.replace("'", "''")));
        }

        if let Some(memo) = query.memo {
            sql_parts.push(format!("requires_extra_id = {}", memo));
        }

        if !sql_parts.is_empty() {
            sql.push_str(" AND ");
            sql.push_str(&sql_parts.join(" AND "));
        }

        sql.push_str(" ORDER BY symbol, network");

        let currencies = sqlx::query_as::<_, Currency>(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(currencies)
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

    /// Sync providers from Trocador API and upsert into database
    pub async fn sync_providers_from_trocador(
        &self,
        trocador_client: &TrocadorClient,
    ) -> Result<usize, SwapError> {
        let trocador_providers = trocador_client.get_providers().await?;

        let mut synced_count = 0;

        for trocador_provider in trocador_providers {
            self.upsert_provider_from_trocador(&trocador_provider).await?;
            synced_count += 1;
        }

        Ok(synced_count)
    }

    /// Upsert a single provider from Trocador data
    async fn upsert_provider_from_trocador(
        &self,
        trocador_provider: &TrocadorProvider,
    ) -> Result<(), SwapError> {
        // Generate slug from name
        let slug = trocador_provider.name.to_lowercase().replace(" ", "-");
        let id = slug.clone();

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
    // RATES
    // =========================================================================

    /// Get live rates from Trocador and transform them into our response format
    pub async fn get_rates(
        &self,
        query: &super::schema::RatesQuery,
    ) -> Result<super::schema::RatesResponse, SwapError> {
        // 0. Check Redis Cache
        let cache_key = format!(
            "rates:{}:{}:{}:{}:{}",
            query.from, query.to, query.network_from, query.network_to, query.amount
        );

        if let Some(service) = &self.redis_service {
            match service.get_json::<super::schema::RatesResponse>(&cache_key).await {
                Ok(Some(cached)) => {
                    tracing::debug!("Cache hit for rates: {}", cache_key);
                    return Ok(cached);
                }
                Err(e) => tracing::warn!("Redis error: {}", e),
                _ => {}
            }
        }

        let api_key = std::env::var("TROCADOR_API_KEY")
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

        let trocador_client = TrocadorClient::new(api_key);

        // 1. Call the Service layer (Trocador API) with retry logic
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

        // 2. Transform and sort the quotes
        let mut rates: Vec<super::schema::RateResponse> = trocador_res
            .quotes
            .quotes
            .into_iter()
            .map(|quote| {
                let amount_to = quote.amount_to.parse::<f64>().unwrap_or(0.0);
                let waste = quote.waste.as_deref().unwrap_or("0.0").parse::<f64>().unwrap_or(0.0);
                let total_fee = waste;
                
                super::schema::RateResponse {
                    provider: quote.provider.clone(),
                    provider_name: quote.provider.clone(),
                    rate: amount_to / query.amount,
                    estimated_amount: amount_to,
                    min_amount: quote.min_amount.unwrap_or(0.0),
                    max_amount: quote.max_amount.unwrap_or(0.0),
                    network_fee: 0.0,
                    provider_fee: total_fee,
                    platform_fee: 0.0, // We can add our markup here later
                    total_fee,
                    rate_type: query.rate_type.clone().unwrap_or(super::schema::RateType::Floating),
                    kyc_required: quote.kycrating.as_deref().unwrap_or("D") != "A",
                    kyc_rating: quote.kycrating,
                    eta_minutes: quote.eta.map(|e| e as u32).or(Some(15)),
                }
            })
            .collect();

        // 3. Sort by best price (highest estimated_amount)
        rates.sort_by(|a, b| {
            b.estimated_amount
                .partial_cmp(&a.estimated_amount)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let response = super::schema::RatesResponse {
            trade_id: trocador_res.trade_id,
            from: query.from.clone(),
            network_from: query.network_from.clone(),
            to: query.to.clone(),
            network_to: query.network_to.clone(),
            amount: query.amount,
            rates,
        };

        // 4. Cache the result
        if let Some(service) = &self.redis_service {
            // Set with TTL of 30 seconds
            if let Err(e) = service.set_json(&cache_key, &response, 30).await {
                tracing::warn!("Failed to cache rates: {}", e);
            } else {
                tracing::debug!("Cached rates for: {}", cache_key);
            }
        }

        Ok(response)
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

        // 1. Call Trocador API with retry logic
        let fixed = matches!(request.rate_type, super::schema::RateType::Fixed);

        let trocador_res = self.call_trocador_with_retry(|| async {
            trocador_client
                .create_trade(
                    request.trade_id.as_deref(),
                    &request.from,
                    &request.network_from,
                    &request.to,
                    &request.network_to,
                    request.amount,
                    &request.recipient_address,
                    request.refund_address.as_deref(),
                    &request.provider,
                    fixed,
                )
                .await
        })
        .await?;

        // 2. Map Trocador status to our internal SwapStatus
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

        // 3. Generate local ID and save to database
        let swap_id = uuid::Uuid::new_v4().to_string();
        
        sqlx::query(
            r#"
            INSERT INTO swaps (
                id, user_id, provider_id, provider_swap_id,
                from_currency, from_network, to_currency, to_network,
                amount, estimated_receive, rate,
                deposit_address, deposit_extra_id,
                recipient_address, recipient_extra_id,
                refund_address, refund_extra_id,
                status, rate_type, is_sandbox,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            "#
        )
        .bind(&swap_id)
        .bind(user_id)
        .bind(&request.provider)
        .bind(&trocador_res.trade_id)
        .bind(&request.from)
        .bind(&request.network_from)
        .bind(&request.to)
        .bind(&request.network_to)
        .bind(request.amount)
        .bind(trocador_res.amount_to)
        .bind(trocador_res.amount_to / request.amount) // rate
        .bind(&trocador_res.address_provider)
        .bind(&trocador_res.address_provider_memo)
        .bind(&request.recipient_address)
        .bind(&request.recipient_extra_id)
        .bind(&request.refund_address)
        .bind(&request.refund_extra_id)
        .bind(status.clone())
        .bind(&request.rate_type)
        .bind(request.sandbox)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        // 4. Transform to response
        Ok(super::schema::CreateSwapResponse {
            swap_id,
            provider: trocador_res.provider,
            from: request.from.clone(),
            to: request.to.clone(),
            deposit_address: trocador_res.address_provider,
            deposit_extra_id: trocador_res.address_provider_memo,
            deposit_amount: request.amount,
            recipient_address: request.recipient_address.clone(),
            estimated_receive: trocador_res.amount_to,
            rate: trocador_res.amount_to / request.amount,
            status,
            rate_type: request.rate_type.clone(),
            is_sandbox: request.sandbox,
            expires_at: Utc::now() + chrono::Duration::minutes(60), // Default expiry if not provided
            created_at: Utc::now(),
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
        let max_retries = 3;
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
                        // Exponential backoff: 1s, 2s, 4s
                        let delay_secs = 2u64.pow(retries - 1);
                        
                        tracing::warn!(
                            "Rate limit hit, retrying in {}s (attempt {}/{})",
                            delay_secs,
                            retries,
                            max_retries
                        );
                        
                        tokio::time::sleep(Duration::from_secs(delay_secs)).await;
                        continue;
                    }

                    // Not a rate limit error or max retries exceeded
                    return Err(SwapError::from(e));
                }
            }
        }
    }
}
