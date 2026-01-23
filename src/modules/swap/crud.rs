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
        // Check if we need to sync from Trocador first
        let should_sync = self.should_sync_currencies().await.unwrap_or(true);
        
        if should_sync {
            if let Some(service) = &self.redis_service {
                let rate_limit_key = "api_calls:trocador:currencies";
                if service.check_rate_limit(rate_limit_key, 2, 300).await.unwrap_or(false) {
                    let api_key = std::env::var("TROCADOR_API_KEY")
                        .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;
                    let client = TrocadorClient::new(api_key);
                    
                    if let Err(e) = self.sync_currencies_from_trocador(&client).await {
                        tracing::warn!("Failed to sync currencies: {}", e);
                    }
                }
            }
        }

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
        let cache_key = format!(
            "rates:{}:{}:{}:{}:{}",
            query.from, query.to, query.network_from, query.network_to, query.amount
        );

        if let Some(service) = &self.redis_service {
            // Try cache first
            if let Ok(Some(cached)) = service.get_json::<super::schema::RatesResponse>(&cache_key).await {
                return Ok(cached);
            }

            // Check rate limit before making API call
            let rate_limit_key = "api_calls:trocador:rates";
            match service.check_rate_limit(rate_limit_key, 5, 60).await {
                Ok(false) => {
                    // Rate limited, wait and retry with exponential backoff
                    tracing::warn!("Rate limit hit for rates API, will retry with backoff");
                }
                _ => {}
            }
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
        let max_retries = 5;
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
                        // Exponential backoff: 2s, 4s, 8s, 16s, 32s
                        let delay_secs = 2u64.pow(retries);
                        
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
