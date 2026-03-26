use chrono::Utc;
use sqlx::{MySql, Pool};
use std::time::Duration;

use super::crud::SwapError;
use super::repository::{NewSwapRecord, SwapRepository, SwapStatusRecord};
use super::schema::{CreateSwapRequest, CreateSwapResponse, SwapStatus, SwapStatusResponse};
use crate::modules::wallet::crud::WalletCrud;
use crate::services::gas::GasEstimator;
use crate::services::pricing::CommissionService;
use crate::services::redis_cache::RedisService;
use crate::services::trocador::{TrocadorError, TrocadorGateway};
use crate::services::wallet::validation::{
    normalize_supported_recipient_extra_id, supported_recipient_extra_id_format,
    validate_address_by_network_family, AddressValidation,
};

pub struct SwapService {
    pool: Pool<MySql>,
    repository: SwapRepository,
    redis_service: Option<RedisService>,
    wallet_mnemonic: Option<String>,
    gas_estimator: GasEstimator,
}

impl SwapService {
    pub fn new(
        pool: Pool<MySql>,
        redis_service: Option<RedisService>,
        wallet_mnemonic: Option<String>,
    ) -> Self {
        let gas_estimator = GasEstimator::new(redis_service.clone());
        Self {
            repository: SwapRepository::new(pool.clone()),
            pool,
            redis_service,
            wallet_mnemonic,
            gas_estimator,
        }
    }

    pub async fn create_swap(
        &self,
        request: &CreateSwapRequest,
        user_id: Option<String>,
    ) -> Result<CreateSwapResponse, SwapError> {
        let trocador_gateway = TrocadorGateway::from_env()
            .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;
        let commission_service = CommissionService::new();
        let swap_id = uuid::Uuid::new_v4().to_string();
        let recipient_address = request.recipient_address.trim().to_string();
        let recipient_extra_id = normalize_supported_recipient_extra_id(
            &request.to,
            &request.network_to,
            request.recipient_extra_id.as_deref(),
        )
        .map_err(SwapError::ValidationError)?;

        tracing::info!(
            "🟢 Starting swap creation: {} {} -> {} {}, amount: {}, provider: {}",
            request.from,
            request.network_from,
            request.to,
            request.network_to,
            request.amount,
            request.provider
        );

        self.validate_recipient_destination(
            &trocador_gateway,
            request,
            &recipient_address,
            recipient_extra_id.as_deref(),
        )
        .await?;

        let (internal_payout_address, address_index) = if let Some(mnemonic) = &self.wallet_mnemonic
        {
            let wallet_crud = WalletCrud::new(self.pool.clone());
            let index = wallet_crud
                .get_next_index()
                .await
                .map_err(|e| SwapError::DatabaseError(format!("Wallet error: {}", e)))?;

            let addr = crate::services::wallet::derivation::derive_address(
                mnemonic,
                &request.to,
                &request.network_to,
                index,
            )
            .await
            .map_err(|e| SwapError::DatabaseError(format!("Derivation error: {}", e)))?;

            tracing::info!(
                "🟢 Generated internal payout address for {} on {}: {} (index: {})",
                request.to,
                request.network_to,
                addr,
                index
            );
            (addr, index)
        } else {
            return Err(SwapError::DatabaseError(
                "Wallet mnemonic not configured".to_string(),
            ));
        };

        tracing::info!("🟡 Validating internal payout address with Trocador...");
        let is_valid = trocador_gateway
            .validate_address(&request.to, &request.network_to, &internal_payout_address)
            .await
            .map_err(|e| {
                SwapError::ExternalApiError(format!("Address validation failed: {}", e))
            })?;

        if !is_valid {
            tracing::error!(
                "🔴 Generated address is invalid: {} for {} on {}",
                internal_payout_address,
                request.to,
                request.network_to
            );
            return Err(SwapError::ExternalApiError(format!(
                "Generated payout address is invalid for {} on {}. This is a system error - please contact support.",
                request.to, request.network_to
            )));
        }
        tracing::info!("✅ Address validated successfully");

        let provider_spread = self
            .resolve_provider_spread_for_create(request, &trocador_gateway, &commission_service)
            .await;

        let fixed = matches!(request.rate_type, super::schema::RateType::Fixed);
        let trocador_res = self
            .call_trocador_with_retry(|| async {
                let res = trocador_gateway
                    .create_trade(
                        request.trade_id.as_deref(),
                        &request.from,
                        &request.network_from,
                        &request.to,
                        &request.network_to,
                        request.amount,
                        &internal_payout_address,
                        request.refund_address.as_deref(),
                        &request.provider,
                        fixed,
                        request.payment,
                        request.min_kycrating.as_deref(),
                    )
                    .await;

                if let Err(ref e) = res {
                    tracing::error!("Trocador create_trade failed: {}", e);
                }
                res
            })
            .await?;

        let normalized_payout_network =
            GasEstimator::normalize_payout_network(&request.to, &request.network_to);
        let gas_cost = self
            .gas_estimator
            .get_gas_cost_for_network(&normalized_payout_network)
            .await;
        let amount_usd = commission_service
            .resolve_live_amount_usd(
                &trocador_gateway,
                &request.from,
                &request.network_from,
                request.amount,
            )
            .await
            .map_err(|e| {
                SwapError::ExternalApiError(format!(
                    "Failed to resolve live market price from Trocador: {}",
                    e
                ))
            })?;
        let commission = commission_service.calculate_commission(
            amount_usd,
            trocador_res.amount_to,
            gas_cost,
            provider_spread,
        );
        let network_fee = gas_cost.max(0.0);
        let platform_fee = commission.platform_fee;
        let estimated_user_receive = (commission.user_receive - network_fee).max(0.0);

        let status = Self::map_created_trade_status(&trocador_res.status);
        let normalized_provider_id = self
            .repository
            .ensure_provider_exists(&request.provider)
            .await?;

        let rate = estimated_user_receive / request.amount;
        self.repository
            .insert_swap(NewSwapRecord {
                id: &swap_id,
                user_id: user_id.as_deref(),
                provider_id: &normalized_provider_id,
                provider_swap_id: &trocador_res.trade_id,
                from_currency: &request.from,
                from_network: &request.network_from,
                to_currency: &request.to,
                to_network: &request.network_to,
                amount: request.amount,
                estimated_receive: estimated_user_receive,
                rate,
                network_fee,
                deposit_address: &trocador_res.address_provider,
                deposit_extra_id: trocador_res.address_provider_memo.as_deref(),
                recipient_address: &recipient_address,
                recipient_extra_id: recipient_extra_id.as_deref(),
                refund_address: request.refund_address.as_deref(),
                refund_extra_id: request.refund_extra_id.as_deref(),
                platform_fee,
                total_fee: platform_fee + network_fee,
                status: status.clone(),
                rate_type: request.rate_type.clone(),
                is_sandbox: request.sandbox,
                is_payment: trocador_res.payment.unwrap_or(false),
            })
            .await?;

        let wallet_crud = WalletCrud::new(self.pool.clone());
        let coin_type = crate::services::wallet::derivation::resolve_coin_type(
            &request.to,
            &request.network_to,
        )
        .map_err(|e| SwapError::DatabaseError(format!("Coin type resolution error: {}", e)))?
            as i32;
        wallet_crud
            .save_address_info(
                &swap_id,
                &internal_payout_address,
                address_index,
                coin_type,
                &recipient_address,
                recipient_extra_id.as_deref(),
            )
            .await
            .map_err(|e| SwapError::DatabaseError(format!("Failed to save address info: {}", e)))?;

        Ok(CreateSwapResponse {
            swap_id,
            provider: trocador_res.provider.clone(),
            from: request.from.clone(),
            from_name: trocador_res
                .coin_from
                .unwrap_or_else(|| request.from.clone()),
            to: request.to.clone(),
            to_name: trocador_res.coin_to.unwrap_or_else(|| request.to.clone()),
            network_from: request.network_from.clone(),
            network_to: request.network_to.clone(),
            deposit_address: trocador_res.address_provider,
            deposit_extra_id: trocador_res.address_provider_memo,
            deposit_amount: request.amount,
            recipient_address,
            estimated_receive: estimated_user_receive,
            rate: estimated_user_receive / request.amount,
            status,
            rate_type: request.rate_type.clone(),
            is_sandbox: request.sandbox,
            is_payment: trocador_res.payment.unwrap_or(false),
            expires_at: Utc::now() + chrono::Duration::minutes(60),
            created_at: Utc::now(),
        })
    }

    async fn validate_recipient_destination(
        &self,
        trocador_gateway: &TrocadorGateway,
        request: &CreateSwapRequest,
        recipient_address: &str,
        recipient_extra_id: Option<&str>,
    ) -> Result<(), SwapError> {
        if recipient_address.is_empty() {
            return Err(SwapError::ValidationError(
                "Recipient address is required".to_string(),
            ));
        }

        if let AddressValidation::Invalid { reason, .. } =
            validate_address_by_network_family(&request.to, &request.network_to, recipient_address)
        {
            return Err(SwapError::ValidationError(format!(
                "Recipient address is invalid for {} on {}: {}",
                request.to, request.network_to, reason
            )));
        }

        let is_valid = self
            .call_trocador_with_retry(|| async {
                trocador_gateway
                    .validate_address(&request.to, &request.network_to, recipient_address)
                    .await
            })
            .await
            .map_err(|e| {
                SwapError::ExternalApiError(format!("Recipient address validation failed: {}", e))
            })?;

        if !is_valid {
            return Err(SwapError::ValidationError(format!(
                "Recipient address is invalid for {} on {}",
                request.to, request.network_to
            )));
        }

        let destination_requires_extra_id = self
            .destination_requires_extra_id(trocador_gateway, &request.to, &request.network_to)
            .await?;

        if !destination_requires_extra_id {
            return Ok(());
        }

        let Some(format) = supported_recipient_extra_id_format(&request.to, &request.network_to)
        else {
            return Err(SwapError::ValidationError(format!(
                "{} on {} requires a memo/tag, but this payout route does not support recipient_extra_id yet",
                request.to, request.network_to
            )));
        };

        if recipient_extra_id.is_none() {
            return Err(SwapError::ValidationError(format!(
                "{} on {} requires a {}",
                request.to,
                request.network_to,
                format.label()
            )));
        }

        Ok(())
    }

    async fn destination_requires_extra_id(
        &self,
        trocador_gateway: &TrocadorGateway,
        ticker: &str,
        network: &str,
    ) -> Result<bool, SwapError> {
        if let Some(requires_extra_id) = sqlx::query_scalar::<_, i8>(
            r#"
            SELECT requires_extra_id
            FROM currencies
            WHERE LOWER(symbol) = LOWER(?)
              AND LOWER(network) = LOWER(?)
            LIMIT 1
            "#,
        )
        .bind(ticker)
        .bind(network)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(format!("Currency lookup failed: {}", e)))?
        {
            return Ok(requires_extra_id != 0);
        }

        let currencies = self
            .call_trocador_with_retry(|| async { trocador_gateway.fetch_currencies().await })
            .await?;

        Ok(currencies
            .iter()
            .find(|currency| {
                currency.ticker.eq_ignore_ascii_case(ticker)
                    && currency.network.eq_ignore_ascii_case(network)
            })
            .map(|currency| currency.memo)
            .unwrap_or(false))
    }

    pub async fn get_swap_status(&self, swap_id: &str) -> Result<SwapStatusResponse, SwapError> {
        let swap = self
            .repository
            .get_swap_status_record(swap_id)
            .await?
            .ok_or(SwapError::SwapNotFound)?;

        if let Some(ref trocador_id) = swap.provider_swap_id {
            let trocador_gateway = TrocadorGateway::from_env()
                .map_err(|_| SwapError::ExternalApiError("TROCADOR_API_KEY not set".to_string()))?;

            match self
                .call_trocador_with_retry(|| async {
                    trocador_gateway.fetch_trade_status(trocador_id).await
                })
                .await
            {
                Ok(trocador_status) => {
                    let provider_status = Self::map_trocador_status(&trocador_status.status);
                    let new_status = swap.status.reconcile_with_provider(provider_status);

                    if new_status != swap.status {
                        self.update_swap_status(
                            swap_id,
                            &new_status,
                            trocador_status.amount_to,
                            None,
                            None,
                        )
                        .await?;
                        self.log_status_change(swap_id, &new_status, None).await?;
                    }

                    return Ok(Self::build_live_status_response(
                        &swap,
                        new_status,
                        trocador_status.amount_to,
                    ));
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch status from Trocador for swap {}: {}",
                        swap_id,
                        e
                    );
                }
            }
        }

        Ok(Self::build_cached_status_response(swap))
    }

    async fn cache_trade_provider_spread(&self, trade_id: &str, provider_spread: f64) {
        if let Some(service) = &self.redis_service {
            let cache_key = format!("trocador:pricing:trade:{}:provider_spread", trade_id);
            let _ = service
                .set_string(&cache_key, &provider_spread.to_string(), 600)
                .await;
        }
    }

    async fn get_cached_trade_provider_spread(&self, trade_id: Option<&str>) -> Option<f64> {
        let trade_id = trade_id?;
        let service = self.redis_service.as_ref()?;
        let cache_key = format!("trocador:pricing:trade:{}:provider_spread", trade_id);

        service
            .get_string(&cache_key)
            .await
            .ok()
            .flatten()
            .and_then(|value| value.parse::<f64>().ok())
    }

    async fn resolve_provider_spread_for_create(
        &self,
        request: &CreateSwapRequest,
        trocador_gateway: &TrocadorGateway,
        commission_service: &CommissionService,
    ) -> f64 {
        if let Some(provider_spread) = self
            .get_cached_trade_provider_spread(request.trade_id.as_deref())
            .await
        {
            return provider_spread;
        }

        let live_rates = self
            .call_trocador_with_retry(|| async {
                trocador_gateway
                    .fetch_rates(
                        &request.from,
                        &request.network_from,
                        &request.to,
                        &request.network_to,
                        request.amount,
                        request.min_kycrating.as_deref(),
                    )
                    .await
            })
            .await;

        match live_rates {
            Ok(rates) => {
                let provider_spread =
                    commission_service.calculate_quote_spread(&rates.quotes.quotes);

                if let Some(trade_id) = request.trade_id.as_deref() {
                    self.cache_trade_provider_spread(trade_id, provider_spread)
                        .await;
                }

                provider_spread
            }
            Err(error) => {
                tracing::warn!(
                    "Failed to resolve provider spread for swap create; falling back to zero spread: {}",
                    error
                );
                0.0
            }
        }
    }

    fn map_created_trade_status(trocador_status: &str) -> SwapStatus {
        SwapStatus::from_trocador_status(trocador_status)
    }

    fn map_trocador_status(trocador_status: &str) -> SwapStatus {
        SwapStatus::from_trocador_status(trocador_status)
    }

    async fn update_swap_status(
        &self,
        swap_id: &str,
        status: &SwapStatus,
        actual_receive: f64,
        tx_hash_in: Option<String>,
        tx_hash_out: Option<String>,
    ) -> Result<(), SwapError> {
        self.repository
            .update_swap_status(swap_id, status, actual_receive, tx_hash_in, tx_hash_out)
            .await
    }

    async fn log_status_change(
        &self,
        swap_id: &str,
        status: &SwapStatus,
        message: Option<String>,
    ) -> Result<(), SwapError> {
        self.repository
            .log_status_change(swap_id, status, message)
            .await
    }

    fn build_live_status_response(
        swap: &SwapStatusRecord,
        status: SwapStatus,
        actual_receive: f64,
    ) -> SwapStatusResponse {
        SwapStatusResponse {
            swap_id: swap.id.clone(),
            provider: swap.provider_id.clone(),
            provider_swap_id: swap.provider_swap_id.clone(),
            status: status.clone(),
            from: swap.from_currency.clone(),
            to: swap.to_currency.clone(),
            amount: swap.amount,
            deposit_address: swap.deposit_address.clone(),
            deposit_extra_id: swap.deposit_extra_id.clone(),
            recipient_address: swap.recipient_address.clone(),
            recipient_extra_id: swap.recipient_extra_id.clone(),
            rate: swap.rate,
            estimated_receive: swap.estimated_receive,
            actual_receive: Some(actual_receive),
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
            completed_at: if status == SwapStatus::Completed {
                Some(Utc::now())
            } else {
                swap.completed_at
            },
        }
    }

    fn build_cached_status_response(swap: SwapStatusRecord) -> SwapStatusResponse {
        SwapStatusResponse {
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
        }
    }

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
                    let error_msg = e.to_string();
                    let is_rate_limit = error_msg.contains("Rate limit")
                        || error_msg.contains("rate limit")
                        || error_msg.contains("429")
                        || error_msg.contains("Too Many Requests");
                    let is_transient_error = error_msg.contains("error sending request")
                        || error_msg.contains("connection")
                        || error_msg.contains("timeout")
                        || error_msg.contains("502")
                        || error_msg.contains("503")
                        || error_msg.contains("Bad Gateway");

                    if (is_rate_limit || is_transient_error) && retries < max_retries {
                        retries += 1;
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

                        tokio::time::sleep(Duration::from_millis(delay_millis)).await;
                        continue;
                    }

                    return Err(SwapError::from(e));
                }
            }
        }
    }
}
