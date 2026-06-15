use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool};
use std::sync::Arc;
use std::time::Duration;

use super::crud::SwapError;
use super::repository::{NewSwapRecord, SwapRepository, SwapStatusRecord};
use super::schema::{CreateSwapRequest, CreateSwapResponse, SwapStatus, SwapStatusResponse};
use crate::modules::wallet::crud::WalletCrud;
use crate::services::gas::GasEstimator;
use crate::services::payout_policy::PayoutPolicyConfig;
use crate::services::pricing::CommissionService;
use crate::services::redis_cache::RedisService;
use crate::services::rpc::{
    resolve_configured_send_chain_key, supports_direct_provider_chain, RpcManager,
};
use crate::services::trocador::{swap_markup_from_env, TrocadorError, TrocadorGateway};
use crate::services::wallet::manager::{
    canonical_payout_asset_network, ensure_local_payout_capability,
};
use crate::services::wallet::signing::SigningService;
use crate::services::wallet::validation::{
    normalize_supported_recipient_extra_id, supported_recipient_extra_id_format,
    validate_address_by_network_family, AddressValidation,
};
use alloy::primitives::U256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayoutRoute {
    DirectSettlement,
    ProviderManaged,
}

pub struct SwapService {
    pool: Pool<MySql>,
    repository: SwapRepository,
    redis_service: Option<RedisService>,
    wallet_mnemonic: Option<String>,
    gas_estimator: GasEstimator,
    rpc_manager: Arc<RpcManager>,
    payout_policy: PayoutPolicyConfig,
}

impl SwapService {
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

    pub async fn create_swap(
        &self,
        request: &CreateSwapRequest,
        user_id: Option<String>,
        client_id: Option<String>,
    ) -> Result<CreateSwapResponse, SwapError> {
        self.create_swap_internal(request, user_id, client_id, false)
            .await
    }

    pub async fn create_provider_managed_swap(
        &self,
        request: &CreateSwapRequest,
        user_id: Option<String>,
        client_id: Option<String>,
    ) -> Result<CreateSwapResponse, SwapError> {
        self.create_swap_internal(request, user_id, client_id, true)
            .await
    }

    async fn create_swap_internal(
        &self,
        request: &CreateSwapRequest,
        user_id: Option<String>,
        client_id: Option<String>,
        force_provider_managed: bool,
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
        let refund_address = request
            .refund_address
            .as_ref()
            .map(|address| address.trim().to_string())
            .filter(|address| !address.is_empty());

        if refund_address.is_none() && request.refund_extra_id.is_some() {
            return Err(SwapError::ValidationError(
                "refund_extra_id was provided without a refund_address".to_string(),
            ));
        }

        let refund_extra_id = if refund_address.is_some() {
            normalize_supported_recipient_extra_id(
                &request.from,
                &request.network_from,
                request.refund_extra_id.as_deref(),
            )
            .map_err(SwapError::ValidationError)?
        } else {
            None
        };

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

        let initial_payout_route = if force_provider_managed {
            tracing::info!(
                "🟢 Hosted swap forcing provider-managed payout for {}/{}",
                request.to,
                request.network_to
            );
            PayoutRoute::ProviderManaged
        } else {
            self.resolve_payout_route(
                &request.to,
                &request.network_to,
                recipient_extra_id.as_deref(),
            )
            .await?
        };
        let destination_requires_extra_id = self
            .currency_requires_extra_id(&trocador_gateway, &request.to, &request.network_to)
            .await?;
        let source_requires_extra_id = if refund_address.is_some() {
            self.currency_requires_extra_id(&trocador_gateway, &request.from, &request.network_from)
                .await?
        } else {
            false
        };

        let (payout_route, trocador_payout_address, address_tracking) = match initial_payout_route {
            PayoutRoute::DirectSettlement => {
                let mnemonic = self.wallet_mnemonic.as_ref().ok_or_else(|| {
                    SwapError::DatabaseError("Wallet mnemonic not configured".to_string())
                })?;
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
                    "🟢 Using internal direct-settlement route for {} on {} via {} (index: {})",
                    request.to,
                    request.network_to,
                    addr,
                    index
                );

                if let Err(reason) = self
                    .preflight_local_signing_capability(
                        &request.to,
                        &request.network_to,
                        index,
                        &addr,
                    )
                    .await
                {
                    tracing::warn!(
                        "⚠️ Local payout preflight failed for {}/{} at {} (index: {}): {}. Falling back to provider-managed payout.",
                        request.to,
                        request.network_to,
                        addr,
                        index,
                        reason
                    );
                    (
                        PayoutRoute::ProviderManaged,
                        recipient_address.clone(),
                        None,
                    )
                } else {
                    tracing::info!("🟡 Validating internal payout address with Trocador...");
                    let is_valid = trocador_gateway
                        .validate_address(&request.to, &request.network_to, &addr)
                        .await
                        .map_err(|e| {
                            SwapError::ExternalApiError(format!("Address validation failed: {}", e))
                        })?;

                    if !is_valid {
                        tracing::warn!(
                            "⚠️ Generated internal payout address {} was rejected by Trocador for {}/{}. Falling back to provider-managed payout.",
                            addr,
                            request.to,
                            request.network_to
                        );
                        (
                            PayoutRoute::ProviderManaged,
                            recipient_address.clone(),
                            None,
                        )
                    } else {
                        tracing::info!("✅ Address validated successfully");

                        let coin_type = crate::services::wallet::derivation::resolve_coin_type(
                            &request.to,
                            &request.network_to,
                        )
                        .map_err(|e| {
                            SwapError::DatabaseError(format!("Coin type resolution error: {}", e))
                        })? as i32;

                        (
                            PayoutRoute::DirectSettlement,
                            addr.clone(),
                            Some((addr, index, coin_type)),
                        )
                    }
                }
            }
            PayoutRoute::ProviderManaged => {
                tracing::warn!(
                    "⚠️ Falling back to provider-managed payout for {}/{}; Trocador will pay the user's address directly",
                    request.to,
                    request.network_to
                );
                (
                    PayoutRoute::ProviderManaged,
                    recipient_address.clone(),
                    None,
                )
            }
        };
        let direct_settlement = payout_route == PayoutRoute::DirectSettlement;
        let trocador_address_memo = match payout_route {
            PayoutRoute::DirectSettlement => {
                Self::trocador_memo_value(None, destination_requires_extra_id)
            }
            PayoutRoute::ProviderManaged => Self::trocador_memo_value(
                recipient_extra_id.as_deref(),
                destination_requires_extra_id,
            ),
        };
        let trocador_refund_memo =
            Self::trocador_memo_value(refund_extra_id.as_deref(), source_requires_extra_id);

        let provider_spread = self
            .resolve_provider_spread_for_create(request, &trocador_gateway, &commission_service)
            .await;

        let fixed = matches!(request.rate_type, super::schema::RateType::Fixed);
        let trocador_webhook = Self::resolve_trocador_webhook_config();
        let swap_markup = swap_markup_from_env().map_err(SwapError::ExternalApiError)?;
        let trocador_res = self
            .call_trocador_with_retry(|| async {
                let swap_markup = swap_markup.clone();
                let res = trocador_gateway
                    .create_trade(
                        request.trade_id.as_deref(),
                        &request.from,
                        &request.network_from,
                        &request.to,
                        &request.network_to,
                        request.amount,
                        &trocador_payout_address,
                        trocador_address_memo.as_deref(),
                        refund_address.as_deref(),
                        trocador_refund_memo.as_deref(),
                        &request.provider,
                        fixed,
                        request.payment,
                        request.min_kycrating.as_deref(),
                        trocador_webhook.as_ref().map(|(url, _)| url.as_str()),
                        trocador_webhook.as_ref().map(|(_, key)| key.as_str()),
                        swap_markup.as_deref(),
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
        let gas_cost = if direct_settlement {
            self.gas_estimator
                .get_gas_cost_for_network(&normalized_payout_network)
                .await
        } else {
            0.0
        };
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
        let network_fee = if direct_settlement {
            gas_cost.max(0.0)
        } else {
            0.0
        };
        // Keep local fee deductions active for direct-settlement only.
        let platform_fee = if direct_settlement {
            commission.platform_fee
        } else {
            0.0
        };
        let estimated_user_receive = if direct_settlement {
            (commission.user_receive - network_fee).max(0.0)
        } else {
            trocador_res.amount_to.max(0.0)
        };

        let status = Self::map_created_trade_status(&trocador_res.status);
        let created_at = Utc::now();
        let expires_at = created_at + chrono::Duration::minutes(60);
        let normalized_provider_id = self
            .repository
            .ensure_provider_exists(&request.provider)
            .await?;

        let rate = estimated_user_receive / request.amount;
        self.repository
            .insert_swap(NewSwapRecord {
                id: &swap_id,
                user_id: user_id.as_deref(),
                client_id: client_id.as_deref(),
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
                refund_address: refund_address.as_deref(),
                refund_extra_id: refund_extra_id.as_deref(),
                platform_fee,
                total_fee: platform_fee + network_fee,
                status: status.clone(),
                rate_type: request.rate_type.clone(),
                is_sandbox: request.sandbox,
                is_payment: trocador_res.payment.unwrap_or(false),
                expires_at,
            })
            .await?;

        if let Some((internal_payout_address, address_index, coin_type)) = address_tracking {
            let wallet_crud = WalletCrud::new(self.pool.clone());
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
                .map_err(|e| {
                    SwapError::DatabaseError(format!("Failed to save address info: {}", e))
                })?;
        }

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
            expires_at,
            created_at,
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
            .currency_requires_extra_id(trocador_gateway, &request.to, &request.network_to)
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

    async fn currency_requires_extra_id(
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
                let markup = swap_markup_from_env().map_err(TrocadorError::ApiError)?;
                trocador_gateway
                    .fetch_rates(
                        &request.from,
                        &request.network_from,
                        &request.to,
                        &request.network_to,
                        request.amount,
                        request.min_kycrating.as_deref(),
                        markup.as_deref(),
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

    async fn resolve_payout_route(
        &self,
        ticker: &str,
        network: &str,
        recipient_extra_id: Option<&str>,
    ) -> Result<PayoutRoute, SwapError> {
        let direct_available = self.direct_settlement_available(ticker, network).await;
        Self::select_payout_route(direct_available, recipient_extra_id, ticker, network)
            .map_err(SwapError::ValidationError)
    }

    pub(crate) async fn direct_settlement_available(&self, ticker: &str, network: &str) -> bool {
        if !self.payout_policy.has_local_certified_chains() {
            tracing::info!(
                "No local certified payout chains configured; using provider-managed payout for {}/{}",
                ticker,
                network
            );
            return false;
        }

        if self.wallet_mnemonic.is_none() {
            tracing::warn!(
                "Wallet mnemonic not configured; falling back to provider-managed payout for {}/{}",
                ticker,
                network
            );
            return false;
        }

        let wallet_crud = WalletCrud::new(self.pool.clone());
        let lookup_network = canonical_payout_asset_network(network);
        let payout_asset_metadata = match wallet_crud
            .get_payout_asset_metadata(ticker, network, &lookup_network)
            .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(
                    "Failed to load payout asset metadata for {}/{}; using provider-managed fallback: {}",
                    ticker,
                    network,
                    error
                );
                return false;
            }
        };

        if let Err(reason) =
            ensure_local_payout_capability(ticker, network, payout_asset_metadata.as_ref())
        {
            tracing::warn!(
                "No exact local payout implementation for {}/{}; using provider-managed fallback: {}",
                ticker,
                network,
                reason
            );
            return false;
        }

        let chain_key =
            match resolve_configured_send_chain_key(self.rpc_manager.as_ref(), ticker, network) {
                Ok(chain_key) => chain_key,
                Err(error) => {
                    tracing::warn!(
                    "No direct payout chain mapping for {}/{}; using provider-managed fallback: {}",
                    ticker,
                    network,
                    error
                );
                    return false;
                }
            };

        let Some(family) = self.rpc_manager.chain_family(&chain_key) else {
            tracing::warn!(
                "Resolved payout chain '{}' for {}/{} has no family; using provider-managed fallback",
                chain_key,
                ticker,
                network
            );
            return false;
        };

        if !self.payout_policy.is_local_certified(&chain_key) {
            tracing::warn!(
                "Resolved payout chain '{}' for {}/{} is not in local_certified policy; using provider-managed fallback",
                chain_key,
                ticker,
                network
            );
            return false;
        }

        if !supports_direct_provider_chain(&chain_key, family) {
            tracing::warn!(
                "Resolved payout chain '{}' for {}/{} does not have a direct wallet provider for family '{}'; using provider-managed fallback",
                chain_key,
                ticker,
                network,
                family
            );
            return false;
        }

        match self.rpc_manager.select_endpoint(&chain_key).await {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(
                    "No active payout RPC endpoint available for '{}' ({}/{}); using provider-managed fallback: {}",
                    chain_key,
                    ticker,
                    network,
                    error
                );
                false
            }
        }
    }

    async fn preflight_local_signing_capability(
        &self,
        ticker: &str,
        network: &str,
        index: u32,
        derived_address: &str,
    ) -> Result<(), String> {
        let mnemonic = self
            .wallet_mnemonic
            .as_deref()
            .ok_or_else(|| "Wallet mnemonic not configured".to_string())?;
        let chain_key =
            resolve_configured_send_chain_key(self.rpc_manager.as_ref(), ticker, network)
                .map_err(|e| format!("Failed to resolve payout chain mapping: {}", e))?;
        let family = self
            .rpc_manager
            .chain_family(&chain_key)
            .ok_or_else(|| format!("Resolved payout chain '{}' has no family", chain_key))?;
        let probe_digest_hex = "00".repeat(32);

        if chain_key == "tron" {
            let private_key = crate::services::wallet::derivation::derive_tron_key(mnemonic, index)
                .await
                .map_err(|e| format!("Failed to derive Tron key: {}", e))?;
            SigningService::sign_tron_transaction_id(&private_key, &probe_digest_hex)
                .map_err(|e| format!("Failed to sign Tron preflight probe: {}", e))?;
            return Ok(());
        }

        if chain_key == "algorand" {
            let private_key =
                crate::services::wallet::derivation::derive_algorand_key(mnemonic, index)
                    .await
                    .map_err(|e| format!("Failed to derive Algorand key: {}", e))?;
            SigningService::sign_ed25519_transaction(&private_key, &probe_digest_hex)
                .map_err(|e| format!("Failed to sign Algorand preflight probe: {}", e))?;
            return Ok(());
        }

        match family {
            "evm" => {
                let private_key =
                    crate::services::wallet::derivation::derive_evm_key(mnemonic, index)
                        .await
                        .map_err(|e| format!("Failed to derive EVM key: {}", e))?;
                let sender_address =
                    crate::services::wallet::derivation::derive_evm_address(mnemonic, index)
                        .await
                        .map_err(|e| format!("Failed to derive EVM sender address: {}", e))?;

                if !sender_address.eq_ignore_ascii_case(derived_address) {
                    return Err(format!(
                        "Derived EVM sender address {} does not match internal payout address {}",
                        sender_address, derived_address
                    ));
                }

                SigningService::sign_evm_raw_transaction(
                    &private_key,
                    1,
                    0,
                    1,
                    21_000,
                    derived_address,
                    U256::ZERO,
                    &[],
                )
                .map_err(|e| format!("Failed to sign EVM preflight probe: {}", e))?;

                Ok(())
            }
            "solana" => {
                let private_key =
                    crate::services::wallet::derivation::derive_solana_key(mnemonic, index)
                        .await
                        .map_err(|e| format!("Failed to derive Solana key: {}", e))?;
                SigningService::sign_ed25519_transaction(&private_key, &probe_digest_hex)
                    .map_err(|e| format!("Failed to sign Solana preflight probe: {}", e))?;
                Ok(())
            }
            "btc" => {
                let private_key =
                    crate::services::wallet::derivation::derive_btc_key(mnemonic, index)
                        .await
                        .map_err(|e| format!("Failed to derive Bitcoin key: {}", e))?;
                SigningService::sign_btc_transaction(&private_key, &probe_digest_hex)
                    .map_err(|e| format!("Failed to sign Bitcoin preflight probe: {}", e))?;
                Ok(())
            }
            "utxo" => {
                let private_key = crate::services::wallet::derivation::derive_exact_key(
                    mnemonic, ticker, network, index,
                )
                .await
                .map_err(|e| format!("Failed to derive UTXO key: {}", e))?;
                SigningService::sign_utxo_transaction(&private_key, &probe_digest_hex)
                    .map_err(|e| format!("Failed to sign UTXO preflight probe: {}", e))?;
                Ok(())
            }
            _ => {
                let private_key = crate::services::wallet::derivation::derive_exact_key(
                    mnemonic, ticker, network, index,
                )
                .await
                .map_err(|e| format!("Failed to derive exact payout key: {}", e))?;
                SigningService::sign_cosmos_transaction(&private_key, &probe_digest_hex)
                    .map_err(|e| format!("Failed to sign chain-specific preflight probe: {}", e))?;
                Ok(())
            }
        }
    }

    fn select_payout_route(
        direct_available: bool,
        _recipient_extra_id: Option<&str>,
        _ticker: &str,
        _network: &str,
    ) -> Result<PayoutRoute, String> {
        if direct_available {
            return Ok(PayoutRoute::DirectSettlement);
        }

        Ok(PayoutRoute::ProviderManaged)
    }

    fn trocador_memo_value(extra_id: Option<&str>, requires_extra_id: bool) -> Option<String> {
        match extra_id {
            Some(extra_id) => Some(extra_id.to_string()),
            None if requires_extra_id => Some("0".to_string()),
            None => None,
        }
    }

    fn resolve_trocador_webhook_config() -> Option<(String, String)> {
        let base_url = std::env::var("RENDER_EXTERNAL_URL")
            .ok()
            .or_else(|| std::env::var("API_BASE_URL").ok());
        let webhook_key = std::env::var("TROCADOR_WEBHOOK_KEY").ok();

        match (base_url, webhook_key) {
            (Some(base_url), Some(webhook_key))
                if !base_url.trim().is_empty() && !webhook_key.trim().is_empty() =>
            {
                let webhook_url =
                    format!("{}/swap/webhooks/trocador", base_url.trim_end_matches('/'));
                Some((webhook_url, webhook_key))
            }
            (Some(_), None) | (None, Some(_)) => {
                tracing::warn!(
                    "Trocador webhook config is incomplete; API_BASE_URL/RENDER_EXTERNAL_URL and TROCADOR_WEBHOOK_KEY are both required. Falling back to polling only."
                );
                None
            }
            _ => None,
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
        let expires_at = Self::resolve_swap_expiry(swap.expires_at, swap.created_at);
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
            expires_at,
            completed_at: if status == SwapStatus::Completed {
                Some(Utc::now())
            } else {
                swap.completed_at
            },
        }
    }

    fn build_cached_status_response(swap: SwapStatusRecord) -> SwapStatusResponse {
        let expires_at = Self::resolve_swap_expiry(swap.expires_at, swap.created_at);
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
            expires_at,
            completed_at: swap.completed_at,
        }
    }

    fn resolve_swap_expiry(
        expires_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        expires_at.or(Some(created_at + chrono::Duration::minutes(60)))
    }

    pub async fn handle_trocador_trade_webhook(
        &self,
        trade: &super::schema::TrocadorTradeResponse,
    ) -> Result<(), SwapError> {
        let Some(swap) = self
            .repository
            .get_swap_status_record_by_provider_trade_id(&trade.trade_id)
            .await?
        else {
            tracing::warn!(
                "Received Trocador webhook for unknown trade_id {}; ignoring",
                trade.trade_id
            );
            return Ok(());
        };

        let wallet_crud = WalletCrud::new(self.pool.clone());
        let provider_managed_payout = match wallet_crud.get_address_info(&swap.id).await {
            Ok(Some(_)) => false,
            Ok(None) => {
                trade.address_user == swap.recipient_address
                    || trade
                        .address_user
                        .eq_ignore_ascii_case(&swap.recipient_address)
            }
            Err(error) => {
                return Err(SwapError::DatabaseError(format!(
                    "Failed to load payout address info for swap {}: {}",
                    swap.id, error
                )));
            }
        };

        let provider_status = Self::map_trocador_status(&trade.status);
        let new_status = if trade.status == "finished" && provider_managed_payout {
            SwapStatus::Completed
        } else {
            swap.status.reconcile_with_provider(provider_status)
        };

        let tx_hash_out = trade
            .details
            .as_ref()
            .and_then(|details| details.hashout.clone());
        let should_update = new_status != swap.status
            || (new_status == SwapStatus::Completed && swap.completed_at.is_none())
            || tx_hash_out.is_some();

        if should_update {
            self.update_swap_status(
                &swap.id,
                &new_status,
                trade.amount_to,
                None,
                tx_hash_out.clone(),
            )
            .await?;

            let message = if new_status == SwapStatus::Completed && provider_managed_payout {
                Some("Swap completed via Trocador webhook (provider-managed payout)".to_string())
            } else {
                Some(format!("Trocador webhook status update: {}", trade.status))
            };
            self.log_status_change(&swap.id, &new_status, message)
                .await?;
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::{PayoutRoute, SwapService};

    #[test]
    fn provider_managed_route_is_allowed_without_extra_id() {
        let route = SwapService::select_payout_route(false, None, "USDT", "ERC20").unwrap();
        assert_eq!(route, PayoutRoute::ProviderManaged);
    }

    #[test]
    fn provider_managed_route_accepts_extra_id_destinations() {
        let route = SwapService::select_payout_route(false, Some("12345"), "XRP", "Mainnet")
            .expect("extra-id destinations should be allowed on provider-managed payout");
        assert_eq!(route, PayoutRoute::ProviderManaged);
    }

    #[test]
    fn trocador_memo_defaults_to_zero_when_required() {
        assert_eq!(
            SwapService::trocador_memo_value(None, true).as_deref(),
            Some("0")
        );
    }

    #[test]
    fn trocador_memo_uses_explicit_extra_id_when_present() {
        assert_eq!(
            SwapService::trocador_memo_value(Some("123456"), true).as_deref(),
            Some("123456")
        );
    }
}
