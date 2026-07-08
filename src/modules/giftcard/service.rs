use crate::{
    modules::giftcard::{
        crud::{new_order_id, GiftCardCrud, GiftCardOrderRecord, NewGiftCardOrder},
        schema::{
            normalize_giftcard_currency_code, CardOrderDetailsResponse, CardOrderResponse,
            CreateGiftCardOrderRequest, CreatePrepaidCardOrderRequest,
        },
    },
    modules::swap::schema::TrocadorTradeResponse,
    services::trocador::{normalize_card_markup, TrocadorGateway},
};
use chrono::Utc;
use sha2::{Digest, Sha256};

const DUPLICATE_WINDOW_SECONDS: i64 = 10 * 60;
const RETRY_DELAY_SECONDS: i64 = 20;
const DEFAULT_MAX_RETRY_ATTEMPTS: i32 = 5;
const DEFAULT_REDACTION_RETENTION_DAYS: i64 = 30;
const CREATE_LOCK_TIMEOUT_SECONDS: i32 = 10;
const ORDER_LOCK_TIMEOUT_SECONDS: i32 = 2;

#[derive(Debug)]
pub enum GiftCardServiceError {
    Validation(String),
    NotFound,
    Forbidden,
    Config(String),
    Database(String),
    External(String),
}

impl std::fmt::Display for GiftCardServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(value)
            | Self::Config(value)
            | Self::Database(value)
            | Self::External(value) => f.write_str(value),
            Self::NotFound => f.write_str("Gift card order not found"),
            Self::Forbidden => f.write_str("Gift card order not found"),
        }
    }
}

impl std::error::Error for GiftCardServiceError {}

enum EffectiveWebhook {
    Managed { url: String, key: String },
    Passthrough { url: String, key: String },
    None,
}

impl EffectiveWebhook {
    fn mode(&self) -> &'static str {
        match self {
            Self::Managed { .. } => "managed",
            Self::Passthrough { .. } => "passthrough",
            Self::None => "none",
        }
    }

    fn url(&self) -> Option<&str> {
        match self {
            Self::Managed { url, .. } | Self::Passthrough { url, .. } => Some(url.as_str()),
            Self::None => None,
        }
    }

    fn key(&self) -> Option<&str> {
        match self {
            Self::Managed { key, .. } | Self::Passthrough { key, .. } => Some(key.as_str()),
            Self::None => None,
        }
    }

    fn retryable(&self) -> bool {
        !matches!(self, Self::Passthrough { .. })
    }
}

enum CreateOrderOutcome {
    Existing(u16, CardOrderResponse),
    Created {
        order_id: String,
        webhook: EffectiveWebhook,
        response: CardOrderResponse,
    },
}

pub struct GiftCardService {
    crud: GiftCardCrud,
}

impl GiftCardService {
    pub fn new(state: std::sync::Arc<crate::AppState>) -> Self {
        let crud = GiftCardCrud::new(state.db.clone());
        Self { crud }
    }

    pub async fn create_giftcard_order(
        &self,
        user_id: Option<&str>,
        client_id: &str,
        req: &CreateGiftCardOrderRequest,
    ) -> Result<(u16, CardOrderResponse), GiftCardServiceError> {
        let card_markup = normalize_card_markup(req.card_markup.as_deref())
            .map_err(GiftCardServiceError::Validation)?;
        let currency_code = normalize_giftcard_currency_code(req.currency_code.as_deref());
        let owner_key = owner_key(user_id, client_id);
        let request_hash = giftcard_request_hash(&owner_key, req, card_markup.as_deref());
        let lock_key = format!("giftcard:create:{}:{}", owner_key, request_hash);
        let lock = self
            .crud
            .acquire_named_lock(&lock_key, CREATE_LOCK_TIMEOUT_SECONDS)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

        let outcome = async {
            if let Some(existing) = self
                .crud
                .find_recent_duplicate(&owner_key, &request_hash, DUPLICATE_WINDOW_SECONDS)
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
            {
                return Ok(CreateOrderOutcome::Existing(
                    200,
                    self.to_response(existing),
                ));
            }

            let webhook =
                resolve_effective_webhook(req.webhook.as_deref(), req.webhook_key.as_deref())?;
            let order_id = new_order_id();
            self.crud
                .insert_order(NewGiftCardOrder {
                    id: &order_id,
                    user_id,
                    client_id: Some(client_id),
                    owner_key: &owner_key,
                    request_hash: &request_hash,
                    order_kind: "giftcard",
                    product_id: Some(&req.product_id),
                    prepaid_provider: None,
                    currency_code: currency_code.as_deref(),
                    source_ticker: &req.ticker_from,
                    source_network: &req.network_from,
                    amount: req.amount,
                    recipient_email: &req.email,
                    card_markup: card_markup.as_deref(),
                    webhook_mode: webhook.mode(),
                    webhook_url: webhook.url(),
                    status: "queued",
                    next_retry_at: if webhook.retryable() {
                        Some(Utc::now())
                    } else {
                        None
                    },
                })
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

            let record = self
                .crud
                .get_order_by_id(&order_id)
                .await
                .map_err(|db_error| GiftCardServiceError::Database(db_error.to_string()))?
                .ok_or(GiftCardServiceError::NotFound)?;

            Ok(CreateOrderOutcome::Created {
                order_id,
                webhook,
                response: self.to_response(record),
            })
        }
        .await;

        let _ = lock.release().await;

        match outcome? {
            CreateOrderOutcome::Existing(status, response) => Ok((status, response)),
            CreateOrderOutcome::Created {
                order_id: _,
                webhook,
                response,
            } if webhook.retryable() => Ok((202, response)),
            CreateOrderOutcome::Created {
                order_id, webhook, ..
            } => self
                .process_order(order_id.as_str(), Some(&webhook))
                .await
                .map(|record| (201, self.to_response(record))),
        }
    }

    pub async fn create_prepaid_order(
        &self,
        user_id: Option<&str>,
        client_id: &str,
        req: &CreatePrepaidCardOrderRequest,
    ) -> Result<(u16, CardOrderResponse), GiftCardServiceError> {
        let card_markup = normalize_card_markup(req.card_markup.as_deref())
            .map_err(GiftCardServiceError::Validation)?;
        let owner_key = owner_key(user_id, client_id);
        let request_hash = prepaid_request_hash(&owner_key, req, card_markup.as_deref());
        let lock_key = format!("giftcard:create:{}:{}", owner_key, request_hash);
        let lock = self
            .crud
            .acquire_named_lock(&lock_key, CREATE_LOCK_TIMEOUT_SECONDS)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

        let outcome = async {
            if let Some(existing) = self
                .crud
                .find_recent_duplicate(&owner_key, &request_hash, DUPLICATE_WINDOW_SECONDS)
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
            {
                return Ok(CreateOrderOutcome::Existing(
                    200,
                    self.to_response(existing),
                ));
            }

            let webhook =
                resolve_effective_webhook(req.webhook.as_deref(), req.webhook_key.as_deref())?;
            let order_id = new_order_id();
            self.crud
                .insert_order(NewGiftCardOrder {
                    id: &order_id,
                    user_id,
                    client_id: Some(client_id),
                    owner_key: &owner_key,
                    request_hash: &request_hash,
                    order_kind: "prepaid",
                    product_id: None,
                    prepaid_provider: Some(&req.provider),
                    currency_code: Some(&req.currency_code),
                    source_ticker: &req.ticker_from,
                    source_network: &req.network_from,
                    amount: req.amount,
                    recipient_email: &req.email,
                    card_markup: card_markup.as_deref(),
                    webhook_mode: webhook.mode(),
                    webhook_url: webhook.url(),
                    status: "queued",
                    next_retry_at: if webhook.retryable() {
                        Some(Utc::now())
                    } else {
                        None
                    },
                })
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

            let record = self
                .crud
                .get_order_by_id(&order_id)
                .await
                .map_err(|db_error| GiftCardServiceError::Database(db_error.to_string()))?
                .ok_or(GiftCardServiceError::NotFound)?;

            Ok(CreateOrderOutcome::Created {
                order_id,
                webhook,
                response: self.to_response(record),
            })
        }
        .await;

        let _ = lock.release().await;

        match outcome? {
            CreateOrderOutcome::Existing(status, response) => Ok((status, response)),
            CreateOrderOutcome::Created {
                order_id: _,
                webhook,
                response,
            } if webhook.retryable() => Ok((202, response)),
            CreateOrderOutcome::Created {
                order_id, webhook, ..
            } => self
                .process_order(order_id.as_str(), Some(&webhook))
                .await
                .map(|record| (201, self.to_response(record))),
        }
    }

    pub async fn get_order_for_requester(
        &self,
        order_ref: &str,
        user_id: Option<&str>,
        client_id: &str,
    ) -> Result<CardOrderResponse, GiftCardServiceError> {
        let mut record = self
            .crud
            .get_order_by_reference(order_ref)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
            .ok_or(GiftCardServiceError::NotFound)?;

        let allowed = authorized_owner_keys(user_id, client_id);
        if !allowed
            .iter()
            .any(|candidate| candidate == &record.owner_key)
        {
            return Err(GiftCardServiceError::Forbidden);
        }

        if let Some(trade_id) = record.upstream_trade_id.clone() {
            if should_refresh_status(&record) {
                if let Ok(updated) = self.refresh_from_trade_status(&record.id, &trade_id).await {
                    record = updated;
                }
            }
        }

        Ok(self.to_response(record))
    }

    pub async fn handle_trocador_webhook(
        &self,
        trade: &TrocadorTradeResponse,
    ) -> Result<(), GiftCardServiceError> {
        let Some(record) = self
            .crud
            .get_order_by_reference(&trade.trade_id)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
        else {
            return Ok(());
        };

        let local_status = map_provider_status(&trade.status);
        self.crud
            .persist_trade(&record.id, trade, &local_status)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))
    }

    pub async fn run_retry_batch(&self, limit: usize) -> Result<usize, GiftCardServiceError> {
        let max_retry_attempts = resolve_max_retry_attempts();
        self.crud
            .mark_exhausted_pending_failed(max_retry_attempts)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

        let pending = self
            .crud
            .claim_pending_orders(limit, max_retry_attempts)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;
        let processed = pending.len();

        for order in pending {
            let _ = self.process_order(order.id.as_str(), None).await;
        }

        Ok(processed)
    }

    pub async fn run_retention_cleanup(&self) -> Result<u64, GiftCardServiceError> {
        self.crud
            .redact_terminal_orders(resolve_redaction_retention_days())
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))
    }

    pub async fn reconcile_active_batch(
        &self,
        limit: usize,
        stale_after_seconds: i64,
    ) -> Result<usize, GiftCardServiceError> {
        let active = self
            .crud
            .list_active_orders_for_refresh(limit, stale_after_seconds)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;
        let processed = active.len();

        for order in active {
            let Some(trade_id) = order.upstream_trade_id.clone() else {
                continue;
            };

            let _ = self.refresh_from_trade_status(&order.id, &trade_id).await;
        }

        Ok(processed)
    }

    async fn process_order(
        &self,
        order_id: &str,
        request_webhook: Option<&EffectiveWebhook>,
    ) -> Result<GiftCardOrderRecord, GiftCardServiceError> {
        let lock_key = format!("giftcard:order:{}", order_id);
        let lock = self
            .crud
            .acquire_named_lock(&lock_key, ORDER_LOCK_TIMEOUT_SECONDS)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

        let outcome = async {
            let record = self
                .crud
                .get_order_by_id(order_id)
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
                .ok_or(GiftCardServiceError::NotFound)?;

            if !matches!(record.status.as_str(), "queued" | "retry_pending") {
                return Ok(record);
            }

            if !self
                .crud
                .mark_order_creating(order_id)
                .await
                .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
            {
                return self
                    .crud
                    .get_order_by_id(order_id)
                    .await
                    .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
                    .ok_or(GiftCardServiceError::NotFound);
            }

            let execution = (|| async {
                let gateway = TrocadorGateway::from_env().map_err(|_| {
                    GiftCardServiceError::Config("TROCADOR_API_KEY not set".to_string())
                })?;
                let owned_webhook;
                let effective_webhook = match request_webhook {
                    Some(value) => value,
                    None => {
                        owned_webhook = resolve_stored_webhook(&record)?;
                        &owned_webhook
                    }
                };

                if record.order_kind == "giftcard" {
                    let created_trade = gateway
                        .order_giftcard(
                            record.product_id.as_deref().ok_or_else(|| {
                                GiftCardServiceError::Validation(
                                    "Stored gift card order is missing product_id".to_string(),
                                )
                            })?,
                            &record.source_ticker,
                            &record.source_network,
                            record.amount,
                            &record.recipient_email,
                            effective_webhook.url(),
                            effective_webhook.key(),
                            record.card_markup.as_deref(),
                        )
                        .await
                        .map_err(|error| GiftCardServiceError::External(error.to_string()))?;

                    hydrate_created_trade(&gateway, created_trade).await
                } else {
                    let created_trade = gateway
                        .order_prepaid_card(
                            record.prepaid_provider.as_deref().ok_or_else(|| {
                                GiftCardServiceError::Validation(
                                    "Stored prepaid order is missing provider".to_string(),
                                )
                            })?,
                            record.currency_code.as_deref().ok_or_else(|| {
                                GiftCardServiceError::Validation(
                                    "Stored prepaid order is missing currency_code".to_string(),
                                )
                            })?,
                            &record.source_ticker,
                            &record.source_network,
                            record.amount,
                            &record.recipient_email,
                            effective_webhook.url(),
                            effective_webhook.key(),
                            record.card_markup.as_deref(),
                        )
                        .await
                        .map_err(|error| GiftCardServiceError::External(error.to_string()))?;

                    hydrate_created_trade(&gateway, created_trade).await
                }
            })()
            .await;

            match execution {
                Ok(trade) => {
                    let local_status = map_provider_status(&trade.status);
                    self.crud
                        .persist_trade(order_id, &trade, &local_status)
                        .await
                        .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

                    self.crud
                        .get_order_by_id(order_id)
                        .await
                        .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
                        .ok_or(GiftCardServiceError::NotFound)
                }
                Err(error) => {
                    let error_message = error.to_string();
                    let attempt_number = record.attempt_count + 1;
                    let base_retryable = request_webhook
                        .map(|value| value.retryable())
                        .unwrap_or_else(|| record.webhook_mode != "passthrough");
                    let can_retry = base_retryable
                        && attempt_number < resolve_max_retry_attempts()
                        && !matches!(
                            error,
                            GiftCardServiceError::Validation(_)
                                | GiftCardServiceError::NotFound
                                | GiftCardServiceError::Forbidden
                        );

                    if can_retry {
                        self.crud
                            .mark_retry_pending(order_id, &error_message, RETRY_DELAY_SECONDS)
                            .await
                            .map_err(|db_error| {
                                GiftCardServiceError::Database(db_error.to_string())
                            })?;
                    } else {
                        self.crud
                            .mark_failed(order_id, &error_message)
                            .await
                            .map_err(|db_error| {
                                GiftCardServiceError::Database(db_error.to_string())
                            })?;
                    }

                    Err(error)
                }
            }
        }
        .await;

        let _ = lock.release().await;
        outcome
    }

    async fn refresh_from_trade_status(
        &self,
        order_id: &str,
        trade_id: &str,
    ) -> Result<GiftCardOrderRecord, GiftCardServiceError> {
        let gateway = TrocadorGateway::from_env()
            .map_err(|_| GiftCardServiceError::Config("TROCADOR_API_KEY not set".to_string()))?;
        let trade = gateway
            .fetch_trade_status(trade_id)
            .await
            .map_err(|error| GiftCardServiceError::External(error.to_string()))?;
        let local_status = map_provider_status(&trade.status);

        self.crud
            .persist_trade(order_id, &trade, &local_status)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?;

        self.crud
            .get_order_by_id(order_id)
            .await
            .map_err(|error| GiftCardServiceError::Database(error.to_string()))?
            .ok_or(GiftCardServiceError::NotFound)
    }

    fn to_response(&self, record: GiftCardOrderRecord) -> CardOrderResponse {
        let queued = matches!(
            record.status.as_str(),
            "queued" | "creating" | "retry_pending"
        );
        let retryable = !matches!(
            record.status.as_str(),
            "completed" | "failed" | "refunded" | "expired"
        );

        CardOrderResponse {
            order_id: record.id,
            trade_id: record.upstream_trade_id,
            order_kind: record.order_kind,
            product_id: record.product_id,
            prepaid_provider: record.prepaid_provider,
            currency_code: record.currency_code,
            provider: record.provider,
            provider_trade_id: record.provider_trade_id,
            provider_password: record.provider_password,
            recipient_email: record.recipient_email,
            status: record.status,
            ticker_from: record.source_ticker,
            network_from: record.source_network,
            ticker_to: record.target_ticker,
            network_to: record.target_network,
            coin_from: record.source_coin_name,
            coin_to: record.target_coin_name,
            amount_from: record.amount,
            amount_to: record.amount_to,
            fixed: record.fixed,
            payment: record.payment,
            deposit_address: record.deposit_address,
            deposit_extra_id: record.deposit_extra_id,
            settlement_address: record.settlement_address,
            settlement_extra_id: record.settlement_extra_id,
            refund_address: record.refund_address,
            refund_extra_id: record.refund_extra_id,
            queued,
            retryable,
            last_error: record.last_error,
            created_at: Some(record.created_at.to_rfc3339()),
            updated_at: Some(record.updated_at.to_rfc3339()),
            completed_at: record.completed_at.map(|value| value.to_rfc3339()),
            details: record.details.map(CardOrderDetailsResponse::from),
        }
    }
}

fn owner_key(user_id: Option<&str>, client_id: &str) -> String {
    match user_id {
        Some(user_id) if !user_id.trim().is_empty() => format!("user:{}", user_id.trim()),
        _ => format!("client:{}", client_id.trim()),
    }
}

fn authorized_owner_keys(user_id: Option<&str>, client_id: &str) -> Vec<String> {
    let mut keys = vec![format!("client:{}", client_id.trim())];
    if let Some(user_id) = user_id {
        if !user_id.trim().is_empty() {
            keys.push(format!("user:{}", user_id.trim()));
        }
    }
    keys
}

fn giftcard_request_hash(
    owner_key: &str,
    req: &CreateGiftCardOrderRequest,
    card_markup: Option<&str>,
) -> String {
    let canonical = format!(
        "owner={}|kind=giftcard|product_id={}|currency_code={}|ticker_from={}|network_from={}|amount={:.12}|email={}|markup={}",
        owner_key,
        req.product_id.trim().to_ascii_lowercase(),
        normalize_giftcard_currency_code(req.currency_code.as_deref())
            .unwrap_or_default()
            .to_ascii_lowercase(),
        req.ticker_from.trim().to_ascii_lowercase(),
        req.network_from.trim().to_ascii_lowercase(),
        req.amount,
        req.email.trim().to_ascii_lowercase(),
        card_markup.unwrap_or("")
    );
    sha256_hex(&canonical)
}

fn prepaid_request_hash(
    owner_key: &str,
    req: &CreatePrepaidCardOrderRequest,
    card_markup: Option<&str>,
) -> String {
    let canonical = format!(
        "owner={}|kind=prepaid|provider={}|currency_code={}|ticker_from={}|network_from={}|amount={:.12}|email={}|markup={}",
        owner_key,
        req.provider.trim().to_ascii_lowercase(),
        req.currency_code.trim().to_ascii_lowercase(),
        req.ticker_from.trim().to_ascii_lowercase(),
        req.network_from.trim().to_ascii_lowercase(),
        req.amount,
        req.email.trim().to_ascii_lowercase(),
        card_markup.unwrap_or("")
    );
    sha256_hex(&canonical)
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn resolve_effective_webhook(
    request_webhook: Option<&str>,
    request_webhook_key: Option<&str>,
) -> Result<EffectiveWebhook, GiftCardServiceError> {
    if let Some((url, key)) = resolve_managed_webhook_config() {
        return Ok(EffectiveWebhook::Managed { url, key });
    }

    match (request_webhook, request_webhook_key) {
        (Some(url), Some(key)) if !url.trim().is_empty() && !key.trim().is_empty() => {
            Ok(EffectiveWebhook::Passthrough {
                url: url.trim().to_string(),
                key: key.trim().to_string(),
            })
        }
        (Some(_), None) | (None, Some(_)) => Err(GiftCardServiceError::Validation(
            "Both webhook and webhook_key are required when using a custom card webhook"
                .to_string(),
        )),
        _ => Ok(EffectiveWebhook::None),
    }
}

fn resolve_stored_webhook(
    order: &GiftCardOrderRecord,
) -> Result<EffectiveWebhook, GiftCardServiceError> {
    match order.webhook_mode.as_str() {
        "managed" => resolve_managed_webhook_config()
            .map(|(url, key)| EffectiveWebhook::Managed { url, key })
            .ok_or_else(|| {
                GiftCardServiceError::Config(
                    "Managed Trocador webhook config is incomplete".to_string(),
                )
            }),
        "none" => Ok(EffectiveWebhook::None),
        "passthrough" => Err(GiftCardServiceError::Config(
            "Stored passthrough gift card orders cannot be retried without the caller-supplied webhook key".to_string(),
        )),
        _ => Err(GiftCardServiceError::Config(
            "Unknown gift card webhook mode".to_string(),
        )),
    }
}

fn resolve_managed_webhook_config() -> Option<(String, String)> {
    let base_url = std::env::var("PUBLIC_BACKEND_URL")
        .ok()
        .or_else(|| std::env::var("RENDER_EXTERNAL_URL").ok())
        .or_else(|| std::env::var("API_BASE_URL").ok());
    let webhook_key = std::env::var("TROCADOR_WEBHOOK_KEY").ok();

    match (base_url, webhook_key) {
        (Some(base_url), Some(webhook_key))
            if !base_url.trim().is_empty() && !webhook_key.trim().is_empty() =>
        {
            Some((
                format!(
                    "{}/giftcards/webhooks/trocador",
                    base_url.trim_end_matches('/')
                ),
                webhook_key,
            ))
        }
        _ => None,
    }
}

fn map_provider_status(provider_status: &str) -> String {
    provider_status.trim().to_ascii_lowercase()
}

fn should_refresh_status(record: &GiftCardOrderRecord) -> bool {
    if matches!(
        record.status.as_str(),
        "queued" | "creating" | "retry_pending" | "completed" | "failed" | "refunded" | "expired"
    ) {
        return false;
    }

    match record.last_synced_at {
        Some(last_synced_at) => (Utc::now() - last_synced_at).num_seconds() >= 10,
        None => true,
    }
}

async fn hydrate_created_trade(
    gateway: &TrocadorGateway,
    created_trade: TrocadorTradeResponse,
) -> Result<TrocadorTradeResponse, GiftCardServiceError> {
    let trade_id = created_trade.trade_id.clone();
    let mut last_error = None;

    for attempt in 0..3 {
        match gateway.fetch_trade_status(&trade_id).await {
            Ok(hydrated_trade) => return Ok(hydrated_trade),
            Err(error) => {
                last_error = Some(error.to_string());
                if attempt < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    tracing::warn!(
        "Failed to hydrate created gift card trade {}; using create response: {}",
        trade_id,
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
    Ok(created_trade)
}

fn resolve_max_retry_attempts() -> i32 {
    std::env::var("GIFTCARD_MAX_RETRY_ATTEMPTS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_RETRY_ATTEMPTS)
}

fn resolve_redaction_retention_days() -> i64 {
    std::env::var("GIFTCARD_REDACTION_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REDACTION_RETENTION_DAYS)
}

#[cfg(test)]
mod tests {
    use super::{
        authorized_owner_keys, giftcard_request_hash, owner_key, prepaid_request_hash,
        resolve_max_retry_attempts, resolve_redaction_retention_days,
    };
    use crate::modules::giftcard::schema::{
        CreateGiftCardOrderRequest, CreatePrepaidCardOrderRequest,
    };

    #[test]
    fn owner_key_prefers_authenticated_user() {
        assert_eq!(owner_key(Some("user-1"), "client-1"), "user:user-1");
        assert_eq!(owner_key(None, "client-1"), "client:client-1");
    }

    #[test]
    fn authorized_owner_keys_include_client_and_user() {
        let keys = authorized_owner_keys(Some("user-1"), "client-1");
        assert!(keys.contains(&"user:user-1".to_string()));
        assert!(keys.contains(&"client:client-1".to_string()));
    }

    #[test]
    fn request_hashes_are_deterministic() {
        let gift = CreateGiftCardOrderRequest {
            product_id: "abc".to_string(),
            ticker_from: "btc".to_string(),
            network_from: "Mainnet".to_string(),
            amount: 100.0,
            email: "me@example.com".to_string(),
            currency_code: Some("USD".to_string()),
            webhook: None,
            webhook_key: None,
            card_markup: Some("1".to_string()),
        };
        let prepaid = CreatePrepaidCardOrderRequest {
            provider: "visa".to_string(),
            currency_code: "USD".to_string(),
            ticker_from: "btc".to_string(),
            network_from: "Mainnet".to_string(),
            amount: 100.0,
            email: "me@example.com".to_string(),
            webhook: None,
            webhook_key: None,
            card_markup: Some("1".to_string()),
        };

        let owner = "client:abc";
        assert_eq!(
            giftcard_request_hash(owner, &gift, Some("1")),
            giftcard_request_hash(owner, &gift, Some("1"))
        );
        assert_eq!(
            prepaid_request_hash(owner, &prepaid, Some("1")),
            prepaid_request_hash(owner, &prepaid, Some("1"))
        );
    }

    #[test]
    fn retry_attempts_env_falls_back_for_invalid_values() {
        std::env::set_var("GIFTCARD_MAX_RETRY_ATTEMPTS", "0");
        assert_eq!(resolve_max_retry_attempts(), 5);

        std::env::set_var("GIFTCARD_MAX_RETRY_ATTEMPTS", "7");
        assert_eq!(resolve_max_retry_attempts(), 7);

        std::env::remove_var("GIFTCARD_MAX_RETRY_ATTEMPTS");
    }

    #[test]
    fn redaction_retention_env_falls_back_for_invalid_values() {
        std::env::set_var("GIFTCARD_REDACTION_RETENTION_DAYS", "-1");
        assert_eq!(resolve_redaction_retention_days(), 30);

        std::env::set_var("GIFTCARD_REDACTION_RETENTION_DAYS", "14");
        assert_eq!(resolve_redaction_retention_days(), 14);

        std::env::remove_var("GIFTCARD_REDACTION_RETENTION_DAYS");
    }
}
