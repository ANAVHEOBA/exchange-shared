use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{pool::PoolConnection, Error, MySql, QueryBuilder, Row};
use uuid::Uuid;

use crate::{
    config::DbPool,
    modules::giftcard::schema::AdminGiftCardOrderQuery,
    modules::swap::schema::{TrocadorTradeDetails, TrocadorTradeResponse},
};

#[derive(Debug, Clone)]
pub struct GiftCardOrderRecord {
    pub id: String,
    pub user_id: Option<String>,
    pub client_id: Option<String>,
    pub owner_key: String,
    pub request_hash: String,
    pub order_kind: String,
    pub product_id: Option<String>,
    pub prepaid_provider: Option<String>,
    pub currency_code: Option<String>,
    pub source_ticker: String,
    pub source_network: String,
    pub amount: f64,
    pub recipient_email: String,
    pub card_markup: Option<String>,
    pub webhook_mode: String,
    pub webhook_url: Option<String>,
    pub upstream_trade_id: Option<String>,
    pub provider: Option<String>,
    pub provider_trade_id: Option<String>,
    pub provider_password: Option<String>,
    pub target_ticker: Option<String>,
    pub target_network: Option<String>,
    pub source_coin_name: Option<String>,
    pub target_coin_name: Option<String>,
    pub amount_to: Option<f64>,
    pub fixed: Option<bool>,
    pub payment: Option<bool>,
    pub deposit_address: Option<String>,
    pub deposit_extra_id: Option<String>,
    pub settlement_address: Option<String>,
    pub settlement_extra_id: Option<String>,
    pub refund_address: Option<String>,
    pub refund_extra_id: Option<String>,
    pub provider_status: Option<String>,
    pub details: Option<TrocadorTradeDetails>,
    pub status: String,
    pub last_error: Option<String>,
    pub attempt_count: i32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub last_synced_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewGiftCardOrder<'a> {
    pub id: &'a str,
    pub user_id: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub owner_key: &'a str,
    pub request_hash: &'a str,
    pub order_kind: &'a str,
    pub product_id: Option<&'a str>,
    pub prepaid_provider: Option<&'a str>,
    pub currency_code: Option<&'a str>,
    pub source_ticker: &'a str,
    pub source_network: &'a str,
    pub amount: f64,
    pub recipient_email: &'a str,
    pub card_markup: Option<&'a str>,
    pub webhook_mode: &'a str,
    pub webhook_url: Option<&'a str>,
    pub status: &'a str,
    pub next_retry_at: Option<DateTime<Utc>>,
}

pub struct GiftCardOrderLock {
    key: String,
    connection: Option<PoolConnection<MySql>>,
}

impl GiftCardOrderLock {
    pub async fn release(mut self) -> Result<(), Error> {
        if let Some(mut connection) = self.connection.take() {
            let _ = sqlx::query_scalar::<_, Option<i64>>("SELECT RELEASE_LOCK(?)")
                .bind(&self.key)
                .fetch_one(&mut *connection)
                .await?;
        }

        Ok(())
    }
}

pub struct GiftCardCrud {
    pool: DbPool,
}

impl GiftCardCrud {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn acquire_named_lock(
        &self,
        key: &str,
        timeout_seconds: i32,
    ) -> Result<GiftCardOrderLock, Error> {
        let normalized_key = normalize_named_lock_key(key);
        let mut connection = self.pool.acquire().await?;
        let acquired = sqlx::query_scalar::<_, Option<i64>>("SELECT GET_LOCK(?, ?)")
            .bind(&normalized_key)
            .bind(timeout_seconds)
            .fetch_one(&mut *connection)
            .await?;

        match acquired {
            Some(1) => Ok(GiftCardOrderLock {
                key: normalized_key,
                connection: Some(connection),
            }),
            _ => Err(Error::Protocol(
                format!("failed to acquire gift card lock for {}", key).into(),
            )),
        }
    }

    pub async fn insert_order(&self, order: NewGiftCardOrder<'_>) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO giftcard_orders (
                id, user_id, client_id, owner_key, request_hash, order_kind,
                product_id, prepaid_provider, currency_code,
                source_ticker, source_network, amount, recipient_email,
                card_markup, webhook_mode, webhook_url, status, next_retry_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(order.id)
        .bind(order.user_id)
        .bind(order.client_id)
        .bind(order.owner_key)
        .bind(order.request_hash)
        .bind(order.order_kind)
        .bind(order.product_id)
        .bind(order.prepaid_provider)
        .bind(order.currency_code)
        .bind(order.source_ticker)
        .bind(order.source_network)
        .bind(order.amount)
        .bind(order.recipient_email)
        .bind(order.card_markup)
        .bind(order.webhook_mode)
        .bind(order.webhook_url)
        .bind(order.status)
        .bind(order.next_retry_at.map(|value| value.naive_utc()))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_recent_duplicate(
        &self,
        owner_key: &str,
        request_hash: &str,
        within_seconds: i64,
    ) -> Result<Option<GiftCardOrderRecord>, Error> {
        let since = Utc::now() - ChronoDuration::seconds(within_seconds);
        let row = sqlx::query(
            r#"
            SELECT
                id,
                user_id,
                client_id,
                owner_key,
                request_hash,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                recipient_email,
                card_markup,
                webhook_mode,
                webhook_url,
                upstream_trade_id,
                provider,
                provider_trade_id,
                provider_password,
                target_ticker,
                target_network,
                source_coin_name,
                target_coin_name,
                CAST(amount_to AS DOUBLE) AS amount_to,
                fixed,
                payment,
                deposit_address,
                deposit_extra_id,
                settlement_address,
                settlement_extra_id,
                refund_address,
                refund_extra_id,
                provider_status,
                details_json,
                status,
                last_error,
                attempt_count,
                next_retry_at,
                last_synced_at,
                completed_at,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE owner_key = ?
              AND request_hash = ?
              AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(owner_key)
        .bind(request_hash)
        .bind(since.naive_utc())
        .fetch_optional(&self.pool)
        .await?;

        row.map(Self::row_to_order).transpose()
    }

    pub async fn get_order_by_id(
        &self,
        order_id: &str,
    ) -> Result<Option<GiftCardOrderRecord>, Error> {
        self.get_order_by_ref("id", order_id).await
    }

    pub async fn get_order_by_reference(
        &self,
        order_ref: &str,
    ) -> Result<Option<GiftCardOrderRecord>, Error> {
        if let Some(record) = self.get_order_by_ref("id", order_ref).await? {
            return Ok(Some(record));
        }

        self.get_order_by_ref("upstream_trade_id", order_ref).await
    }

    pub async fn admin_list_orders(
        &self,
        query: &AdminGiftCardOrderQuery,
    ) -> Result<Vec<GiftCardOrderRecord>, Error> {
        let mut builder = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                id,
                user_id,
                client_id,
                owner_key,
                request_hash,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                recipient_email,
                card_markup,
                webhook_mode,
                webhook_url,
                upstream_trade_id,
                provider,
                provider_trade_id,
                provider_password,
                target_ticker,
                target_network,
                source_coin_name,
                target_coin_name,
                CAST(amount_to AS DOUBLE) AS amount_to,
                fixed,
                payment,
                deposit_address,
                deposit_extra_id,
                settlement_address,
                settlement_extra_id,
                refund_address,
                refund_extra_id,
                provider_status,
                details_json,
                status,
                last_error,
                attempt_count,
                next_retry_at,
                last_synced_at,
                completed_at,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE 1 = 1
            "#,
        );

        if let Some(status) = query
            .status
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder.push(" AND status = ").push_bind(status);
        }
        if let Some(email) = query
            .email
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder.push(" AND recipient_email = ").push_bind(email);
        }
        if let Some(trade_id) = query
            .trade_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder
                .push(" AND (id = ")
                .push_bind(trade_id)
                .push(" OR upstream_trade_id = ")
                .push_bind(trade_id)
                .push(" OR provider_trade_id = ")
                .push_bind(trade_id)
                .push(")");
        }
        if let Some(client_id) = query
            .client_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder.push(" AND client_id = ").push_bind(client_id);
        }
        if let Some(provider) = query
            .provider
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder.push(" AND provider = ").push_bind(provider);
        }
        if let Some(product_id) = query
            .product_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            builder.push(" AND product_id = ").push_bind(product_id);
        }

        let limit = query.limit.unwrap_or(50).clamp(1, 200) as i64;
        builder
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(limit);

        let rows = builder.build().fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(Self::row_to_order)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn admin_mark_retry_now(&self, order_id: &str) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET status = 'retry_pending',
                next_retry_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND status IN ('queued', 'retry_pending', 'failed', 'creating')
            "#,
        )
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn audit_reveal(
        &self,
        order_id: &str,
        field_group: &str,
        reason: &str,
        admin_id: &str,
        admin_email: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO ops_reveal_events (
                entity_type, entity_id, field_group, reason, admin_id, admin_email
            )
            VALUES ('giftcard_order', ?, ?, ?, ?, ?)
            "#,
        )
        .bind(order_id)
        .bind(field_group)
        .bind(reason)
        .bind(admin_id)
        .bind(admin_email)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_order_by_ref(
        &self,
        field: &str,
        value: &str,
    ) -> Result<Option<GiftCardOrderRecord>, Error> {
        let sql = format!(
            r#"
            SELECT
                id,
                user_id,
                client_id,
                owner_key,
                request_hash,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                recipient_email,
                card_markup,
                webhook_mode,
                webhook_url,
                upstream_trade_id,
                provider,
                provider_trade_id,
                provider_password,
                target_ticker,
                target_network,
                source_coin_name,
                target_coin_name,
                CAST(amount_to AS DOUBLE) AS amount_to,
                fixed,
                payment,
                deposit_address,
                deposit_extra_id,
                settlement_address,
                settlement_extra_id,
                refund_address,
                refund_extra_id,
                provider_status,
                details_json,
                status,
                last_error,
                attempt_count,
                next_retry_at,
                last_synced_at,
                completed_at,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE {} = ?
            LIMIT 1
            "#,
            field
        );

        let row = sqlx::query(&sql)
            .bind(value)
            .fetch_optional(&self.pool)
            .await?;

        row.map(Self::row_to_order).transpose()
    }

    pub async fn mark_order_creating(&self, order_id: &str) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET status = 'creating',
                attempt_count = attempt_count + 1,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND status IN ('queued', 'retry_pending')
            "#,
        )
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn persist_trade(
        &self,
        order_id: &str,
        trade: &TrocadorTradeResponse,
        local_status: &str,
    ) -> Result<(), Error> {
        let details_json = trade
            .details
            .as_ref()
            .and_then(|details| serde_json::to_string(details).ok());
        let completed_at = if matches!(
            local_status,
            "completed" | "failed" | "refunded" | "expired"
        ) {
            Some(Utc::now().naive_utc())
        } else {
            None
        };

        sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET upstream_trade_id = ?,
                provider = ?,
                provider_trade_id = ?,
                provider_password = ?,
                target_ticker = ?,
                target_network = ?,
                source_coin_name = ?,
                target_coin_name = ?,
                amount = ?,
                amount_to = ?,
                fixed = ?,
                payment = ?,
                deposit_address = ?,
                deposit_extra_id = ?,
                settlement_address = ?,
                settlement_extra_id = ?,
                refund_address = ?,
                refund_extra_id = ?,
                provider_status = ?,
                details_json = ?,
                status = ?,
                completed_at = COALESCE(?, completed_at),
                last_synced_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(&trade.trade_id)
        .bind(&trade.provider)
        .bind(&trade.id_provider)
        .bind(&trade.password)
        .bind(&trade.ticker_to)
        .bind(&trade.network_to)
        .bind(&trade.coin_from)
        .bind(&trade.coin_to)
        .bind(trade.amount_from)
        .bind(trade.amount_to)
        .bind(trade.fixed)
        .bind(trade.payment)
        .bind(&trade.address_provider)
        .bind(&trade.address_provider_memo)
        .bind(&trade.address_user)
        .bind(&trade.address_user_memo)
        .bind(&trade.refund_address)
        .bind(&trade.refund_address_memo)
        .bind(&trade.status)
        .bind(details_json)
        .bind(local_status)
        .bind(completed_at)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_retry_pending(
        &self,
        order_id: &str,
        error: &str,
        retry_after_seconds: i64,
    ) -> Result<(), Error> {
        let next_retry_at = (Utc::now() + ChronoDuration::seconds(retry_after_seconds)).naive_utc();

        sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET status = 'retry_pending',
                last_error = ?,
                next_retry_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(error)
        .bind(next_retry_at)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_failed(&self, order_id: &str, error: &str) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET status = 'failed',
                last_error = ?,
                completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(error)
        .bind(order_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_exhausted_pending_failed(&self, max_attempts: i32) -> Result<u64, Error> {
        let result = sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET status = 'failed',
                last_error = COALESCE(last_error, 'Max retry attempts exhausted'),
                completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('queued', 'retry_pending')
              AND attempt_count >= ?
            "#,
        )
        .bind(max_attempts)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn claim_pending_orders(
        &self,
        limit: usize,
        max_attempts: i32,
    ) -> Result<Vec<GiftCardOrderRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                user_id,
                client_id,
                owner_key,
                request_hash,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                recipient_email,
                card_markup,
                webhook_mode,
                webhook_url,
                upstream_trade_id,
                provider,
                provider_trade_id,
                provider_password,
                target_ticker,
                target_network,
                source_coin_name,
                target_coin_name,
                CAST(amount_to AS DOUBLE) AS amount_to,
                fixed,
                payment,
                deposit_address,
                deposit_extra_id,
                settlement_address,
                settlement_extra_id,
                refund_address,
                refund_extra_id,
                provider_status,
                details_json,
                status,
                last_error,
                attempt_count,
                next_retry_at,
                last_synced_at,
                completed_at,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE status IN ('queued', 'retry_pending')
              AND attempt_count < ?
              AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
            ORDER BY created_at ASC
            LIMIT ?
            "#,
        )
        .bind(max_attempts)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(Self::row_to_order)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn list_active_orders_for_refresh(
        &self,
        limit: usize,
        stale_after_seconds: i64,
    ) -> Result<Vec<GiftCardOrderRecord>, Error> {
        let stale_before = (Utc::now() - ChronoDuration::seconds(stale_after_seconds)).naive_utc();
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                user_id,
                client_id,
                owner_key,
                request_hash,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                recipient_email,
                card_markup,
                webhook_mode,
                webhook_url,
                upstream_trade_id,
                provider,
                provider_trade_id,
                provider_password,
                target_ticker,
                target_network,
                source_coin_name,
                target_coin_name,
                CAST(amount_to AS DOUBLE) AS amount_to,
                fixed,
                payment,
                deposit_address,
                deposit_extra_id,
                settlement_address,
                settlement_extra_id,
                refund_address,
                refund_extra_id,
                provider_status,
                details_json,
                status,
                last_error,
                attempt_count,
                next_retry_at,
                last_synced_at,
                completed_at,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE upstream_trade_id IS NOT NULL
              AND status NOT IN ('completed', 'failed', 'refunded', 'expired')
              AND (last_synced_at IS NULL OR last_synced_at <= ?)
            ORDER BY COALESCE(last_synced_at, created_at) ASC
            LIMIT ?
            "#,
        )
        .bind(stale_before)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(Self::row_to_order)
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn redact_terminal_orders(&self, retention_days: i64) -> Result<u64, Error> {
        let cutoff = (Utc::now() - ChronoDuration::days(retention_days)).naive_utc();
        let result = sqlx::query(
            r#"
            UPDATE giftcard_orders
            SET recipient_email = CONCAT('[redacted:', CHAR_LENGTH(recipient_email), ' chars]'),
                webhook_url = NULL,
                provider_password = NULL,
                deposit_address = NULL,
                deposit_extra_id = NULL,
                settlement_address = NULL,
                settlement_extra_id = NULL,
                refund_address = NULL,
                refund_extra_id = NULL,
                details_json = NULL,
                last_error = CASE
                    WHEN last_error IS NULL OR last_error LIKE '[redacted error:%' THEN last_error
                    ELSE CONCAT('[redacted error:', CHAR_LENGTH(last_error), ' chars]')
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE status IN ('completed', 'failed', 'refunded', 'expired')
              AND completed_at IS NOT NULL
              AND completed_at <= ?
              AND (
                    recipient_email NOT LIKE '[redacted:%'
                    OR webhook_url IS NOT NULL
                    OR provider_password IS NOT NULL
                    OR deposit_address IS NOT NULL
                    OR deposit_extra_id IS NOT NULL
                    OR settlement_address IS NOT NULL
                    OR settlement_extra_id IS NOT NULL
                    OR refund_address IS NOT NULL
                    OR refund_extra_id IS NOT NULL
                    OR details_json IS NOT NULL
                    OR (last_error IS NOT NULL AND last_error NOT LIKE '[redacted error:%')
                  )
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    fn row_to_order(row: sqlx::mysql::MySqlRow) -> Result<GiftCardOrderRecord, Error> {
        let details = match row.try_get::<Option<String>, _>("details_json")? {
            Some(raw) if !raw.trim().is_empty() => {
                serde_json::from_str::<TrocadorTradeDetails>(&raw).ok()
            }
            _ => None,
        };

        Ok(GiftCardOrderRecord {
            id: row.try_get("id")?,
            user_id: row.try_get("user_id")?,
            client_id: row.try_get("client_id")?,
            owner_key: row.try_get("owner_key")?,
            request_hash: row.try_get("request_hash")?,
            order_kind: row.try_get("order_kind")?,
            product_id: row.try_get("product_id")?,
            prepaid_provider: row.try_get("prepaid_provider")?,
            currency_code: row.try_get("currency_code")?,
            source_ticker: row.try_get("source_ticker")?,
            source_network: row.try_get("source_network")?,
            amount: row.try_get("amount")?,
            recipient_email: row.try_get("recipient_email")?,
            card_markup: row.try_get("card_markup")?,
            webhook_mode: row.try_get("webhook_mode")?,
            webhook_url: row.try_get("webhook_url")?,
            upstream_trade_id: row.try_get("upstream_trade_id")?,
            provider: row.try_get("provider")?,
            provider_trade_id: row.try_get("provider_trade_id")?,
            provider_password: row.try_get("provider_password")?,
            target_ticker: row.try_get("target_ticker")?,
            target_network: row.try_get("target_network")?,
            source_coin_name: row.try_get("source_coin_name")?,
            target_coin_name: row.try_get("target_coin_name")?,
            amount_to: row.try_get("amount_to")?,
            fixed: row.try_get("fixed")?,
            payment: row.try_get("payment")?,
            deposit_address: row.try_get("deposit_address")?,
            deposit_extra_id: row.try_get("deposit_extra_id")?,
            settlement_address: row.try_get("settlement_address")?,
            settlement_extra_id: row.try_get("settlement_extra_id")?,
            refund_address: row.try_get("refund_address")?,
            refund_extra_id: row.try_get("refund_extra_id")?,
            provider_status: row.try_get("provider_status")?,
            details,
            status: row.try_get("status")?,
            last_error: row.try_get("last_error")?,
            attempt_count: row.try_get("attempt_count")?,
            next_retry_at: row
                .try_get::<Option<NaiveDateTime>, _>("next_retry_at")?
                .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc)),
            last_synced_at: row
                .try_get::<Option<NaiveDateTime>, _>("last_synced_at")?
                .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc)),
            completed_at: row
                .try_get::<Option<NaiveDateTime>, _>("completed_at")?
                .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc)),
            created_at: DateTime::<Utc>::from_naive_utc_and_offset(
                row.try_get::<NaiveDateTime, _>("created_at")?,
                Utc,
            ),
            updated_at: DateTime::<Utc>::from_naive_utc_and_offset(
                row.try_get::<NaiveDateTime, _>("updated_at")?,
                Utc,
            ),
        })
    }
}

pub fn new_order_id() -> String {
    Uuid::new_v4().to_string()
}

fn normalize_named_lock_key(key: &str) -> String {
    if key.len() <= 64 {
        return key.to_string();
    }

    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::normalize_named_lock_key;

    #[test]
    fn keep_short_lock_keys_unchanged() {
        let key = "giftcard:test:lock";
        assert_eq!(normalize_named_lock_key(key), key);
    }

    #[test]
    fn hash_long_lock_keys_to_mysql_safe_length() {
        let key = "giftcard:create:client:00798552-8bf8-48d8-8a14-15f48eb91804:0b2c34b9b7f037db390e0f487251bc31e30e8bd89b25b3b225c556ea2fd02355";
        let normalized = normalize_named_lock_key(key);

        assert_eq!(normalized.len(), 64);
        assert_ne!(normalized, key);
    }
}
