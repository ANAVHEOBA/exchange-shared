use chrono::{DateTime, NaiveDateTime, Utc};
use csv::Writer;
use serde::Serialize;
use sqlx::{MySql, QueryBuilder, Row};

use super::{
    model::AdminAccount,
    schema::{
        AdminLoginResponse, AdminOverviewResponse, AdminOverviewSwapMetrics,
        AdminOverviewWhatsAppMetrics, AdminSwapExportQuery, AdminUserResponse,
        OpsCreateNoteRequest, OpsFinanceDailyRow, OpsFinanceProviderRow, OpsFinanceQuery,
        OpsFinanceResponse, OpsFinanceTotals, OpsHealthResponse, OpsNoteResponse,
        OpsProviderHealthRow, OpsRiskFlag, OpsSearchGiftCardResult, OpsSearchQuery,
        OpsSearchResponse, OpsSearchSupportResult, OpsSearchSwapResult, OpsWebhookDeliveryRow,
        OpsWebhookMonitorResponse, OpsWorkerHealth,
    },
};
use crate::config::DbPool;
use crate::services::jwt::JwtService;

#[derive(Debug)]
pub enum AdminError {
    InvalidCredentials,
    InvalidRequest(String),
    TokenCreation(String),
    Database(String),
    Csv(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials => write!(f, "Invalid admin email or password"),
            Self::InvalidRequest(error) => write!(f, "Invalid export request: {}", error),
            Self::TokenCreation(error) => write!(f, "Failed to create admin token: {}", error),
            Self::Database(error) => write!(f, "Database error: {}", error),
            Self::Csv(error) => write!(f, "CSV export error: {}", error),
        }
    }
}

pub struct AdminCrud<'a> {
    db: DbPool,
    jwt_service: &'a JwtService,
    account: AdminAccount,
}

impl<'a> AdminCrud<'a> {
    pub fn new(db: DbPool, jwt_service: &'a JwtService) -> Self {
        Self {
            db,
            jwt_service,
            account: AdminAccount::from_env(),
        }
    }

    pub async fn login(
        &self,
        email: &str,
        password: &str,
    ) -> Result<AdminLoginResponse, AdminError> {
        if !self.account.matches_credentials(email, password) {
            return Err(AdminError::InvalidCredentials);
        }

        let access_token = self
            .jwt_service
            .create_access_token(&self.account.id, &self.account.email)
            .map_err(|error| AdminError::TokenCreation(error.to_string()))?;

        let refresh_token = self
            .jwt_service
            .create_refresh_token(&self.account.id)
            .map_err(|error| AdminError::TokenCreation(error.to_string()))?;

        Ok(AdminLoginResponse {
            access_token,
            refresh_token,
            token_type: "Bearer",
            expires_in: self.jwt_service.get_access_token_duration_secs(),
            admin: AdminUserResponse {
                id: self.account.id.clone(),
                email: self.account.email.clone(),
            },
        })
    }

    pub async fn export_swaps_csv(
        &self,
        query: &AdminSwapExportQuery,
    ) -> Result<Vec<u8>, AdminError> {
        let date_from = parse_optional_rfc3339(query.date_from.as_deref(), "date_from")?;
        let date_to = parse_optional_rfc3339(query.date_to.as_deref(), "date_to")?;

        let mut query_builder = QueryBuilder::<MySql>::new(
            "SELECT
                id AS swap_id,
                user_id,
                client_id,
                provider_id AS provider,
                provider_swap_id,
                CAST(status AS CHAR) AS status,
                from_currency,
                from_network,
                to_currency,
                to_network,
                CAST(amount AS DOUBLE) AS amount,
                CAST(estimated_receive AS DOUBLE) AS estimated_receive,
                CAST(actual_receive AS DOUBLE) AS actual_receive,
                CAST(rate AS DOUBLE) AS rate,
                CAST(network_fee AS DOUBLE) AS network_fee,
                CAST(provider_fee AS DOUBLE) AS provider_fee,
                CAST(platform_fee AS DOUBLE) AS platform_fee,
                CAST(total_fee AS DOUBLE) AS total_fee,
                deposit_address,
                deposit_extra_id,
                recipient_address,
                recipient_extra_id,
                refund_address,
                refund_extra_id,
                tx_hash_in,
                tx_hash_out,
                CAST(rate_type AS CHAR) AS rate_type,
                CAST(is_payment AS UNSIGNED) AS is_payment,
                CAST(is_sandbox AS UNSIGNED) AS is_sandbox,
                error,
                expires_at,
                completed_at,
                created_at,
                updated_at
            FROM swaps
            WHERE 1 = 1",
        );

        if let Some(provider) = query.provider.as_deref() {
            query_builder
                .push(" AND provider_id = ")
                .push_bind(provider.trim());
        }
        if let Some(provider_swap_id) = query.provider_swap_id.as_deref() {
            query_builder
                .push(" AND provider_swap_id = ")
                .push_bind(provider_swap_id.trim());
        }
        if let Some(status) = query.status.as_deref() {
            query_builder
                .push(" AND status = ")
                .push_bind(status.trim());
        }
        if let Some(from_currency) = query.from_currency.as_deref() {
            query_builder
                .push(" AND from_currency = ")
                .push_bind(from_currency.trim());
        }
        if let Some(from_network) = query.from_network.as_deref() {
            query_builder
                .push(" AND from_network = ")
                .push_bind(from_network.trim());
        }
        if let Some(to_currency) = query.to_currency.as_deref() {
            query_builder
                .push(" AND to_currency = ")
                .push_bind(to_currency.trim());
        }
        if let Some(to_network) = query.to_network.as_deref() {
            query_builder
                .push(" AND to_network = ")
                .push_bind(to_network.trim());
        }
        if let Some(user_id) = query.user_id.as_deref() {
            query_builder
                .push(" AND user_id = ")
                .push_bind(user_id.trim());
        }
        if let Some(client_id) = query.client_id.as_deref() {
            query_builder
                .push(" AND client_id = ")
                .push_bind(client_id.trim());
        }
        if let Some(is_sandbox) = query.is_sandbox {
            query_builder
                .push(" AND is_sandbox = ")
                .push_bind(is_sandbox);
        }
        if let Some(is_payment) = query.is_payment {
            query_builder
                .push(" AND is_payment = ")
                .push_bind(is_payment);
        }
        if let Some(date_from) = date_from {
            query_builder
                .push(" AND created_at >= ")
                .push_bind(date_from);
        }
        if let Some(date_to) = date_to {
            query_builder.push(" AND created_at <= ").push_bind(date_to);
        }

        query_builder.push(" ORDER BY created_at DESC, swap_id DESC");

        let rows = query_builder
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        let mut writer = Writer::from_writer(Vec::new());

        for row in rows {
            let export_row = AdminSwapCsvRow {
                swap_id: row.get("swap_id"),
                user_id: row.try_get("user_id").ok(),
                client_id: row.try_get("client_id").ok(),
                provider: row.get("provider"),
                provider_swap_id: row.try_get("provider_swap_id").ok(),
                status: row.get("status"),
                from_currency: row.get("from_currency"),
                from_network: row.get("from_network"),
                to_currency: row.get("to_currency"),
                to_network: row.get("to_network"),
                amount: row.get("amount"),
                estimated_receive: row.get("estimated_receive"),
                actual_receive: row.try_get("actual_receive").ok(),
                rate: row.get("rate"),
                network_fee: row.get("network_fee"),
                provider_fee: row.get("provider_fee"),
                platform_fee: row.get("platform_fee"),
                total_fee: row.get("total_fee"),
                deposit_address: row.get("deposit_address"),
                deposit_extra_id: row.try_get("deposit_extra_id").ok(),
                recipient_address: row.get("recipient_address"),
                recipient_extra_id: row.try_get("recipient_extra_id").ok(),
                refund_address: row.try_get("refund_address").ok(),
                refund_extra_id: row.try_get("refund_extra_id").ok(),
                tx_hash_in: row.try_get("tx_hash_in").ok(),
                tx_hash_out: row.try_get("tx_hash_out").ok(),
                rate_type: row.get("rate_type"),
                is_payment: row.get::<u8, _>("is_payment") != 0,
                is_sandbox: row.get::<u8, _>("is_sandbox") != 0,
                error: row.try_get("error").ok(),
                expires_at: row.try_get("expires_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            };

            writer
                .serialize(export_row)
                .map_err(|error| AdminError::Csv(error.to_string()))?;
        }

        writer
            .into_inner()
            .map_err(|error| AdminError::Csv(error.into_error().to_string()))
    }

    pub async fn overview(&self) -> Result<AdminOverviewResponse, AdminError> {
        let open_swaps = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM swaps
            WHERE status NOT IN ('completed', 'failed', 'refunded', 'expired')
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let failed_last_24h = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM swaps
            WHERE status = 'failed'
              AND updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY)
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let refunded_last_24h = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM swaps
            WHERE status = 'refunded'
              AND updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY)
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let open_conversations = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM whatsapp_sessions
            WHERE admin_status NOT IN ('closed', 'rejected', 'paid')
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let giftcard_sell_leads = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM whatsapp_sessions
            WHERE admin_tag = 'giftcard_sell'
              AND admin_status NOT IN ('closed', 'rejected', 'paid')
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let waiting_user = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM whatsapp_sessions
            WHERE admin_status = 'waiting_user'
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(AdminOverviewResponse {
            swaps: AdminOverviewSwapMetrics {
                open: open_swaps.max(0) as u64,
                failed_last_24h: failed_last_24h.max(0) as u64,
                refunded_last_24h: refunded_last_24h.max(0) as u64,
            },
            whatsapp: AdminOverviewWhatsAppMetrics {
                open_conversations: open_conversations.max(0) as u64,
                giftcard_sell_leads: giftcard_sell_leads.max(0) as u64,
                waiting_user: waiting_user.max(0) as u64,
            },
        })
    }

    pub async fn global_search(
        &self,
        query: &OpsSearchQuery,
    ) -> Result<OpsSearchResponse, AdminError> {
        let search = query.q.trim();
        if search.is_empty() {
            return Err(AdminError::InvalidRequest(
                "Search query cannot be empty".to_string(),
            ));
        }

        let limit = query.limit.unwrap_or(10).clamp(1, 50) as i64;
        let like = format!("%{}%", search);

        let swap_rows = sqlx::query(
            r#"
            SELECT
                id,
                provider_id,
                provider_swap_id,
                CAST(status AS CHAR) AS status,
                from_currency,
                from_network,
                to_currency,
                to_network,
                CAST(amount AS DOUBLE) AS amount,
                CAST(estimated_receive AS DOUBLE) AS estimated_receive,
                client_id,
                user_id,
                tx_hash_in,
                tx_hash_out,
                created_at,
                updated_at
            FROM swaps
            WHERE id = ?
               OR provider_swap_id = ?
               OR client_id = ?
               OR user_id = ?
               OR deposit_address = ?
               OR recipient_address = ?
               OR refund_address = ?
               OR tx_hash_in = ?
               OR tx_hash_out = ?
               OR provider_id LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?
            "#,
        )
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(&like)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let giftcard_rows = sqlx::query(
            r#"
            SELECT
                id,
                upstream_trade_id,
                order_kind,
                product_id,
                prepaid_provider,
                currency_code,
                recipient_email,
                status,
                provider_status,
                provider,
                provider_trade_id,
                source_ticker,
                source_network,
                CAST(amount AS DOUBLE) AS amount,
                CAST(amount_to AS DOUBLE) AS amount_to,
                client_id,
                user_id,
                created_at,
                updated_at
            FROM giftcard_orders
            WHERE id = ?
               OR upstream_trade_id = ?
               OR provider_trade_id = ?
               OR client_id = ?
               OR user_id = ?
               OR recipient_email = ?
               OR deposit_address = ?
               OR settlement_address = ?
               OR refund_address = ?
               OR product_id LIKE ?
               OR prepaid_provider LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?
            "#,
        )
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(search)
        .bind(&like)
        .bind(&like)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let support_rows = sqlx::query(
            r#"
            SELECT
                wa_id,
                state,
                admin_status,
                admin_tag,
                assigned_to,
                updated_at
            FROM whatsapp_sessions
            WHERE wa_id = ?
               OR assigned_to = ?
               OR admin_tag LIKE ?
               OR internal_note LIKE ?
            ORDER BY updated_at DESC
            LIMIT ?
            "#,
        )
        .bind(search)
        .bind(search)
        .bind(&like)
        .bind(&like)
        .bind(limit)
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsSearchResponse {
            query: search.to_string(),
            swaps: swap_rows
                .into_iter()
                .map(|row| OpsSearchSwapResult {
                    id: row.get("id"),
                    provider: row.get("provider_id"),
                    provider_swap_id: row.try_get("provider_swap_id").ok(),
                    status: row.get("status"),
                    from_currency: row.get("from_currency"),
                    from_network: row.get("from_network"),
                    to_currency: row.get("to_currency"),
                    to_network: row.get("to_network"),
                    amount: row.get("amount"),
                    estimated_receive: row.get("estimated_receive"),
                    client_id: row.try_get("client_id").ok(),
                    user_id: row.try_get("user_id").ok(),
                    tx_hash_in: row.try_get("tx_hash_in").ok(),
                    tx_hash_out: row.try_get("tx_hash_out").ok(),
                    created_at: format_datetime(row.get("created_at")),
                    updated_at: format_datetime(row.get("updated_at")),
                })
                .collect(),
            giftcards: giftcard_rows
                .into_iter()
                .map(|row| {
                    let email: String = row.get("recipient_email");
                    OpsSearchGiftCardResult {
                        id: row.get("id"),
                        trade_id: row.try_get("upstream_trade_id").ok(),
                        order_kind: row.get("order_kind"),
                        product_id: row.try_get("product_id").ok(),
                        prepaid_provider: row.try_get("prepaid_provider").ok(),
                        currency_code: row.try_get("currency_code").ok(),
                        recipient_email_masked: mask_email(&email),
                        status: row.get("status"),
                        provider_status: row.try_get("provider_status").ok(),
                        provider: row.try_get("provider").ok(),
                        provider_trade_id: row.try_get("provider_trade_id").ok(),
                        source_ticker: row.get("source_ticker"),
                        source_network: row.get("source_network"),
                        amount: row.get("amount"),
                        amount_to: row.try_get("amount_to").ok(),
                        client_id: row.try_get("client_id").ok(),
                        user_id: row.try_get("user_id").ok(),
                        created_at: format_datetime(row.get("created_at")),
                        updated_at: format_datetime(row.get("updated_at")),
                    }
                })
                .collect(),
            support: support_rows
                .into_iter()
                .map(|row| OpsSearchSupportResult {
                    wa_id: row.get("wa_id"),
                    status: row.get("admin_status"),
                    tag: row.try_get("admin_tag").ok(),
                    assigned_to: row.try_get("assigned_to").ok(),
                    state: row.get("state"),
                    updated_at: format_datetime(row.get("updated_at")),
                })
                .collect(),
        })
    }

    pub async fn ops_health(&self) -> Result<OpsHealthResponse, AdminError> {
        let worker = OpsWorkerHealth {
            giftcard_queued: count_query(
                &self.db,
                "SELECT COUNT(*) FROM giftcard_orders WHERE status = 'queued'",
            )
            .await?,
            giftcard_retry_pending: count_query(
                &self.db,
                "SELECT COUNT(*) FROM giftcard_orders WHERE status = 'retry_pending'",
            )
            .await?,
            giftcard_creating: count_query(
                &self.db,
                "SELECT COUNT(*) FROM giftcard_orders WHERE status = 'creating'",
            )
            .await?,
            giftcard_stale_active: count_query(
                &self.db,
                "SELECT COUNT(*) FROM giftcard_orders WHERE status NOT IN ('completed','failed','refunded','expired') AND updated_at < (UTC_TIMESTAMP() - INTERVAL 30 MINUTE)",
            )
            .await?,
            swap_polling_due: count_query(
                &self.db,
                "SELECT COUNT(*) FROM polling_states WHERE next_poll_at <= UTC_TIMESTAMP()",
            )
            .await?,
            swap_polling_stale: count_query(
                &self.db,
                "SELECT COUNT(*) FROM polling_states WHERE updated_at < (UTC_TIMESTAMP() - INTERVAL 30 MINUTE)",
            )
            .await?,
            webhook_retry_due: count_query(
                &self.db,
                "SELECT COUNT(*) FROM webhook_deliveries WHERE delivered_at IS NULL AND is_dlq = FALSE AND next_retry_at <= UTC_TIMESTAMP()",
            )
            .await?,
            webhook_dead_letters: count_query(
                &self.db,
                "SELECT COUNT(*) FROM webhook_deliveries WHERE is_dlq = TRUE",
            )
            .await?,
        };

        let provider_rows = sqlx::query(
            r#"
            SELECT
                provider,
                CAST(SUM(open_swaps) AS SIGNED) AS open_swaps,
                CAST(SUM(failed_swaps_24h) AS SIGNED) AS failed_swaps_24h,
                CAST(SUM(giftcard_active) AS SIGNED) AS giftcard_active,
                CAST(SUM(giftcard_failed_24h) AS SIGNED) AS giftcard_failed_24h,
                MAX(last_activity_at) AS last_activity_at
            FROM (
                SELECT
                    provider_id AS provider,
                    SUM(CASE WHEN status NOT IN ('completed','failed','refunded','expired') THEN 1 ELSE 0 END) AS open_swaps,
                    SUM(CASE WHEN status = 'failed' AND updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY) THEN 1 ELSE 0 END) AS failed_swaps_24h,
                    0 AS giftcard_active,
                    0 AS giftcard_failed_24h,
                    MAX(updated_at) AS last_activity_at
                FROM swaps
                GROUP BY provider_id
                UNION ALL
                SELECT
                    COALESCE(provider, 'giftcard_provider_unknown') AS provider,
                    0 AS open_swaps,
                    0 AS failed_swaps_24h,
                    SUM(CASE WHEN status NOT IN ('completed','failed','refunded','expired') THEN 1 ELSE 0 END) AS giftcard_active,
                    SUM(CASE WHEN status = 'failed' AND updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY) THEN 1 ELSE 0 END) AS giftcard_failed_24h,
                    MAX(updated_at) AS last_activity_at
                FROM giftcard_orders
                GROUP BY COALESCE(provider, 'giftcard_provider_unknown')
            ) provider_health
            GROUP BY provider
            ORDER BY failed_swaps_24h DESC, giftcard_failed_24h DESC, provider ASC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let mut risk_flags = self.detect_giftcard_math_flags().await?;
        risk_flags.extend(self.detect_swap_flags().await?);

        Ok(OpsHealthResponse {
            generated_at: Utc::now().to_rfc3339(),
            worker,
            providers: provider_rows
                .into_iter()
                .map(|row| OpsProviderHealthRow {
                    provider: row.get("provider"),
                    open_swaps: int_field_to_u64(&row, "open_swaps"),
                    failed_swaps_24h: int_field_to_u64(&row, "failed_swaps_24h"),
                    giftcard_active: int_field_to_u64(&row, "giftcard_active"),
                    giftcard_failed_24h: int_field_to_u64(&row, "giftcard_failed_24h"),
                    last_activity_at: row
                        .try_get::<Option<NaiveDateTime>, _>("last_activity_at")
                        .ok()
                        .flatten()
                        .map(format_datetime),
                })
                .collect(),
            risk_flags,
        })
    }

    pub async fn finance_summary(
        &self,
        query: &OpsFinanceQuery,
    ) -> Result<OpsFinanceResponse, AdminError> {
        let date_from = parse_optional_rfc3339(query.date_from.as_deref(), "date_from")?;
        let date_to = parse_optional_rfc3339(query.date_to.as_deref(), "date_to")?;

        let mut totals_query = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps,
                CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps,
                CAST(SUM(CASE WHEN status = 'expired' THEN 1 ELSE 0 END) AS SIGNED) AS expired_swaps,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS DOUBLE) AS swap_volume_input,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN platform_fee ELSE 0 END), 0) AS DOUBLE) AS swap_platform_fees,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN provider_fee ELSE 0 END), 0) AS DOUBLE) AS swap_provider_fees
            FROM swaps
            WHERE 1 = 1
            "#,
        );
        push_date_filter(&mut totals_query, date_from, date_to, "created_at");
        let totals_row = totals_query
            .build()
            .fetch_one(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        let mut giftcard_totals_query = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS giftcard_completed,
                CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS giftcard_failed,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS DOUBLE) AS giftcard_volume
            FROM giftcard_orders
            WHERE 1 = 1
            "#,
        );
        push_date_filter(&mut giftcard_totals_query, date_from, date_to, "created_at");
        let giftcard_totals_row = giftcard_totals_query
            .build()
            .fetch_one(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        let daily = self.finance_daily(date_from, date_to).await?;
        let providers = self.finance_providers(date_from, date_to).await?;

        Ok(OpsFinanceResponse {
            generated_at: Utc::now().to_rfc3339(),
            totals: OpsFinanceTotals {
                completed_swaps: int_field_to_u64(&totals_row, "completed_swaps"),
                failed_swaps: int_field_to_u64(&totals_row, "failed_swaps"),
                expired_swaps: int_field_to_u64(&totals_row, "expired_swaps"),
                swap_volume_input: float_field(&totals_row, "swap_volume_input"),
                swap_platform_fees: float_field(&totals_row, "swap_platform_fees"),
                swap_provider_fees: float_field(&totals_row, "swap_provider_fees"),
                giftcard_completed: int_field_to_u64(&giftcard_totals_row, "giftcard_completed"),
                giftcard_failed: int_field_to_u64(&giftcard_totals_row, "giftcard_failed"),
                giftcard_volume: float_field(&giftcard_totals_row, "giftcard_volume"),
            },
            daily,
            providers,
        })
    }

    pub async fn webhook_monitor(&self) -> Result<OpsWebhookMonitorResponse, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                swap_id,
                event_type,
                attempt_number,
                max_attempts,
                next_retry_at,
                delivered_at,
                response_status,
                response_time_ms,
                error_message,
                is_dlq,
                created_at,
                updated_at
            FROM webhook_deliveries
            WHERE delivered_at IS NULL OR is_dlq = TRUE
            ORDER BY is_dlq DESC, COALESCE(next_retry_at, created_at) ASC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsWebhookMonitorResponse {
            deliveries: rows
                .into_iter()
                .map(|row| OpsWebhookDeliveryRow {
                    id: row.get("id"),
                    swap_id: row.get("swap_id"),
                    event_type: row.get("event_type"),
                    attempt_number: row.get("attempt_number"),
                    max_attempts: row.get("max_attempts"),
                    next_retry_at: row
                        .try_get::<Option<NaiveDateTime>, _>("next_retry_at")
                        .ok()
                        .flatten()
                        .map(format_datetime),
                    delivered_at: row
                        .try_get::<Option<NaiveDateTime>, _>("delivered_at")
                        .ok()
                        .flatten()
                        .map(format_datetime),
                    response_status: row.try_get("response_status").ok(),
                    response_time_ms: row.try_get("response_time_ms").ok(),
                    error_message: row.try_get("error_message").ok(),
                    is_dlq: row.get::<u8, _>("is_dlq") != 0,
                    created_at: format_datetime(row.get("created_at")),
                    updated_at: format_datetime(row.get("updated_at")),
                })
                .collect(),
        })
    }

    pub async fn create_note(
        &self,
        admin_id: &str,
        admin_email: &str,
        req: &OpsCreateNoteRequest,
    ) -> Result<OpsNoteResponse, AdminError> {
        let result = sqlx::query(
            r#"
            INSERT INTO ops_notes (entity_type, entity_id, admin_id, admin_email, note)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind(req.entity_type.trim())
        .bind(req.entity_id.trim())
        .bind(admin_id)
        .bind(admin_email)
        .bind(req.note.trim())
        .execute(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsNoteResponse {
            id: result.last_insert_id(),
            entity_type: req.entity_type.trim().to_string(),
            entity_id: req.entity_id.trim().to_string(),
            admin_email: admin_email.to_string(),
            note: req.note.trim().to_string(),
            created_at: Utc::now().to_rfc3339(),
        })
    }

    async fn detect_giftcard_math_flags(&self) -> Result<Vec<OpsRiskFlag>, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT id, currency_code, source_ticker, amount, amount_to, provider_status, updated_at
            FROM giftcard_orders
            WHERE source_ticker NOT IN ('USDT', 'USDC', 'DAI')
              AND ABS(CAST(amount AS DOUBLE) - COALESCE(CAST(amount_to AS DOUBLE), CAST(amount AS DOUBLE))) < 0.00000001
              AND status NOT IN ('failed', 'expired', 'refunded')
            ORDER BY updated_at DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let currency_code: Option<String> = row.try_get("currency_code").ok();
                let source_ticker: String = row.get("source_ticker");
                OpsRiskFlag {
                    entity_type: "giftcard_order".to_string(),
                    entity_id: row.get("id"),
                    severity: "high".to_string(),
                    code: "giftcard_crypto_equals_fiat".to_string(),
                    message: format!(
                        "{} payment amount appears equal to fiat card value ({})",
                        source_ticker,
                        currency_code.unwrap_or_else(|| "unknown currency".to_string())
                    ),
                }
            })
            .collect())
    }

    async fn detect_swap_flags(&self) -> Result<Vec<OpsRiskFlag>, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT id, status, error, updated_at
            FROM swaps
            WHERE status NOT IN ('completed','failed','refunded','expired')
              AND updated_at < (UTC_TIMESTAMP() - INTERVAL 45 MINUTE)
            ORDER BY updated_at ASC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsRiskFlag {
                entity_type: "swap".to_string(),
                entity_id: row.get("id"),
                severity: "medium".to_string(),
                code: "swap_stale_active".to_string(),
                message: format!(
                    "Swap has been active with status {}",
                    row.get::<String, _>("status")
                ),
            })
            .collect())
    }

    async fn finance_daily(
        &self,
        date_from: Option<DateTime<Utc>>,
        date_to: Option<DateTime<Utc>>,
    ) -> Result<Vec<OpsFinanceDailyRow>, AdminError> {
        let mut query = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                day,
                CAST(SUM(completed_swaps) AS SIGNED) AS completed_swaps,
                CAST(SUM(failed_swaps) AS SIGNED) AS failed_swaps,
                CAST(SUM(swap_volume_input) AS DOUBLE) AS swap_volume_input,
                CAST(SUM(swap_platform_fees) AS DOUBLE) AS swap_platform_fees,
                CAST(SUM(giftcard_completed) AS SIGNED) AS giftcard_completed,
                CAST(SUM(giftcard_volume) AS DOUBLE) AS giftcard_volume
            FROM (
                SELECT
                    DATE(created_at) AS day,
                    CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps,
                    CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps,
                    CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS DOUBLE) AS swap_volume_input,
                    CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN platform_fee ELSE 0 END), 0) AS DOUBLE) AS swap_platform_fees,
                    0 AS giftcard_completed,
                    0 AS giftcard_volume
                FROM swaps
                WHERE 1 = 1
            "#,
        );
        push_date_filter(&mut query, date_from, date_to, "created_at");
        query.push(
            r#"
                GROUP BY DATE(created_at)
                UNION ALL
                SELECT
                    DATE(created_at) AS day,
                    0 AS completed_swaps,
                    0 AS failed_swaps,
                    0 AS swap_volume_input,
                    0 AS swap_platform_fees,
                    CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS giftcard_completed,
                    CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS DOUBLE) AS giftcard_volume
                FROM giftcard_orders
                WHERE 1 = 1
            "#,
        );
        push_date_filter(&mut query, date_from, date_to, "created_at");
        query.push(
            r#"
                GROUP BY DATE(created_at)
            ) daily
            GROUP BY day
            ORDER BY day DESC
            LIMIT 90
            "#,
        );

        let rows = query
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsFinanceDailyRow {
                date: row.get::<chrono::NaiveDate, _>("day").to_string(),
                completed_swaps: int_field_to_u64(&row, "completed_swaps"),
                failed_swaps: int_field_to_u64(&row, "failed_swaps"),
                swap_volume_input: float_field(&row, "swap_volume_input"),
                swap_platform_fees: float_field(&row, "swap_platform_fees"),
                giftcard_completed: int_field_to_u64(&row, "giftcard_completed"),
                giftcard_volume: float_field(&row, "giftcard_volume"),
            })
            .collect())
    }

    async fn finance_providers(
        &self,
        date_from: Option<DateTime<Utc>>,
        date_to: Option<DateTime<Utc>>,
    ) -> Result<Vec<OpsFinanceProviderRow>, AdminError> {
        let mut query = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                provider_id,
                CAST(COUNT(*) AS SIGNED) AS swaps,
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps,
                CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END), 0) AS DOUBLE) AS volume_input,
                CAST(COALESCE(SUM(CASE WHEN status = 'completed' THEN platform_fee ELSE 0 END), 0) AS DOUBLE) AS platform_fees
            FROM swaps
            WHERE 1 = 1
            "#,
        );
        push_date_filter(&mut query, date_from, date_to, "created_at");
        query.push(" GROUP BY provider_id ORDER BY volume_input DESC LIMIT 100");

        let rows = query
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsFinanceProviderRow {
                provider: row.get("provider_id"),
                swaps: int_field_to_u64(&row, "swaps"),
                completed_swaps: int_field_to_u64(&row, "completed_swaps"),
                failed_swaps: int_field_to_u64(&row, "failed_swaps"),
                volume_input: float_field(&row, "volume_input"),
                platform_fees: float_field(&row, "platform_fees"),
            })
            .collect())
    }
}

async fn count_query(db: &DbPool, sql: &str) -> Result<u64, AdminError> {
    let count = sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;
    Ok(count.max(0) as u64)
}

fn push_date_filter(
    query: &mut QueryBuilder<'_, MySql>,
    date_from: Option<DateTime<Utc>>,
    date_to: Option<DateTime<Utc>>,
    column: &str,
) {
    if let Some(date_from) = date_from {
        query
            .push(format!(" AND {} >= ", column))
            .push_bind(date_from);
    }
    if let Some(date_to) = date_to {
        query
            .push(format!(" AND {} <= ", column))
            .push_bind(date_to);
    }
}

fn format_datetime(value: NaiveDateTime) -> String {
    DateTime::<Utc>::from_naive_utc_and_offset(value, Utc).to_rfc3339()
}

fn mask_email(email: &str) -> String {
    let Some((local, domain)) = email.split_once('@') else {
        return "[masked]".to_string();
    };
    let prefix: String = local.chars().take(2).collect();
    format!("{}***@{}", prefix, domain)
}

fn int_field_to_u64(row: &sqlx::mysql::MySqlRow, field: &str) -> u64 {
    row.try_get::<Option<i64>, _>(field)
        .ok()
        .flatten()
        .unwrap_or(0)
        .max(0) as u64
}

fn float_field(row: &sqlx::mysql::MySqlRow, field: &str) -> f64 {
    row.try_get::<Option<f64>, _>(field)
        .ok()
        .flatten()
        .unwrap_or(0.0)
}

fn parse_optional_rfc3339(
    value: Option<&str>,
    field_name: &str,
) -> Result<Option<DateTime<Utc>>, AdminError> {
    value
        .map(|raw| {
            chrono::DateTime::parse_from_rfc3339(raw)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|_| {
                    AdminError::InvalidRequest(format!(
                        "{} must be a valid RFC3339 timestamp",
                        field_name
                    ))
                })
        })
        .transpose()
}

#[derive(Debug, Serialize)]
struct AdminSwapCsvRow {
    swap_id: String,
    user_id: Option<String>,
    client_id: Option<String>,
    provider: String,
    provider_swap_id: Option<String>,
    status: String,
    from_currency: String,
    from_network: String,
    to_currency: String,
    to_network: String,
    amount: f64,
    estimated_receive: f64,
    actual_receive: Option<f64>,
    rate: f64,
    network_fee: f64,
    provider_fee: f64,
    platform_fee: f64,
    total_fee: f64,
    deposit_address: String,
    deposit_extra_id: Option<String>,
    recipient_address: String,
    recipient_extra_id: Option<String>,
    refund_address: Option<String>,
    refund_extra_id: Option<String>,
    tx_hash_in: Option<String>,
    tx_hash_out: Option<String>,
    rate_type: String,
    is_payment: bool,
    is_sandbox: bool,
    error: Option<String>,
    expires_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}
