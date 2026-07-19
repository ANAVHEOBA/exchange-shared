use chrono::{DateTime, NaiveDateTime, Utc};
use csv::Writer;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::{MySql, QueryBuilder, Row};

use super::{
    model::AdminAccount,
    schema::{
        AdminLoginResponse, AdminOverviewResponse, AdminOverviewSwapMetrics,
        AdminOverviewWhatsAppMetrics, AdminSwapExportQuery, AdminUserResponse, OpsAssetDetailQuery,
        OpsAssetDetailResponse, OpsAssetListResponse, OpsAssetQuery, OpsAssetRow,
        OpsCreateNoteRequest, OpsDashboardKpis, OpsDashboardQuickAccessItem,
        OpsDashboardRecentActivityItem, OpsDashboardResponse, OpsDashboardStatusBreakdown,
        OpsDashboardTopGiftCard, OpsDashboardTopPair, OpsDashboardVolumePoint,
        OpsFinanceDailyRow, OpsFinanceProviderRow, OpsFinanceQuery, OpsFinanceResponse,
        OpsFinanceTotals, OpsGiftCardCatalogDetailQuery, OpsGiftCardCatalogDetailResponse,
        OpsGiftCardCatalogQuery, OpsGiftCardCatalogResponse, OpsHealthResponse,
        OpsNoteResponse, OpsPayoutPolicySettings, OpsProviderDetailResponse,
        OpsProviderHealthRow, OpsProviderListQuery, OpsProviderListResponse, OpsProviderSummary,
        OpsRiskFlag, OpsSearchGiftCardResult, OpsSearchQuery, OpsSearchResponse,
        OpsSearchSupportResult, OpsSearchSwapResult, OpsSettingsDiagnosticsResponse,
        OpsSettingsResponse, OpsWebhookDeliveryRow, OpsWebhookDetailResponse,
        OpsWebhookMonitorResponse, OpsWebhookQuery, OpsWorkerHealth,
    },
};
use crate::config::DbPool;
use crate::modules::giftcard::{
    fallback_catalog::fallback_catalog, schema::GiftCardProductResponse,
};
use crate::services::jwt::JwtService;
use crate::services::payout_policy::PayoutPolicyConfig;
use crate::services::trocador::{swap_markup_from_env, TrocadorGateway};

#[derive(Debug)]
pub enum AdminError {
    InvalidCredentials,
    InvalidRequest(String),
    NotFound(String),
    TokenCreation(String),
    Config(String),
    Database(String),
    External(String),
    Csv(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials => write!(f, "Invalid admin email or password"),
            Self::InvalidRequest(error) => write!(f, "Invalid request: {}", error),
            Self::NotFound(error) => write!(f, "Not found: {}", error),
            Self::TokenCreation(error) => write!(f, "Failed to create admin token: {}", error),
            Self::Config(error) => write!(f, "Configuration error: {}", error),
            Self::Database(error) => write!(f, "Database error: {}", error),
            Self::External(error) => write!(f, "External service error: {}", error),
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

    pub async fn dashboard(&self) -> Result<OpsDashboardResponse, AdminError> {
        let (
            summary,
            health,
            kpis,
            status_breakdown,
            recent_activity,
            volume_trend,
            top_pairs,
            top_giftcards,
        ) = tokio::try_join!(
            self.overview(),
            self.ops_health(),
            self.dashboard_kpis(),
            self.dashboard_status_breakdown(),
            self.dashboard_recent_activity(),
            self.dashboard_volume_trend(),
            self.dashboard_top_pairs(),
            self.dashboard_top_giftcards()
        )?;

        Ok(OpsDashboardResponse {
            generated_at: Utc::now().to_rfc3339(),
            summary,
            kpis,
            status_breakdown,
            quick_access: Self::dashboard_quick_access(),
            recent_activity,
            volume_trend,
            top_pairs,
            top_giftcards,
            worker: health.worker,
            providers: health.providers,
            risk_flags: health.risk_flags,
        })
    }

    async fn dashboard_kpis(&self) -> Result<OpsDashboardKpis, AdminError> {
        let total_swap_volume = sqlx::query_scalar::<_, Option<f64>>(
            r#"
            SELECT CAST(COALESCE(SUM(amount), 0) AS DOUBLE)
            FROM swaps
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .unwrap_or(0.0);

        let total_giftcard_sales = sqlx::query_scalar::<_, Option<f64>>(
            r#"
            SELECT CAST(COALESCE(SUM(COALESCE(amount_to, amount)), 0) AS DOUBLE)
            FROM giftcard_orders
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .unwrap_or(0.0);

        let total_platform_revenue = sqlx::query_scalar::<_, Option<f64>>(
            r#"
            SELECT CAST(COALESCE(SUM(platform_fee), 0) AS DOUBLE)
            FROM swaps
            WHERE status = 'completed'
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .unwrap_or(0.0);

        let total_transactions = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM swaps) +
                (SELECT COUNT(*) FROM giftcard_orders)
            "#,
        )
        .fetch_one(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let active_users = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM users")
            .fetch_one(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsDashboardKpis {
            total_swap_volume,
            total_giftcard_sales,
            total_platform_revenue,
            total_transactions: total_transactions.max(0) as u64,
            active_users: active_users.max(0) as u64,
        })
    }

    async fn dashboard_status_breakdown(&self) -> Result<OpsDashboardStatusBreakdown, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT status_group, CAST(SUM(total_count) AS SIGNED) AS total_count
            FROM (
                SELECT
                    CASE
                        WHEN status = 'completed' THEN 'completed'
                        WHEN status = 'failed' THEN 'failed'
                        WHEN status = 'expired' THEN 'expired'
                        WHEN status = 'refunded' THEN 'refunded'
                        ELSE 'open'
                    END AS status_group,
                    COUNT(*) AS total_count
                FROM swaps
                GROUP BY status_group

                UNION ALL

                SELECT
                    CASE
                        WHEN status = 'completed' THEN 'completed'
                        WHEN status = 'failed' THEN 'failed'
                        WHEN status = 'expired' THEN 'expired'
                        WHEN status = 'refunded' THEN 'refunded'
                        ELSE 'open'
                    END AS status_group,
                    COUNT(*) AS total_count
                FROM giftcard_orders
                GROUP BY status_group
            ) status_rows
            GROUP BY status_group
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let mut breakdown = OpsDashboardStatusBreakdown {
            completed: 0,
            failed: 0,
            expired: 0,
            refunded: 0,
            open: 0,
        };

        for row in rows {
            let total = int_field_to_u64(&row, "total_count");
            match row.get::<String, _>("status_group").as_str() {
                "completed" => breakdown.completed = total,
                "failed" => breakdown.failed = total,
                "expired" => breakdown.expired = total,
                "refunded" => breakdown.refunded = total,
                _ => breakdown.open = total,
            }
        }

        Ok(breakdown)
    }

    async fn dashboard_recent_activity(
        &self,
    ) -> Result<Vec<OpsDashboardRecentActivityItem>, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM (
                SELECT
                    'swap' AS entity_type,
                    id AS entity_id,
                    CONCAT(from_currency, ' -> ', to_currency) AS title,
                    CONCAT(from_network, ' to ', to_network) AS subtitle,
                    CAST(status AS CHAR) AS status,
                    provider_id AS provider,
                    CAST(amount AS DOUBLE) AS amount,
                    from_currency AS currency,
                    CONCAT('/swap/ops/', id) AS detail_path,
                    created_at
                FROM swaps
                UNION ALL
                SELECT
                    'giftcard_order' AS entity_type,
                    id AS entity_id,
                    COALESCE(product_id, prepaid_provider, 'giftcard') AS title,
                    COALESCE(currency_code, source_network) AS subtitle,
                    status,
                    provider,
                    CAST(COALESCE(amount_to, amount) AS DOUBLE) AS amount,
                    COALESCE(currency_code, source_ticker) AS currency,
                    CONCAT('/giftcards/ops/orders/', id) AS detail_path,
                    created_at
                FROM giftcard_orders
            ) activity
            ORDER BY created_at DESC
            LIMIT 10
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsDashboardRecentActivityItem {
                entity_type: row.get("entity_type"),
                entity_id: row.get("entity_id"),
                title: row.get("title"),
                subtitle: row.try_get("subtitle").ok(),
                status: row.get("status"),
                provider: row.try_get("provider").ok(),
                amount: row.try_get("amount").ok(),
                currency: row.try_get("currency").ok(),
                detail_path: row.get("detail_path"),
                created_at: format_datetime(row.get("created_at")),
            })
            .collect())
    }

    async fn dashboard_volume_trend(&self) -> Result<Vec<OpsDashboardVolumePoint>, AdminError> {
        let swap_rows = sqlx::query(
            r#"
            SELECT
                DATE(created_at) AS activity_date,
                CAST(COUNT(*) AS SIGNED) AS swap_count,
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps,
                CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps,
                CAST(COALESCE(SUM(amount), 0) AS DOUBLE) AS swap_volume_input
            FROM swaps
            GROUP BY DATE(created_at)
            ORDER BY activity_date DESC
            LIMIT 30
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let giftcard_rows = sqlx::query(
            r#"
            SELECT
                DATE(created_at) AS activity_date,
                CAST(COUNT(*) AS SIGNED) AS giftcard_count,
                CAST(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS SIGNED) AS giftcard_completed,
                CAST(COALESCE(SUM(COALESCE(amount_to, amount)), 0) AS DOUBLE) AS giftcard_volume
            FROM giftcard_orders
            GROUP BY DATE(created_at)
            ORDER BY activity_date DESC
            LIMIT 30
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        let mut by_date = std::collections::BTreeMap::new();

        for row in swap_rows {
            let date: String = row.get("activity_date");
            by_date.insert(
                date,
                OpsDashboardVolumePoint {
                    date: String::new(),
                    completed_swaps: int_field_to_u64(&row, "completed_swaps"),
                    failed_swaps: int_field_to_u64(&row, "failed_swaps"),
                    swap_volume_input: float_field(&row, "swap_volume_input"),
                    giftcard_completed: 0,
                    giftcard_volume: 0.0,
                },
            );
        }

        for row in giftcard_rows {
            let date: String = row.get("activity_date");
            let entry = by_date.entry(date).or_insert_with(|| OpsDashboardVolumePoint {
                date: String::new(),
                completed_swaps: 0,
                failed_swaps: 0,
                swap_volume_input: 0.0,
                giftcard_completed: 0,
                giftcard_volume: 0.0,
            });
            entry.giftcard_completed = int_field_to_u64(&row, "giftcard_completed");
            entry.giftcard_volume = float_field(&row, "giftcard_volume");
        }

        Ok(by_date
            .into_iter()
            .map(|(date, mut point)| {
                point.date = date;
                point
            })
            .rev()
            .collect())
    }

    async fn dashboard_top_pairs(&self) -> Result<Vec<OpsDashboardTopPair>, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT
                from_currency,
                from_network,
                to_currency,
                to_network,
                CAST(COUNT(*) AS SIGNED) AS trades,
                CAST(COALESCE(SUM(amount), 0) AS DOUBLE) AS volume_input
            FROM swaps
            GROUP BY from_currency, from_network, to_currency, to_network
            ORDER BY trades DESC, volume_input DESC
            LIMIT 10
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsDashboardTopPair {
                from_currency: row.get("from_currency"),
                from_network: row.get("from_network"),
                to_currency: row.get("to_currency"),
                to_network: row.get("to_network"),
                trades: int_field_to_u64(&row, "trades"),
                volume_input: float_field(&row, "volume_input"),
            })
            .collect())
    }

    async fn dashboard_top_giftcards(&self) -> Result<Vec<OpsDashboardTopGiftCard>, AdminError> {
        let rows = sqlx::query(
            r#"
            SELECT
                COALESCE(NULLIF(TRIM(prepaid_provider), ''), NULLIF(TRIM(product_id), ''), 'giftcard') AS product,
                COALESCE(currency_code, source_ticker) AS currency,
                CAST(COUNT(*) AS SIGNED) AS orders,
                CAST(COALESCE(SUM(COALESCE(amount_to, amount)), 0) AS DOUBLE) AS volume
            FROM giftcard_orders
            GROUP BY product, currency
            ORDER BY orders DESC, volume DESC
            LIMIT 5
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| OpsDashboardTopGiftCard {
                product: row.get("product"),
                currency: row.try_get("currency").ok(),
                orders: int_field_to_u64(&row, "orders"),
                volume: float_field(&row, "volume"),
            })
            .collect())
    }

    fn dashboard_quick_access() -> Vec<OpsDashboardQuickAccessItem> {
        vec![
            OpsDashboardQuickAccessItem {
                key: "search".to_string(),
                label: "Global Search".to_string(),
                description: "Find by Assetar ID, provider ID, email, wallet, or tx hash."
                    .to_string(),
                path: "/ops/search".to_string(),
            },
            OpsDashboardQuickAccessItem {
                key: "giftcards".to_string(),
                label: "Gift Cards".to_string(),
                description: "Review queue state, retries, delivery, and reconcile failures."
                    .to_string(),
                path: "/giftcards/ops/orders".to_string(),
            },
            OpsDashboardQuickAccessItem {
                key: "swaps".to_string(),
                label: "Swaps".to_string(),
                description: "Inspect deposit status, provider timelines, and payout state."
                    .to_string(),
                path: "/swap/ops".to_string(),
            },
            OpsDashboardQuickAccessItem {
                key: "whatsapp".to_string(),
                label: "WhatsApp".to_string(),
                description: "Handle support conversations, assignments, and internal notes."
                    .to_string(),
                path: "/whatsapp/ops/conversations".to_string(),
            },
            OpsDashboardQuickAccessItem {
                key: "providers".to_string(),
                label: "Provider Health".to_string(),
                description: "Review failure hotspots, stale queues, and risk flags.".to_string(),
                path: "/ops/providers".to_string(),
            },
            OpsDashboardQuickAccessItem {
                key: "finance".to_string(),
                label: "Finance".to_string(),
                description: "Track completed volume, fees, and daily reporting slices."
                    .to_string(),
                path: "/ops/finance/summary".to_string(),
            },
        ]
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

    pub async fn list_assets(
        &self,
        query: &OpsAssetQuery,
    ) -> Result<OpsAssetListResponse, AdminError> {
        let limit = query.limit.unwrap_or(250).clamp(1, 500) as i64;
        let search = query
            .search
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let mut query_builder = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                symbol,
                name,
                network,
                requires_extra_id,
                extra_id_name,
                logo_url,
                CAST(min_amount AS DOUBLE) AS min_amount,
                CAST(max_amount AS DOUBLE) AS max_amount,
                is_active,
                last_synced_at
            FROM currencies
            WHERE 1 = 1
            "#,
        );

        if let Some(ticker) = query.ticker.as_deref() {
            query_builder
                .push(" AND symbol = ")
                .push_bind(ticker.trim().to_ascii_uppercase());
        }
        if let Some(network) = query.network.as_deref() {
            query_builder
                .push(" AND network = ")
                .push_bind(network.trim());
        }
        if let Some(memo_required) = query.memo_required {
            query_builder
                .push(" AND requires_extra_id = ")
                .push_bind(memo_required);
        }
        if query.active_only.unwrap_or(false) {
            query_builder.push(" AND is_active = TRUE");
        }
        if let Some(search) = search {
            let like = format!("%{}%", search);
            query_builder.push(" AND (symbol LIKE ");
            query_builder.push_bind(like.clone());
            query_builder.push(" OR name LIKE ");
            query_builder.push_bind(like.clone());
            query_builder.push(" OR network LIKE ");
            query_builder.push_bind(like);
            query_builder.push(")");
        }

        query_builder.push(" ORDER BY symbol ASC, network ASC LIMIT ");
        query_builder.push_bind(limit);

        let rows = query_builder
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsAssetListResponse {
            generated_at: Utc::now().to_rfc3339(),
            assets: rows.into_iter().map(map_asset_row).collect(),
        })
    }

    pub async fn get_asset_detail(
        &self,
        ticker: &str,
        query: &OpsAssetDetailQuery,
    ) -> Result<OpsAssetDetailResponse, AdminError> {
        let row = sqlx::query(
            r#"
            SELECT
                symbol,
                name,
                network,
                requires_extra_id,
                extra_id_name,
                logo_url,
                CAST(min_amount AS DOUBLE) AS min_amount,
                CAST(max_amount AS DOUBLE) AS max_amount,
                is_active,
                last_synced_at
            FROM currencies
            WHERE symbol = ? AND network = ?
            LIMIT 1
            "#,
        )
        .bind(ticker.trim().to_ascii_uppercase())
        .bind(query.network.trim())
        .fetch_optional(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .ok_or_else(|| {
            AdminError::NotFound(format!(
                "asset {} on {} was not found",
                ticker.trim(),
                query.network.trim()
            ))
        })?;

        let provider_count = count_query_pair(
            &self.db,
            r#"
            SELECT COUNT(DISTINCT pc.provider_id)
            FROM provider_currencies pc
            INNER JOIN currencies c ON c.id = pc.currency_id
            WHERE c.symbol = ? AND c.network = ?
            "#,
            ticker.trim(),
            query.network.trim(),
        )
        .await?;

        let source_pair_count = count_query_pair(
            &self.db,
            r#"
            SELECT COUNT(*)
            FROM trading_pairs tp
            INNER JOIN currencies c ON c.id = tp.from_currency_id
            WHERE c.symbol = ? AND c.network = ?
            "#,
            ticker.trim(),
            query.network.trim(),
        )
        .await?;

        let destination_pair_count = count_query_pair(
            &self.db,
            r#"
            SELECT COUNT(*)
            FROM trading_pairs tp
            INNER JOIN currencies c ON c.id = tp.to_currency_id
            WHERE c.symbol = ? AND c.network = ?
            "#,
            ticker.trim(),
            query.network.trim(),
        )
        .await?;

        Ok(OpsAssetDetailResponse {
            generated_at: Utc::now().to_rfc3339(),
            asset: map_asset_row(row),
            provider_count,
            source_pair_count,
            destination_pair_count,
        })
    }

    pub async fn list_giftcard_catalog(
        &self,
        query: &OpsGiftCardCatalogQuery,
    ) -> Result<OpsGiftCardCatalogResponse, AdminError> {
        let (source, cards) = self.fetch_catalog_cards(query.country.as_deref()).await?;
        let search = query
            .search
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let category = query
            .category
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let limit = query.limit.unwrap_or(500).clamp(1, 1000) as usize;

        let mut cards: Vec<GiftCardProductResponse> = cards
            .into_iter()
            .filter(|card| {
                let search_match = match search {
                    Some(search) => {
                        let lowered = search.to_ascii_lowercase();
                        card.name.to_ascii_lowercase().contains(&lowered)
                            || card.product_id.to_ascii_lowercase().contains(&lowered)
                            || card
                                .category
                                .as_deref()
                                .unwrap_or_default()
                                .to_ascii_lowercase()
                                .contains(&lowered)
                    }
                    None => true,
                };
                let category_match = match category {
                    Some(category) => card
                        .category
                        .as_deref()
                        .map(|value| value.eq_ignore_ascii_case(category))
                        .unwrap_or(false),
                    None => true,
                };
                search_match && category_match
            })
            .collect();

        cards.truncate(limit);

        Ok(OpsGiftCardCatalogResponse {
            generated_at: Utc::now().to_rfc3339(),
            country: query.country.clone(),
            source,
            cards,
        })
    }

    pub async fn get_giftcard_catalog_item(
        &self,
        product_id: &str,
        query: &OpsGiftCardCatalogDetailQuery,
    ) -> Result<OpsGiftCardCatalogDetailResponse, AdminError> {
        let (source, cards) = self.fetch_catalog_cards(query.country.as_deref()).await?;
        let product_id = product_id.trim();
        let card = cards
            .into_iter()
            .find(|card| card.product_id == product_id)
            .ok_or_else(|| {
                AdminError::NotFound(format!("gift card product {} was not found", product_id))
            })?;

        Ok(OpsGiftCardCatalogDetailResponse {
            generated_at: Utc::now().to_rfc3339(),
            source,
            card,
        })
    }

    pub async fn list_providers(
        &self,
        query: &OpsProviderListQuery,
    ) -> Result<OpsProviderListResponse, AdminError> {
        let limit = query.limit.unwrap_or(100).clamp(1, 250) as i64;
        let search = query
            .search
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let mut query_builder = QueryBuilder::<MySql>::new(
            r#"
            SELECT
                p.id,
                p.name,
                p.kyc_rating,
                CAST(p.insurance_percentage AS DOUBLE) AS insurance_percentage,
                p.markup_enabled,
                p.eta_minutes,
                p.is_active,
                p.last_synced_at,
                CAST(SUM(CASE WHEN s.status NOT IN ('completed','failed','refunded','expired') THEN 1 ELSE 0 END) AS SIGNED) AS open_swaps,
                CAST(SUM(CASE WHEN s.status = 'failed' AND s.updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY) THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps_24h,
                CAST(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps_30d,
                CAST(COALESCE(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN s.amount ELSE 0 END), 0) AS DOUBLE) AS volume_input_30d,
                CAST(COALESCE(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN s.platform_fee ELSE 0 END), 0) AS DOUBLE) AS platform_fees_30d,
                MAX(s.updated_at) AS last_activity_at
            FROM providers p
            LEFT JOIN swaps s ON s.provider_id = p.id
            WHERE 1 = 1
            "#,
        );

        if let Some(search) = search {
            let like = format!("%{}%", search);
            query_builder.push(" AND (p.id LIKE ");
            query_builder.push_bind(like.clone());
            query_builder.push(" OR p.name LIKE ");
            query_builder.push_bind(like);
            query_builder.push(")");
        }
        if let Some(rating) = query.rating.as_deref() {
            query_builder
                .push(" AND p.kyc_rating = ")
                .push_bind(rating.trim());
        }
        if let Some(markup_enabled) = query.markup_enabled {
            query_builder
                .push(" AND p.markup_enabled = ")
                .push_bind(markup_enabled);
        }
        if query.active_only.unwrap_or(false) {
            query_builder.push(" AND p.is_active = TRUE");
        }

        query_builder.push(
            " GROUP BY p.id, p.name, p.kyc_rating, p.insurance_percentage, p.markup_enabled, p.eta_minutes, p.is_active, p.last_synced_at",
        );
        query_builder.push(" ORDER BY failed_swaps_24h DESC, open_swaps DESC, p.name ASC LIMIT ");
        query_builder.push_bind(limit);

        let rows = query_builder
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsProviderListResponse {
            generated_at: Utc::now().to_rfc3339(),
            providers: rows.into_iter().map(map_provider_summary).collect(),
        })
    }

    pub async fn get_provider_detail(
        &self,
        provider_id: &str,
    ) -> Result<OpsProviderDetailResponse, AdminError> {
        let row = sqlx::query(
            r#"
            SELECT
                p.id,
                p.name,
                p.kyc_rating,
                CAST(p.insurance_percentage AS DOUBLE) AS insurance_percentage,
                p.markup_enabled,
                p.eta_minutes,
                p.is_active,
                p.last_synced_at,
                CAST(SUM(CASE WHEN s.status NOT IN ('completed','failed','refunded','expired') THEN 1 ELSE 0 END) AS SIGNED) AS open_swaps,
                CAST(SUM(CASE WHEN s.status = 'failed' AND s.updated_at >= (UTC_TIMESTAMP() - INTERVAL 1 DAY) THEN 1 ELSE 0 END) AS SIGNED) AS failed_swaps_24h,
                CAST(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS SIGNED) AS completed_swaps_30d,
                CAST(COALESCE(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN s.amount ELSE 0 END), 0) AS DOUBLE) AS volume_input_30d,
                CAST(COALESCE(SUM(CASE WHEN s.status = 'completed' AND s.created_at >= (UTC_TIMESTAMP() - INTERVAL 30 DAY) THEN s.platform_fee ELSE 0 END), 0) AS DOUBLE) AS platform_fees_30d,
                MAX(s.updated_at) AS last_activity_at
            FROM providers p
            LEFT JOIN swaps s ON s.provider_id = p.id
            WHERE p.id = ?
            GROUP BY p.id, p.name, p.kyc_rating, p.insurance_percentage, p.markup_enabled, p.eta_minutes, p.is_active, p.last_synced_at
            LIMIT 1
            "#,
        )
        .bind(provider_id.trim())
        .fetch_optional(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .ok_or_else(|| {
            AdminError::NotFound(format!("provider {} was not found", provider_id.trim()))
        })?;

        let pair_rows = sqlx::query(
            r#"
            SELECT
                from_currency,
                from_network,
                to_currency,
                to_network,
                CAST(COUNT(*) AS SIGNED) AS trades,
                CAST(COALESCE(SUM(amount), 0) AS DOUBLE) AS volume_input
            FROM swaps
            WHERE provider_id = ?
            GROUP BY from_currency, from_network, to_currency, to_network
            ORDER BY trades DESC, volume_input DESC
            LIMIT 10
            "#,
        )
        .bind(provider_id.trim())
        .fetch_all(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsProviderDetailResponse {
            generated_at: Utc::now().to_rfc3339(),
            provider: map_provider_summary(row),
            top_pairs: pair_rows
                .into_iter()
                .map(|row| OpsDashboardTopPair {
                    from_currency: row.get("from_currency"),
                    from_network: row.get("from_network"),
                    to_currency: row.get("to_currency"),
                    to_network: row.get("to_network"),
                    trades: int_field_to_u64(&row, "trades"),
                    volume_input: float_field(&row, "volume_input"),
                })
                .collect(),
        })
    }

    pub async fn settings(&self) -> Result<OpsSettingsResponse, AdminError> {
        let public_base_url = public_base_url();
        let webhook_key_configured = env_present("TROCADOR_WEBHOOK_KEY");
        let trocador_webhook_enabled = std::env::var("TROCADOR_WEBHOOK_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true);
        let payout_policy = PayoutPolicyConfig::from_env();
        let swap_markup = swap_markup_from_env().map_err(AdminError::Config)?;

        Ok(OpsSettingsResponse {
            generated_at: Utc::now().to_rfc3339(),
            admin_email: self.account.email.clone(),
            trocador_api_key_configured: env_present("TROCADOR_API_KEY"),
            trocador_webhook_enabled,
            trocador_webhook_key_configured: webhook_key_configured,
            public_base_url: public_base_url.clone(),
            swap_webhook_url: swap_webhook_url(),
            giftcard_webhook_url: giftcard_webhook_url(),
            swap_markup,
            allowed_swap_markups: vec!["0", "1", "1.65", "3"]
                .into_iter()
                .map(String::from)
                .collect(),
            allowed_card_markups: vec!["1", "2", "3"].into_iter().map(String::from).collect(),
            payout_policy: OpsPayoutPolicySettings {
                local_certified_chains: payout_policy.local_certified_chain_keys(),
                trocador_only_chains: payout_policy.trocador_only_chain_keys(),
            },
        })
    }

    pub async fn settings_diagnostics(&self) -> Result<OpsSettingsDiagnosticsResponse, AdminError> {
        let mut errors = Vec::new();
        let mut api_key_valid = false;
        let mut providers_fetch_ok = false;
        let mut currencies_fetch_ok = false;
        let mut giftcards_fetch_ok = false;

        match TrocadorGateway::from_env() {
            Ok(gateway) => {
                match gateway.fetch_providers().await {
                    Ok(_) => providers_fetch_ok = true,
                    Err(error) => errors.push(format!("providers_fetch_failed: {}", error)),
                }

                match gateway.fetch_currencies().await {
                    Ok(_) => currencies_fetch_ok = true,
                    Err(error) => errors.push(format!("currencies_fetch_failed: {}", error)),
                }

                match gateway.fetch_giftcards(None).await {
                    Ok(_) => giftcards_fetch_ok = true,
                    Err(error) => errors.push(format!("giftcards_fetch_failed: {}", error)),
                }

                api_key_valid = providers_fetch_ok || currencies_fetch_ok || giftcards_fetch_ok;
            }
            Err(_) => {
                errors.push("TROCADOR_API_KEY is not configured".to_string());
            }
        }

        let webhook_base_url_present = public_base_url().is_some();
        let webhook_key_configured = env_present("TROCADOR_WEBHOOK_KEY");

        Ok(OpsSettingsDiagnosticsResponse {
            generated_at: Utc::now().to_rfc3339(),
            api_key_valid,
            providers_fetch_ok,
            currencies_fetch_ok,
            giftcards_fetch_ok,
            webhook_base_url_present,
            swap_webhook_config_complete: swap_webhook_url().is_some(),
            giftcard_webhook_config_complete: webhook_base_url_present && webhook_key_configured,
            errors,
        })
    }

    pub async fn webhook_monitor(
        &self,
        query: &OpsWebhookQuery,
    ) -> Result<OpsWebhookMonitorResponse, AdminError> {
        let include_delivered = query.include_delivered.unwrap_or(true);
        let limit = query.limit.unwrap_or(100).clamp(1, 500) as i64;

        let mut query_builder = QueryBuilder::<MySql>::new(
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
            WHERE 1 = 1
            "#,
        );

        if !include_delivered {
            query_builder.push(" AND (delivered_at IS NULL OR is_dlq = TRUE)");
        }
        if let Some(swap_id) = query.swap_id.as_deref() {
            query_builder
                .push(" AND swap_id = ")
                .push_bind(swap_id.trim());
        }
        if let Some(event_type) = query.event_type.as_deref() {
            query_builder
                .push(" AND event_type = ")
                .push_bind(event_type.trim());
        }

        query_builder.push(" ORDER BY created_at DESC LIMIT ");
        query_builder.push_bind(limit);

        let rows = query_builder
            .build()
            .fetch_all(&self.db)
            .await
            .map_err(|error| AdminError::Database(error.to_string()))?;

        Ok(OpsWebhookMonitorResponse {
            deliveries: rows.into_iter().map(map_webhook_delivery_row).collect(),
        })
    }

    pub async fn webhook_detail(
        &self,
        delivery_id: &str,
    ) -> Result<OpsWebhookDetailResponse, AdminError> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                webhook_id,
                swap_id,
                event_type,
                attempt_number,
                max_attempts,
                next_retry_at,
                delivered_at,
                response_status,
                response_body,
                response_time_ms,
                error_message,
                is_dlq,
                signature,
                CAST(payload AS CHAR) AS payload_json,
                created_at,
                updated_at
            FROM webhook_deliveries
            WHERE id = ?
            LIMIT 1
            "#,
        )
        .bind(delivery_id.trim())
        .fetch_optional(&self.db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?
        .ok_or_else(|| {
            AdminError::NotFound(format!(
                "webhook delivery {} was not found",
                delivery_id.trim()
            ))
        })?;

        let payload = row
            .try_get::<Option<String>, _>("payload_json")
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<JsonValue>(&raw).ok())
            .unwrap_or(JsonValue::Null);

        Ok(OpsWebhookDetailResponse {
            delivery: OpsWebhookDeliveryRow {
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
            },
            webhook_id: row.get("webhook_id"),
            signature: row.get("signature"),
            payload,
            response_body: row.try_get("response_body").ok(),
        })
    }

    async fn fetch_catalog_cards(
        &self,
        country: Option<&str>,
    ) -> Result<(String, Vec<GiftCardProductResponse>), AdminError> {
        let gateway = TrocadorGateway::from_env()
            .map_err(|_| AdminError::Config("TROCADOR_API_KEY not set".to_string()))?;
        let fallback_cards = fallback_catalog(country);
        let mut last_error = None;

        let country_values = country
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_default();

        if country_values.is_empty() {
            match gateway.fetch_giftcards(None).await {
                Ok(cards) if !cards.is_empty() => {
                    return Ok((
                        "trocador".to_string(),
                        cards.into_iter().map(Into::into).collect(),
                    ));
                }
                Ok(_) => {}
                Err(error) => last_error = Some(error.to_string()),
            }
        } else {
            for country_value in country_values {
                match gateway.fetch_giftcards(Some(country_value.as_str())).await {
                    Ok(cards) if !cards.is_empty() => {
                        return Ok((
                            "trocador".to_string(),
                            cards.into_iter().map(Into::into).collect(),
                        ));
                    }
                    Ok(_) => {}
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
        }

        if !fallback_cards.is_empty() {
            return Ok(("fallback".to_string(), fallback_cards));
        }

        Err(AdminError::External(last_error.unwrap_or_else(|| {
            "gift card catalog fetch returned no data".to_string()
        })))
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

fn map_asset_row(row: sqlx::mysql::MySqlRow) -> OpsAssetRow {
    OpsAssetRow {
        ticker: row.get("symbol"),
        name: row.get("name"),
        network: row.get("network"),
        memo_required: row.get::<bool, _>("requires_extra_id"),
        extra_id_name: row.try_get("extra_id_name").ok(),
        image: row.try_get("logo_url").ok(),
        minimum: row.try_get("min_amount").ok(),
        maximum: row.try_get("max_amount").ok(),
        is_active: row.get::<bool, _>("is_active"),
        last_synced_at: row
            .try_get::<Option<NaiveDateTime>, _>("last_synced_at")
            .ok()
            .flatten()
            .map(format_datetime),
    }
}

fn map_provider_summary(row: sqlx::mysql::MySqlRow) -> OpsProviderSummary {
    OpsProviderSummary {
        id: row.get("id"),
        name: row.get("name"),
        kyc_rating: row.get("kyc_rating"),
        insurance_percentage: row.try_get("insurance_percentage").ok(),
        markup_enabled: row.get::<bool, _>("markup_enabled"),
        eta_minutes: row.try_get("eta_minutes").ok(),
        is_active: row.get::<bool, _>("is_active"),
        last_synced_at: row
            .try_get::<Option<NaiveDateTime>, _>("last_synced_at")
            .ok()
            .flatten()
            .map(format_datetime),
        open_swaps: int_field_to_u64(&row, "open_swaps"),
        failed_swaps_24h: int_field_to_u64(&row, "failed_swaps_24h"),
        completed_swaps_30d: int_field_to_u64(&row, "completed_swaps_30d"),
        volume_input_30d: float_field(&row, "volume_input_30d"),
        platform_fees_30d: float_field(&row, "platform_fees_30d"),
        last_activity_at: row
            .try_get::<Option<NaiveDateTime>, _>("last_activity_at")
            .ok()
            .flatten()
            .map(format_datetime),
    }
}

fn map_webhook_delivery_row(row: sqlx::mysql::MySqlRow) -> OpsWebhookDeliveryRow {
    OpsWebhookDeliveryRow {
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
    }
}

async fn count_query(db: &DbPool, sql: &str) -> Result<u64, AdminError> {
    let count = sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(db)
        .await
        .map_err(|error| AdminError::Database(error.to_string()))?;
    Ok(count.max(0) as u64)
}

async fn count_query_pair(
    db: &DbPool,
    sql: &str,
    first: &str,
    second: &str,
) -> Result<u64, AdminError> {
    let count = sqlx::query_scalar::<_, i64>(sql)
        .bind(first)
        .bind(second)
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

fn env_present(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn public_base_url() -> Option<String> {
    std::env::var("PUBLIC_BACKEND_URL")
        .ok()
        .or_else(|| std::env::var("RENDER_EXTERNAL_URL").ok())
        .or_else(|| std::env::var("API_BASE_URL").ok())
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

fn swap_webhook_url() -> Option<String> {
    let enabled = std::env::var("TROCADOR_WEBHOOK_ENABLED")
        .ok()
        .and_then(|value| value.parse::<bool>().ok())
        .unwrap_or(true);

    if !enabled || !env_present("TROCADOR_WEBHOOK_KEY") {
        return None;
    }

    public_base_url().map(|base| format!("{}/swap/webhooks/trocador", base))
}

fn giftcard_webhook_url() -> Option<String> {
    if !env_present("TROCADOR_WEBHOOK_KEY") {
        return None;
    }

    public_base_url().map(|base| format!("{}/giftcards/webhooks/trocador", base))
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
