use chrono::{DateTime, Utc};
use csv::Writer;
use serde::Serialize;
use sqlx::{MySql, QueryBuilder, Row};

use super::{
    model::AdminAccount,
    schema::{
        AdminLoginResponse, AdminOverviewResponse, AdminOverviewSwapMetrics,
        AdminOverviewWhatsAppMetrics, AdminSwapExportQuery, AdminUserResponse,
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
