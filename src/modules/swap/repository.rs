use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, Row};

use super::crud::SwapError;
use super::model::Provider;
use super::schema::{
    FiltersApplied, HistoryCursor, HistoryQuery, HistoryResponse, PaginationInfo, PairResponse,
    PairsPaginationInfo, PairsQuery, PairsResponse, ProvidersQuery, RateType, SwapStatus,
    SwapSummary, TrocadorCurrency, TrocadorProvider,
};
use crate::services::wallet::validation::default_extra_id_name;

pub struct SwapRepository {
    pool: Pool<MySql>,
}

pub struct NewSwapRecord<'a> {
    pub id: &'a str,
    pub user_id: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub provider_id: &'a str,
    pub provider_swap_id: &'a str,
    pub from_currency: &'a str,
    pub from_network: &'a str,
    pub to_currency: &'a str,
    pub to_network: &'a str,
    pub amount: f64,
    pub estimated_receive: f64,
    pub rate: f64,
    pub network_fee: f64,
    pub deposit_address: &'a str,
    pub deposit_extra_id: Option<&'a str>,
    pub recipient_address: &'a str,
    pub recipient_extra_id: Option<&'a str>,
    pub refund_address: Option<&'a str>,
    pub refund_extra_id: Option<&'a str>,
    pub platform_fee: f64,
    pub total_fee: f64,
    pub status: SwapStatus,
    pub rate_type: RateType,
    pub is_sandbox: bool,
    pub is_payment: bool,
    pub expires_at: DateTime<Utc>,
}

pub struct SwapStatusRecord {
    pub id: String,
    pub user_id: Option<String>,
    pub client_id: Option<String>,
    pub provider_id: String,
    pub provider_swap_id: Option<String>,
    pub from_currency: String,
    pub from_network: String,
    pub to_currency: String,
    pub to_network: String,
    pub amount: f64,
    pub estimated_receive: f64,
    pub actual_receive: Option<f64>,
    pub rate: f64,
    pub network_fee: f64,
    pub provider_fee: f64,
    pub platform_fee: f64,
    pub total_fee: f64,
    pub deposit_address: String,
    pub deposit_extra_id: Option<String>,
    pub recipient_address: String,
    pub recipient_extra_id: Option<String>,
    pub refund_address: Option<String>,
    pub refund_extra_id: Option<String>,
    pub tx_hash_in: Option<String>,
    pub tx_hash_out: Option<String>,
    pub status: SwapStatus,
    pub rate_type: RateType,
    pub is_sandbox: i8,
    pub error: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SwapRepository {
    pub fn new(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    pub async fn get_expected_trocador_amount(&self, swap_id: &str) -> Result<f64, SwapError> {
        let swap = sqlx::query!(
            r#"
            SELECT CAST(estimated_receive AS DOUBLE) as "estimated_receive!: f64",
                   CAST(network_fee AS DOUBLE) as "network_fee!: f64",
                   CAST(platform_fee AS DOUBLE) as "platform_fee!: f64"
            FROM swaps WHERE id = ?
            "#,
            swap_id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(swap.estimated_receive + swap.platform_fee + swap.network_fee)
    }

    pub async fn get_latest_currency_sync(&self) -> Result<Option<DateTime<Utc>>, SwapError> {
        let result = sqlx::query_scalar!("SELECT MAX(last_synced_at) FROM currencies")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(result.flatten())
    }

    pub async fn upsert_currencies_batch(
        &self,
        currencies: &[TrocadorCurrency],
    ) -> Result<(), SwapError> {
        if currencies.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO currencies (
                symbol, name, network, is_active, logo_url,
                requires_extra_id, extra_id_name, min_amount, max_amount, last_synced_at
            ) ",
        );

        query_builder.push_values(currencies, |mut b, currency| {
            let extra_id_name =
                default_extra_id_name(&currency.ticker, &currency.network, currency.memo);
            b.push_bind(&currency.ticker)
                .push_bind(&currency.name)
                .push_bind(&currency.network)
                .push("TRUE")
                .push_bind(&currency.image)
                .push_bind(currency.memo)
                .push_bind(extra_id_name)
                .push_bind(currency.minimum)
                .push_bind(currency.maximum)
                .push("NOW()");
        });

        query_builder.push(
            " ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                logo_url = VALUES(logo_url),
                requires_extra_id = VALUES(requires_extra_id),
                extra_id_name = VALUES(extra_id_name),
                min_amount = VALUES(min_amount),
                max_amount = VALUES(max_amount),
                last_synced_at = VALUES(last_synced_at)",
        );

        query_builder
            .build()
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn get_latest_provider_sync(&self) -> Result<Option<DateTime<Utc>>, SwapError> {
        let result = sqlx::query_scalar!("SELECT MAX(last_synced_at) FROM providers")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(result.flatten())
    }

    pub async fn upsert_provider_from_trocador(
        &self,
        trocador_provider: &TrocadorProvider,
    ) -> Result<(), SwapError> {
        let id = Self::normalize_provider_id(&trocador_provider.name);
        let slug = trocador_provider.name.to_lowercase().replace(" ", "-");

        let existing = sqlx::query_scalar!(
            "SELECT id FROM providers WHERE LOWER(name) = LOWER(?) LIMIT 1",
            trocador_provider.name
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        if let Some(existing_id) = existing {
            sqlx::query!(
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
                "#,
                trocador_provider.name,
                slug,
                trocador_provider.rating,
                trocador_provider.insurance,
                trocador_provider.eta as i32,
                trocador_provider.enabled_markup,
                existing_id
            )
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        } else {
            sqlx::query!(
                r#"
                INSERT INTO providers (
                    id, name, slug, is_active, kyc_rating,
                    insurance_percentage, eta_minutes, markup_enabled, last_synced_at
                )
                VALUES (?, ?, ?, TRUE, ?, ?, ?, ?, NOW())
                "#,
                id,
                trocador_provider.name,
                slug,
                trocador_provider.rating,
                trocador_provider.insurance,
                trocador_provider.eta as i32,
                trocador_provider.enabled_markup
            )
            .execute(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        }

        Ok(())
    }

    pub async fn get_providers(&self, query: ProvidersQuery) -> Result<Vec<Provider>, SwapError> {
        let mut sql = String::from(
            "SELECT id, name, slug, is_active, kyc_rating, insurance_percentage,
             eta_minutes, markup_enabled, api_url, logo_url, website_url,
             last_synced_at, created_at, updated_at
             FROM providers
             WHERE is_active = TRUE",
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

        match query.sort.as_deref() {
            Some("name") => sql.push_str(" ORDER BY name ASC"),
            Some("rating") => sql.push_str(" ORDER BY kyc_rating ASC, name ASC"),
            Some("eta") => sql.push_str(" ORDER BY eta_minutes ASC"),
            _ => sql.push_str(" ORDER BY name ASC"),
        }

        sqlx::query_as::<_, Provider>(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))
    }

    pub async fn get_pairs(&self, query: PairsQuery) -> Result<PairsResponse, SwapError> {
        let mut count_sql = String::from(
            "SELECT COUNT(DISTINCT tp.id)
             FROM trading_pairs tp
             INNER JOIN currencies c1 ON tp.from_currency_id = c1.id
             INNER JOIN currencies c2 ON tp.to_currency_id = c2.id
             WHERE 1=1",
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
             WHERE 1=1",
        );

        let mut conditions = Vec::new();

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

        match query.status.as_deref() {
            Some("active") => conditions.push("tp.is_active = TRUE".to_string()),
            Some("disabled") => conditions.push("tp.is_active = FALSE".to_string()),
            Some("all") | None => {}
            _ => {}
        }

        if !conditions.is_empty() {
            let condition_str = format!(" AND {}", conditions.join(" AND "));
            count_sql.push_str(&condition_str);
            data_sql.push_str(&condition_str);
        }

        let total: (i64,) = sqlx::query_as(&count_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;
        let total_elements = total.0;

        let order_clause = match query.order_by.as_deref() {
            Some(order) if order.contains("name") => {
                if order.contains("desc") {
                    " ORDER BY c1.symbol DESC, c2.symbol DESC"
                } else {
                    " ORDER BY c1.symbol ASC, c2.symbol ASC"
                }
            }
            Some(order) if order.contains("updated") => {
                if order.contains("desc") {
                    " ORDER BY tp.updated_at DESC"
                } else {
                    " ORDER BY tp.updated_at ASC"
                }
            }
            _ => " ORDER BY c1.symbol ASC, c2.symbol ASC",
        };
        data_sql.push_str(order_clause);

        let offset = query.page * query.size;
        data_sql.push_str(&format!(" LIMIT {} OFFSET {}", query.size, offset));

        let rows: Vec<(
            i64,
            String,
            String,
            String,
            String,
            bool,
            Option<f64>,
            Option<f64>,
            DateTime<Utc>,
        )> = sqlx::query_as(&data_sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        let pairs: Vec<PairResponse> = rows
            .into_iter()
            .map(|row| PairResponse {
                name: format!("{}/{}", row.1, row.3),
                base_currency: row.1,
                base_network: row.2,
                quote_currency: row.3,
                quote_network: row.4,
                status: if row.5 {
                    "active".to_string()
                } else {
                    "disabled".to_string()
                },
                min_amount: row.6,
                max_amount: row.7,
                last_updated: row.8,
            })
            .collect();

        let total_pages = ((total_elements as f64) / (query.size as f64)).ceil() as u32;
        let has_next = query.page + 1 < total_pages;
        let has_prev = query.page > 0;

        Ok(PairsResponse {
            pairs,
            pagination: PairsPaginationInfo {
                page: query.page,
                size: query.size,
                total_elements,
                total_pages,
                has_next,
                has_prev,
            },
        })
    }

    pub async fn ensure_provider_exists(&self, provider_name: &str) -> Result<String, SwapError> {
        let normalized_provider_id = Self::normalize_provider_id(provider_name);
        let provider_exists = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM providers WHERE id = ?",
            normalized_provider_id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        if provider_exists == 0 {
            tracing::warn!(
                "Provider '{}' not found in database, auto-inserting",
                normalized_provider_id
            );
            sqlx::query!(
                r#"
                INSERT INTO providers (id, name, slug, is_active, kyc_rating, insurance_percentage, eta_minutes, markup_enabled)
                VALUES (?, ?, ?, TRUE, 'C', 0.015, 10, FALSE)
                ON DUPLICATE KEY UPDATE id = id
                "#,
                normalized_provider_id,
                provider_name,
                normalized_provider_id
            )
            .execute(&self.pool)
            .await
            .map_err(|e| {
                SwapError::DatabaseError(format!("Failed to auto-insert provider: {}", e))
            })?;
        }

        Ok(normalized_provider_id)
    }

    pub async fn insert_swap(&self, record: NewSwapRecord<'_>) -> Result<(), SwapError> {
        sqlx::query(
            r#"
            INSERT INTO swaps (
                id, user_id, client_id, provider_id, provider_swap_id,
                from_currency, from_network, to_currency, to_network,
                amount, estimated_receive, rate, network_fee,
                deposit_address, deposit_extra_id,
                recipient_address, recipient_extra_id,
                refund_address, refund_extra_id,
                platform_fee, total_fee,
                status, rate_type, is_sandbox, is_payment,
                expires_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(record.id)
        .bind(record.user_id)
        .bind(record.client_id)
        .bind(record.provider_id)
        .bind(record.provider_swap_id)
        .bind(record.from_currency)
        .bind(record.from_network)
        .bind(record.to_currency)
        .bind(record.to_network)
        .bind(record.amount)
        .bind(record.estimated_receive)
        .bind(record.rate)
        .bind(record.network_fee)
        .bind(record.deposit_address)
        .bind(record.deposit_extra_id)
        .bind(record.recipient_address)
        .bind(record.recipient_extra_id)
        .bind(record.refund_address)
        .bind(record.refund_extra_id)
        .bind(record.platform_fee)
        .bind(record.total_fee)
        .bind(record.status.as_str())
        .bind(record.rate_type.as_db_str())
        .bind(record.is_sandbox)
        .bind(record.is_payment)
        .bind(record.expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn get_swap_status_record(
        &self,
        swap_id: &str,
    ) -> Result<Option<SwapStatusRecord>, SwapError> {
        let row = sqlx::query(
            r#"
            SELECT id, user_id, provider_id, provider_swap_id,
                   client_id,
                   from_currency, from_network, to_currency, to_network,
                   CAST(amount AS DOUBLE) AS amount,
                   CAST(estimated_receive AS DOUBLE) AS estimated_receive,
                   CAST(actual_receive AS DOUBLE) AS actual_receive,
                   CAST(rate AS DOUBLE) AS rate,
                   CAST(network_fee AS DOUBLE) AS network_fee,
                   CAST(COALESCE(provider_fee, 0) AS DOUBLE) AS provider_fee,
                   CAST(platform_fee AS DOUBLE) AS platform_fee,
                   CAST(total_fee AS DOUBLE) AS total_fee,
                   deposit_address, deposit_extra_id,
                   recipient_address, recipient_extra_id,
                   refund_address, refund_extra_id,
                   tx_hash_in, tx_hash_out,
                   status,
                   rate_type,
                   is_sandbox, error,
                   expires_at, completed_at, created_at, updated_at
            FROM swaps
            WHERE id = ?
            "#,
        )
        .bind(swap_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(row.map(|swap| SwapStatusRecord {
            id: swap.get("id"),
            user_id: swap.get("user_id"),
            client_id: swap.get("client_id"),
            provider_id: swap.get("provider_id"),
            provider_swap_id: swap.get("provider_swap_id"),
            from_currency: swap.get("from_currency"),
            from_network: swap.get("from_network"),
            to_currency: swap.get("to_currency"),
            to_network: swap.get("to_network"),
            amount: swap.get("amount"),
            estimated_receive: swap.get("estimated_receive"),
            actual_receive: swap.try_get("actual_receive").ok(),
            rate: swap.get("rate"),
            network_fee: swap.get("network_fee"),
            provider_fee: swap.get("provider_fee"),
            platform_fee: swap.get("platform_fee"),
            total_fee: swap.get("total_fee"),
            deposit_address: swap.get("deposit_address"),
            deposit_extra_id: swap.get("deposit_extra_id"),
            recipient_address: swap.get("recipient_address"),
            recipient_extra_id: swap.get("recipient_extra_id"),
            refund_address: swap.get("refund_address"),
            refund_extra_id: swap.get("refund_extra_id"),
            tx_hash_in: swap.get("tx_hash_in"),
            tx_hash_out: swap.get("tx_hash_out"),
            status: SwapStatus::from_persisted(&swap.get::<String, _>("status"))
                .unwrap_or(SwapStatus::Waiting),
            rate_type: match swap.get::<String, _>("rate_type").as_str() {
                "fixed" => RateType::Fixed,
                _ => RateType::Floating,
            },
            is_sandbox: swap.get("is_sandbox"),
            error: swap.get("error"),
            expires_at: swap.get("expires_at"),
            completed_at: swap.get("completed_at"),
            created_at: swap.get("created_at"),
            updated_at: swap.get("updated_at"),
        }))
    }

    pub async fn update_swap_status(
        &self,
        swap_id: &str,
        status: &SwapStatus,
        actual_receive: f64,
        tx_hash_in: Option<String>,
        tx_hash_out: Option<String>,
    ) -> Result<(), SwapError> {
        let completed_at = if *status == SwapStatus::Completed {
            Some(Utc::now())
        } else {
            None
        };

        sqlx::query!(
            r#"
            UPDATE swaps
            SET status = ?,
                actual_receive = ?,
                tx_hash_in = COALESCE(?, tx_hash_in),
                tx_hash_out = COALESCE(?, tx_hash_out),
                completed_at = COALESCE(?, completed_at),
                updated_at = NOW()
            WHERE id = ?
            "#,
            status,
            actual_receive,
            tx_hash_in,
            tx_hash_out,
            completed_at,
            swap_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn log_status_change(
        &self,
        swap_id: &str,
        status: &SwapStatus,
        message: Option<String>,
    ) -> Result<(), SwapError> {
        sqlx::query!(
            r#"
            INSERT INTO swap_status_history (swap_id, status, message, created_at)
            VALUES (?, ?, ?, NOW())
            "#,
            swap_id,
            status,
            message
        )
        .execute(&self.pool)
        .await
        .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    pub async fn get_swap_history(
        &self,
        user_id: &str,
        query: HistoryQuery,
    ) -> Result<HistoryResponse, SwapError> {
        self.get_swap_history_for_scope("user_id = ?", user_id, query)
            .await
    }

    pub async fn get_swap_history_for_client(
        &self,
        client_id: &str,
        query: HistoryQuery,
    ) -> Result<HistoryResponse, SwapError> {
        self.get_swap_history_for_scope("client_id = ? AND user_id IS NULL", client_id, query)
            .await
    }

    async fn get_swap_history_for_scope(
        &self,
        scope_clause: &str,
        scope_value: &str,
        query: HistoryQuery,
    ) -> Result<HistoryResponse, SwapError> {
        let cursor = if let Some(cursor_str) = &query.cursor {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor_str)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid base64: {}", e)))?;
            let json = String::from_utf8(bytes)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid UTF-8: {}", e)))?;
            let cursor: HistoryCursor = serde_json::from_str(&json)
                .map_err(|e| SwapError::InvalidCursor(format!("Invalid JSON: {}", e)))?;
            Some(cursor)
        } else {
            None
        };

        let limit = query.limit.min(100).max(1);

        let date_from = query
            .date_from
            .as_ref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let date_to = query
            .date_to
            .as_ref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

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
            WHERE user_id = ?",
        );

        sql = sql.replace("WHERE user_id = ?", &format!("WHERE {}", scope_clause));

        let mut bind_values: Vec<String> = vec![scope_value.to_string()];

        if let Some(ref cursor) = cursor {
            sql.push_str(" AND (created_at, id) < (?, ?)");
            bind_values.push(cursor.created_at.to_rfc3339());
            bind_values.push(cursor.id.clone());
        }

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

        let sort_by = query.sort_by.as_deref().unwrap_or("created_at");
        let sort_order = query.sort_order.as_deref().unwrap_or("desc").to_uppercase();
        sql.push_str(&format!(
            " ORDER BY {} {}, id {}",
            sort_by, sort_order, sort_order
        ));
        sql.push_str(&format!(" LIMIT {}", limit + 1));

        let mut query_builder = sqlx::query(&sql);
        for value in &bind_values {
            query_builder = query_builder.bind(value);
        }

        let rows = query_builder
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwapError::DatabaseError(e.to_string()))?;

        let has_more = rows.len() > limit as usize;
        let swaps_data = if has_more {
            &rows[..limit as usize]
        } else {
            &rows[..]
        };

        let swaps: Vec<SwapSummary> = swaps_data
            .iter()
            .map(|row| {
                use sqlx::Row;

                let status_str: String = row.get("status");
                let status = SwapStatus::from_persisted(&status_str).unwrap_or(SwapStatus::Waiting);

                let rate_type_str: String = row.get("rate_type");
                let rate_type = match rate_type_str.as_str() {
                    "fixed" => RateType::Fixed,
                    _ => RateType::Floating,
                };

                SwapSummary {
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
            })
            .collect();

        let next_cursor = if has_more && !swaps.is_empty() {
            let last = &swaps[swaps.len() - 1];
            let cursor_obj = HistoryCursor {
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

        Ok(HistoryResponse {
            swaps,
            pagination: PaginationInfo {
                limit,
                has_more,
                next_cursor,
            },
            filters_applied: FiltersApplied {
                status: query.status,
                from_currency: query.from_currency,
                to_currency: query.to_currency,
                provider: query.provider,
                date_from: query.date_from,
                date_to: query.date_to,
            },
        })
    }

    fn normalize_provider_id(provider_name: &str) -> String {
        provider_name
            .to_lowercase()
            .replace(" ", "")
            .replace("-", "")
    }
}
