use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::{pool::PoolConnection, Error, MySql, QueryBuilder, Row};
use uuid::Uuid;

use crate::config::DbPool;
use crate::modules::whatsapp::schema::AdminConversationQuery;
use crate::services::whatsapp::NormalizedWebhookEvent;
use crate::services::whatsapp::{
    build_stored_event_payload, extract_message_text_from_payload, redact_outbound_body,
    redact_text_preview,
};

pub struct SessionRecord {
    pub id: String,
    pub locale: String,
    pub state: String,
    pub draft_json: Option<String>,
}

pub struct AdminConversationRecord {
    pub wa_id: String,
    pub phone_number_id: String,
    pub locale: String,
    pub state: String,
    pub admin_status: String,
    pub admin_tag: Option<String>,
    pub assigned_to: Option<String>,
    pub internal_note: Option<String>,
    pub last_inbound_at: Option<DateTime<Utc>>,
    pub last_outbound_at: Option<DateTime<Utc>>,
    pub last_message_preview: Option<String>,
    pub last_outbound_status: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

pub struct AdminConversationEventRecord {
    pub id: String,
    pub event_kind: String,
    pub message_type: Option<String>,
    pub provider_message_id: Option<String>,
    pub text: Option<String>,
    pub processed: i32,
    pub attempt_count: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

pub struct AdminOutboundMessageRecord {
    pub id: String,
    pub message_kind: String,
    pub status: String,
    pub provider_message_id: Option<String>,
    pub body: String,
    pub error_message: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

pub struct WhatsAppCrud {
    pool: DbPool,
}

pub struct QueuedMessageEvent {
    pub id: String,
    pub phone_number_id: String,
    pub wa_id: String,
    pub provider_message_id: Option<String>,
    pub text: Option<String>,
    pub attempt_count: i32,
}

pub struct WhatsAppSessionLock {
    key: String,
    connection: Option<PoolConnection<MySql>>,
}

impl WhatsAppSessionLock {
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

impl WhatsAppCrud {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn acquire_session_lock(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        timeout_seconds: i32,
    ) -> Result<WhatsAppSessionLock, Error> {
        let mut connection = self.pool.acquire().await?;
        let key = format!("whatsapp:{}:{}", phone_number_id, wa_id);

        let acquired = sqlx::query_scalar::<_, Option<i64>>("SELECT GET_LOCK(?, ?)")
            .bind(&key)
            .bind(timeout_seconds)
            .fetch_one(&mut *connection)
            .await?;

        match acquired {
            Some(1) => Ok(WhatsAppSessionLock {
                key,
                connection: Some(connection),
            }),
            _ => Err(Error::Protocol(
                format!("failed to acquire WhatsApp session lock for {}", key).into(),
            )),
        }
    }

    pub async fn touch_session(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        last_inbound_message_id: Option<&str>,
    ) -> Result<(), Error> {
        let now = Utc::now().naive_utc();
        let id = Uuid::new_v4().to_string();

        sqlx::query(
            r#"
            INSERT INTO whatsapp_sessions (
                id, wa_id, phone_number_id, state, last_inbound_message_id, last_inbound_at
            )
            VALUES (?, ?, ?, 'idle', ?, ?)
            ON DUPLICATE KEY UPDATE
                last_inbound_message_id = COALESCE(VALUES(last_inbound_message_id), last_inbound_message_id),
                last_inbound_at = VALUES(last_inbound_at),
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(id)
        .bind(wa_id)
        .bind(phone_number_id)
        .bind(last_inbound_message_id)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_session(
        &self,
        wa_id: &str,
        phone_number_id: &str,
    ) -> Result<Option<SessionRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT id, locale, state, CAST(draft AS CHAR) AS draft_json
            FROM whatsapp_sessions
            WHERE wa_id = ? AND phone_number_id = ?
            LIMIT 1
            "#,
        )
        .bind(wa_id)
        .bind(phone_number_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| SessionRecord {
            id: row.get("id"),
            locale: row.get("locale"),
            state: row.get("state"),
            draft_json: row.get("draft_json"),
        }))
    }

    pub async fn list_admin_conversations(
        &self,
        query: &AdminConversationQuery,
    ) -> Result<(Vec<AdminConversationRecord>, u64), Error> {
        let page = query.page.max(1);
        let limit = query.limit.clamp(1, 100);
        let offset = ((page - 1) * limit) as i64;

        let mut where_clauses = Vec::new();
        let mut bind_values = Vec::new();

        if let Some(admin_status) = query.admin_status.as_deref() {
            where_clauses.push("s.admin_status = ?");
            bind_values.push(admin_status.trim().to_string());
        }
        if let Some(admin_tag) = query.admin_tag.as_deref() {
            where_clauses.push("s.admin_tag = ?");
            bind_values.push(admin_tag.trim().to_string());
        }
        if let Some(assigned_to) = query.assigned_to.as_deref() {
            where_clauses.push("s.assigned_to = ?");
            bind_values.push(assigned_to.trim().to_string());
        }
        if let Some(state) = query.state.as_deref() {
            where_clauses.push("s.state = ?");
            bind_values.push(state.trim().to_string());
        }
        if let Some(wa_id) = query.wa_id.as_deref() {
            where_clauses.push("s.wa_id = ?");
            bind_values.push(wa_id.trim().to_string());
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_clauses.join(" AND "))
        };

        let count_sql = format!(
            "SELECT COUNT(*) AS total FROM whatsapp_sessions s{}",
            where_sql
        );
        let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql);
        for value in &bind_values {
            count_query = count_query.bind(value);
        }
        let total = count_query.fetch_one(&self.pool).await?.max(0) as u64;

        let list_sql = format!(
            r#"
            SELECT
                s.wa_id,
                s.phone_number_id,
                s.locale,
                s.state,
                s.admin_status,
                s.admin_tag,
                s.assigned_to,
                s.internal_note,
                s.last_inbound_at,
                s.last_outbound_at,
                s.updated_at,
                (
                    SELECT e.text_preview
                    FROM whatsapp_events e
                    WHERE e.wa_id = s.wa_id
                      AND e.phone_number_id = s.phone_number_id
                    ORDER BY e.created_at DESC
                    LIMIT 1
                ) AS last_message_preview,
                (
                    SELECT o.status
                    FROM whatsapp_outbound_messages o
                    WHERE o.wa_id = s.wa_id
                      AND o.phone_number_id = s.phone_number_id
                    ORDER BY o.created_at DESC
                    LIMIT 1
                ) AS last_outbound_status,
                (
                    SELECT e.last_error
                    FROM whatsapp_events e
                    WHERE e.wa_id = s.wa_id
                      AND e.phone_number_id = s.phone_number_id
                      AND e.last_error IS NOT NULL
                    ORDER BY e.updated_at DESC
                    LIMIT 1
                ) AS last_error
            FROM whatsapp_sessions s
            {}
            ORDER BY COALESCE(s.last_inbound_at, s.updated_at) DESC, s.updated_at DESC
            LIMIT ? OFFSET ?
            "#,
            where_sql
        );

        let mut list_query = sqlx::query(&list_sql);
        for value in &bind_values {
            list_query = list_query.bind(value);
        }
        list_query = list_query.bind(limit as i64).bind(offset);

        let rows = list_query.fetch_all(&self.pool).await?;
        let records = rows
            .into_iter()
            .map(|row| AdminConversationRecord {
                wa_id: row.get("wa_id"),
                phone_number_id: row.get("phone_number_id"),
                locale: row.get("locale"),
                state: row.get("state"),
                admin_status: row.get("admin_status"),
                admin_tag: row.try_get("admin_tag").unwrap_or(None),
                assigned_to: row.try_get("assigned_to").unwrap_or(None),
                internal_note: row.try_get("internal_note").unwrap_or(None),
                last_inbound_at: row.try_get("last_inbound_at").unwrap_or(None),
                last_outbound_at: row.try_get("last_outbound_at").unwrap_or(None),
                last_message_preview: row.try_get("last_message_preview").unwrap_or(None),
                last_outbound_status: row.try_get("last_outbound_status").unwrap_or(None),
                last_error: row.try_get("last_error").unwrap_or(None),
                updated_at: row.get("updated_at"),
            })
            .collect();

        Ok((records, total))
    }

    pub async fn get_admin_conversation(
        &self,
        wa_id: &str,
    ) -> Result<Option<AdminConversationRecord>, Error> {
        let row = sqlx::query(
            r#"
            SELECT
                s.wa_id,
                s.phone_number_id,
                s.locale,
                s.state,
                s.admin_status,
                s.admin_tag,
                s.assigned_to,
                s.internal_note,
                s.last_inbound_at,
                s.last_outbound_at,
                s.updated_at,
                (
                    SELECT e.text_preview
                    FROM whatsapp_events e
                    WHERE e.wa_id = s.wa_id
                      AND e.phone_number_id = s.phone_number_id
                    ORDER BY e.created_at DESC
                    LIMIT 1
                ) AS last_message_preview,
                (
                    SELECT o.status
                    FROM whatsapp_outbound_messages o
                    WHERE o.wa_id = s.wa_id
                      AND o.phone_number_id = s.phone_number_id
                    ORDER BY o.created_at DESC
                    LIMIT 1
                ) AS last_outbound_status,
                (
                    SELECT e.last_error
                    FROM whatsapp_events e
                    WHERE e.wa_id = s.wa_id
                      AND e.phone_number_id = s.phone_number_id
                      AND e.last_error IS NOT NULL
                    ORDER BY e.updated_at DESC
                    LIMIT 1
                ) AS last_error
            FROM whatsapp_sessions s
            WHERE s.wa_id = ?
            ORDER BY s.updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(wa_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| AdminConversationRecord {
            wa_id: row.get("wa_id"),
            phone_number_id: row.get("phone_number_id"),
            locale: row.get("locale"),
            state: row.get("state"),
            admin_status: row.get("admin_status"),
            admin_tag: row.try_get("admin_tag").unwrap_or(None),
            assigned_to: row.try_get("assigned_to").unwrap_or(None),
            internal_note: row.try_get("internal_note").unwrap_or(None),
            last_inbound_at: row.try_get("last_inbound_at").unwrap_or(None),
            last_outbound_at: row.try_get("last_outbound_at").unwrap_or(None),
            last_message_preview: row.try_get("last_message_preview").unwrap_or(None),
            last_outbound_status: row.try_get("last_outbound_status").unwrap_or(None),
            last_error: row.try_get("last_error").unwrap_or(None),
            updated_at: row.get("updated_at"),
        }))
    }

    pub async fn upsert_session_state<T: serde::Serialize>(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        state: &T,
        locale: &str,
        draft: &impl serde::Serialize,
        last_inbound_message_id: Option<&str>,
    ) -> Result<(), Error> {
        let state_json = serde_json::to_value(state).unwrap_or(serde_json::Value::Null);
        let state_name = state_json.as_str().unwrap_or("idle").to_string();
        let draft_json = serde_json::to_string(draft).ok();
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().naive_utc();

        sqlx::query(
            r#"
            INSERT INTO whatsapp_sessions (
                id, wa_id, phone_number_id, locale, state, draft, last_inbound_message_id, last_inbound_at
            )
            VALUES (?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?)
            ON DUPLICATE KEY UPDATE
                locale = VALUES(locale),
                state = VALUES(state),
                draft = VALUES(draft),
                last_inbound_message_id = COALESCE(VALUES(last_inbound_message_id), last_inbound_message_id),
                last_inbound_at = VALUES(last_inbound_at),
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(id)
        .bind(wa_id)
        .bind(phone_number_id)
        .bind(locale)
        .bind(state_name)
        .bind(draft_json)
        .bind(last_inbound_message_id)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn update_admin_conversation(
        &self,
        wa_id: &str,
        admin_status: Option<&str>,
        admin_tag: Option<Option<&str>>,
        assigned_to: Option<Option<&str>>,
        internal_note: Option<Option<&str>>,
    ) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            UPDATE whatsapp_sessions
            SET admin_status = COALESCE(?, admin_status),
                admin_tag = CASE
                    WHEN ? = 1 THEN ?
                    ELSE admin_tag
                END,
                assigned_to = CASE
                    WHEN ? = 1 THEN ?
                    ELSE assigned_to
                END,
                internal_note = CASE
                    WHEN ? = 1 THEN ?
                    ELSE internal_note
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE wa_id = ?
            "#,
        )
        .bind(admin_status)
        .bind(admin_tag.is_some())
        .bind(admin_tag.flatten())
        .bind(assigned_to.is_some())
        .bind(assigned_to.flatten())
        .bind(internal_note.is_some())
        .bind(internal_note.flatten())
        .bind(wa_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_conversation_events(
        &self,
        wa_id: &str,
        limit: usize,
    ) -> Result<Vec<AdminConversationEventRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                event_kind,
                message_type,
                provider_message_id,
                CAST(payload AS CHAR) AS payload_json,
                processed,
                attempt_count,
                last_error,
                created_at
            FROM whatsapp_events
            WHERE wa_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            "#,
        )
        .bind(wa_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let payload_json: String = row.get("payload_json");
                let payload =
                    serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
                AdminConversationEventRecord {
                    id: row.get("id"),
                    event_kind: row.get("event_kind"),
                    message_type: row.try_get("message_type").unwrap_or(None),
                    provider_message_id: row.try_get("provider_message_id").unwrap_or(None),
                    text: extract_message_text_from_payload(&payload).unwrap_or(None),
                    processed: row.get::<i32, _>("processed"),
                    attempt_count: row.get("attempt_count"),
                    last_error: row.try_get("last_error").unwrap_or(None),
                    created_at: row.get("created_at"),
                }
            })
            .collect())
    }

    pub async fn list_conversation_outbound_messages(
        &self,
        wa_id: &str,
        limit: usize,
    ) -> Result<Vec<AdminOutboundMessageRecord>, Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                id,
                message_kind,
                status,
                provider_message_id,
                body,
                error_message,
                sent_at,
                created_at
            FROM whatsapp_outbound_messages
            WHERE wa_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            "#,
        )
        .bind(wa_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| AdminOutboundMessageRecord {
                id: row.get("id"),
                message_kind: row.get("message_kind"),
                status: row.get("status"),
                provider_message_id: row.try_get("provider_message_id").unwrap_or(None),
                body: row.get("body"),
                error_message: row.try_get("error_message").unwrap_or(None),
                sent_at: row.try_get("sent_at").unwrap_or(None),
                created_at: row.get("created_at"),
            })
            .collect())
    }

    pub async fn insert_event(&self, event: &NormalizedWebhookEvent) -> Result<bool, Error> {
        let id = Uuid::new_v4().to_string();
        let payload =
            serde_json::to_string(&build_stored_event_payload(event).map_err(|error| {
                Error::Protocol(
                    format!("failed to prepare WhatsApp event payload: {}", error).into(),
                )
            })?)
            .unwrap_or_else(|_| "null".to_string());
        let text_preview = event
            .text_preview
            .as_deref()
            .map(redact_text_preview)
            .unwrap_or_default();

        let result = sqlx::query(
            r#"
            INSERT INTO whatsapp_events (
                id,
                dedupe_key,
                phone_number_id,
                wa_id,
                provider_message_id,
                event_kind,
                message_type,
                event_timestamp,
                text_preview,
                payload
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))
            ON DUPLICATE KEY UPDATE
                updated_at = CURRENT_TIMESTAMP
            "#,
        )
        .bind(id)
        .bind(&event.dedupe_key)
        .bind(&event.phone_number_id)
        .bind(&event.wa_id)
        .bind(&event.provider_message_id)
        .bind(&event.event_kind)
        .bind(&event.message_type)
        .bind(&event.event_timestamp)
        .bind(if text_preview.is_empty() {
            None::<String>
        } else {
            Some(text_preview)
        })
        .bind(payload)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn record_outbound_message(
        &self,
        session_id: Option<&str>,
        wa_id: &str,
        phone_number_id: &str,
        message_kind: &str,
        body: &str,
    ) -> Result<String, Error> {
        let id = Uuid::new_v4().to_string();

        sqlx::query(
            r#"
            INSERT INTO whatsapp_outbound_messages (
                id, session_id, wa_id, phone_number_id, message_kind, body, status
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
            "#,
        )
        .bind(&id)
        .bind(session_id)
        .bind(wa_id)
        .bind(phone_number_id)
        .bind(message_kind)
        .bind(redact_outbound_body(body))
        .execute(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn mark_outbound_sent(
        &self,
        outbound_id: &str,
        provider_message_id: Option<&str>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_outbound_messages
            SET status = 'sent',
                provider_message_id = COALESCE(?, provider_message_id),
                sent_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(provider_message_id)
        .bind(outbound_id)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            UPDATE whatsapp_sessions s
            JOIN whatsapp_outbound_messages o ON o.session_id = s.id
            SET s.last_outbound_message_id = COALESCE(?, s.last_outbound_message_id),
                s.last_outbound_at = CURRENT_TIMESTAMP,
                s.updated_at = CURRENT_TIMESTAMP
            WHERE o.id = ?
            "#,
        )
        .bind(provider_message_id)
        .bind(outbound_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_outbound_failed(
        &self,
        outbound_id: &str,
        error_message: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_outbound_messages
            SET status = 'failed',
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(error_message)
        .bind(outbound_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_outbound_status(
        &self,
        provider_message_id: &str,
        status: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_outbound_messages
            SET status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE provider_message_id = ?
            "#,
        )
        .bind(status)
        .bind(provider_message_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_event_processed(&self, event_id: &str) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_events
            SET processed = 1,
                processed_at = CURRENT_TIMESTAMP,
                processing_started_at = NULL,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(event_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_event_processed_by_dedupe_key(&self, dedupe_key: &str) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_events
            SET processed = 1,
                processed_at = CURRENT_TIMESTAMP,
                processing_started_at = NULL,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE dedupe_key = ?
            "#,
        )
        .bind(dedupe_key)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_event_failed(
        &self,
        event_id: &str,
        error_message: &str,
        exhausted: bool,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE whatsapp_events
            SET processed = ?,
                processing_started_at = NULL,
                last_error = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            "#,
        )
        .bind(if exhausted { 3 } else { 0 })
        .bind(error_message)
        .bind(event_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn claim_pending_message_events(
        &self,
        limit: usize,
        max_attempts: i32,
        stale_after_seconds: i64,
    ) -> Result<Vec<QueuedMessageEvent>, Error> {
        let mut transaction = self.pool.begin().await?;
        let stale_before = Utc::now().naive_utc() - ChronoDuration::seconds(stale_after_seconds);

        let rows = sqlx::query(
            r#"
            SELECT id,
                   phone_number_id,
                   wa_id,
                   provider_message_id,
                   CAST(payload AS CHAR) AS payload_json,
                   attempt_count
            FROM whatsapp_events
            WHERE event_kind = 'message'
              AND attempt_count < ?
              AND (
                processed = 0
                OR (
                    processed = 2
                    AND processing_started_at IS NOT NULL
                    AND processing_started_at < ?
                )
              )
            ORDER BY created_at ASC
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(max_attempts)
        .bind(stale_before)
        .bind(limit as i64)
        .fetch_all(&mut *transaction)
        .await?;

        if rows.is_empty() {
            transaction.commit().await?;
            return Ok(Vec::new());
        }

        let mut claimed = Vec::with_capacity(rows.len());
        let mut ids = Vec::with_capacity(rows.len());

        for row in rows {
            let payload_json: String = row.get("payload_json");
            let payload: serde_json::Value =
                serde_json::from_str(&payload_json).unwrap_or(serde_json::Value::Null);
            let attempt_count = row.get::<i32, _>("attempt_count") + 1;

            ids.push(row.get::<String, _>("id"));
            claimed.push(QueuedMessageEvent {
                id: row.get("id"),
                phone_number_id: row.get("phone_number_id"),
                wa_id: row.get("wa_id"),
                provider_message_id: row.get("provider_message_id"),
                text: extract_message_text_from_payload(&payload).unwrap_or(None),
                attempt_count,
            });
        }

        let mut query_builder = QueryBuilder::<MySql>::new(
            r#"
            UPDATE whatsapp_events
            SET processed = 2,
                processing_started_at = CURRENT_TIMESTAMP,
                attempt_count = attempt_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id IN (
            "#,
        );
        let mut separated = query_builder.separated(", ");
        for id in &ids {
            separated.push_bind(id);
        }
        query_builder.push(")");

        query_builder.build().execute(&mut *transaction).await?;
        transaction.commit().await?;

        Ok(claimed)
    }

    pub async fn purge_old_records(
        &self,
        event_retention_days: i64,
        outbound_retention_days: i64,
        session_retention_days: i64,
    ) -> Result<(), Error> {
        let event_cutoff = Utc::now().naive_utc() - ChronoDuration::days(event_retention_days);
        let outbound_cutoff =
            Utc::now().naive_utc() - ChronoDuration::days(outbound_retention_days);
        let session_cutoff = Utc::now().naive_utc() - ChronoDuration::days(session_retention_days);

        sqlx::query(
            r#"
            DELETE FROM whatsapp_events
            WHERE created_at < ?
              AND processed IN (1, 3)
            "#,
        )
        .bind(event_cutoff)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM whatsapp_outbound_messages
            WHERE created_at < ?
              AND status IN ('sent', 'delivered', 'read', 'failed')
            "#,
        )
        .bind(outbound_cutoff)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM whatsapp_sessions
            WHERE updated_at < ?
            "#,
        )
        .bind(session_cutoff)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
