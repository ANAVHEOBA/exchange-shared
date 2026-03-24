use crate::modules::monitor::model::PollingState;
use chrono::Utc;
use sqlx::{MySql, Pool};

pub struct MonitorCrud {
    pool: Pool<MySql>,
}

impl MonitorCrud {
    pub fn new(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    /// Get all swaps that are due for polling
    pub async fn get_due_polls(&self) -> Result<Vec<PollingState>, sqlx::Error> {
        sqlx::query_as::<_, PollingState>(
            "SELECT * FROM polling_states WHERE next_poll_at <= NOW()",
        )
        .fetch_all(&self.pool)
        .await
    }

    /// Update the polling state after a run
    pub async fn update_poll_result(
        &self,
        swap_id: &str,
        status: &str,
        next_poll_in_secs: u64,
    ) -> Result<(), sqlx::Error> {
        let next_poll = Utc::now() + chrono::Duration::seconds(next_poll_in_secs as i64);

        sqlx::query(
            r#"
            INSERT INTO polling_states (swap_id, last_polled_at, next_poll_at, poll_count, last_status)
            VALUES (?, NOW(), ?, 1, ?)
            ON DUPLICATE KEY UPDATE
                last_polled_at = NOW(),
                next_poll_at = ?,
                poll_count = poll_count + 1,
                last_status = VALUES(last_status),
                updated_at = NOW()
            "#
        )
        .bind(swap_id)
        .bind(next_poll)
        .bind(status)
        .bind(next_poll)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
