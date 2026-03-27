use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PollingState {
    pub swap_id: String,
    pub last_polled_at: Option<DateTime<Utc>>,
    pub next_poll_at: DateTime<Utc>,
    pub poll_count: i32,
    pub last_status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
