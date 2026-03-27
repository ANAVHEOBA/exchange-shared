use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollingConfig {
    pub initial_interval_secs: u64,
    pub decay_factor: f64,
    pub max_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorStatus {
    pub active_polls: usize,
    pub last_run_at: DateTime<Utc>,
}
