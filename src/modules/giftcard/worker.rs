use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

use crate::{modules::giftcard::service::GiftCardService, AppState};

const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const BUSY_POLL_INTERVAL: Duration = Duration::from_millis(500);
const CREATE_BATCH_SIZE: usize = 10;
const REFRESH_BATCH_SIZE: usize = 20;
const REFRESH_STALE_AFTER_SECONDS: i64 = 60;
const CLEANUP_INTERVAL: Duration = Duration::from_hours(1);

pub async fn run_giftcard_worker(state: Arc<AppState>) {
    let service = GiftCardService::new(state);
    let mut last_cleanup = Instant::now() - CLEANUP_INTERVAL;

    loop {
        let mut processed_any = false;

        if let Ok(processed) = service.run_retry_batch(CREATE_BATCH_SIZE).await {
            processed_any |= processed > 0;
        }

        if let Ok(processed) = service
            .reconcile_active_batch(REFRESH_BATCH_SIZE, REFRESH_STALE_AFTER_SECONDS)
            .await
        {
            processed_any |= processed > 0;
        }

        if last_cleanup.elapsed() >= CLEANUP_INTERVAL {
            match service.run_retention_cleanup().await {
                Ok(redacted) => {
                    if redacted > 0 {
                        tracing::info!(
                            "Gift card retention cleanup redacted {} terminal order(s)",
                            redacted
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!("Gift card retention cleanup failed: {}", error);
                }
            }

            last_cleanup = Instant::now();
        }

        sleep(if processed_any {
            BUSY_POLL_INTERVAL
        } else {
            IDLE_POLL_INTERVAL
        })
        .await;
    }
}
