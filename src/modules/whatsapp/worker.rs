use std::sync::Arc;
use tokio::time::{sleep, Duration, Instant};

use crate::{modules::whatsapp::crud::WhatsAppCrud, AppState};

use super::service::WhatsAppFlowService;

const WORKER_BATCH_SIZE: usize = 10;
const WORKER_MAX_ATTEMPTS: i32 = 5;
const STALE_PROCESSING_SECONDS: i64 = 90;
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const BUSY_POLL_INTERVAL: Duration = Duration::from_millis(200);
const CLEANUP_INTERVAL: Duration = Duration::from_hours(1);
const EVENT_RETENTION_DAYS: i64 = 30;
const OUTBOUND_RETENTION_DAYS: i64 = 30;
const SESSION_RETENTION_DAYS: i64 = 90;

pub async fn run_whatsapp_worker(state: Arc<AppState>) {
    let crud = WhatsAppCrud::new(state.db.clone());
    let flow = WhatsAppFlowService::new(state.clone());
    let mut last_cleanup = Instant::now() - CLEANUP_INTERVAL;

    loop {
        let mut processed_any = false;

        match crud
            .claim_pending_message_events(
                WORKER_BATCH_SIZE,
                WORKER_MAX_ATTEMPTS,
                STALE_PROCESSING_SECONDS,
            )
            .await
        {
            Ok(events) => {
                if !events.is_empty() {
                    processed_any = true;
                }

                for event in events {
                    let result = match event.text.as_deref() {
                        Some(text) if !text.trim().is_empty() => {
                            flow.process_message_event(
                                &event.phone_number_id,
                                &event.wa_id,
                                event.provider_message_id.as_deref(),
                                text,
                            )
                            .await
                        }
                        _ => Err("stored WhatsApp message text is unavailable".to_string()),
                    };

                    match result {
                        Ok(()) => {
                            if let Err(error) = crud.mark_event_processed(&event.id).await {
                                tracing::error!(
                                    "failed to mark WhatsApp event {} as processed: {}",
                                    event.id,
                                    error
                                );
                            }
                        }
                        Err(error) => {
                            let exhausted = event.attempt_count >= WORKER_MAX_ATTEMPTS;
                            if let Err(mark_error) =
                                crud.mark_event_failed(&event.id, &error, exhausted).await
                            {
                                tracing::error!(
                                    "failed to mark WhatsApp event {} as failed: {}",
                                    event.id,
                                    mark_error
                                );
                            }
                            tracing::error!(
                                "failed to process WhatsApp event {} for {} on {} (attempt {}): {}",
                                event.id,
                                event.wa_id,
                                event.phone_number_id,
                                event.attempt_count,
                                error
                            );

                            // Retries are exhausted, so this input will never be
                            // reprocessed - make sure the user hears something
                            // instead of silence, even though we don't show them
                            // the internal reason.
                            if exhausted {
                                if let Err(notify_error) = flow
                                    .notify_processing_failed(&event.wa_id, &event.phone_number_id)
                                    .await
                                {
                                    tracing::error!(
                                        "failed to notify {} on {} about exhausted processing failure: {}",
                                        event.wa_id,
                                        event.phone_number_id,
                                        notify_error
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => {
                tracing::error!("WhatsApp worker failed to claim queued events: {}", error);
            }
        }

        if last_cleanup.elapsed() >= CLEANUP_INTERVAL {
            match crud
                .purge_old_records(
                    EVENT_RETENTION_DAYS,
                    OUTBOUND_RETENTION_DAYS,
                    SESSION_RETENTION_DAYS,
                )
                .await
            {
                Ok(()) => {
                    tracing::info!("WhatsApp retention cleanup completed");
                }
                Err(error) => {
                    tracing::warn!("WhatsApp retention cleanup failed: {}", error);
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
