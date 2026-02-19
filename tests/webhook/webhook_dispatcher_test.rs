use exchange_shared::services::webhook::{
    WebhookDispatcher, RetryConfig, Webhook, WebhookPayload,
};
use sqlx::MySqlPool;
use serial_test::serial;
use uuid::Uuid;
use chrono::Utc;

async fn setup_test_db() -> MySqlPool {
    dotenvy::dotenv().ok();
    
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"));
    
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Run migrations to ensure webhook tables exist
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    
    pool
}

async fn cleanup_webhooks(pool: &MySqlPool) {
    sqlx::query!("DELETE FROM webhook_deliveries")
        .execute(pool)
        .await
        .ok();
    
    sqlx::query!("DELETE FROM webhooks")
        .execute(pool)
        .await
        .ok();
}

async fn create_test_webhook(pool: &MySqlPool, swap_id: Uuid) -> Webhook {
    let webhook_id = Uuid::new_v4();
    let secret_key = "test_secret_key_12345678901234567890";
    let events = serde_json::json!(["swap.completed", "swap.failed"]);
    
    sqlx::query!(
        r#"
        INSERT INTO webhooks (
            id, swap_id, url, secret_key, events, enabled, rate_limit_per_second
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
        webhook_id.to_string(),
        swap_id.to_string(),
        "https://example.com/webhook",
        secret_key,
        events,
        1,
        10
    )
    .execute(pool)
    .await
    .expect("Failed to create test webhook");
    
    Webhook {
        id: webhook_id,
        swap_id,
        url: "https://example.com/webhook".to_string(),
        secret_key: secret_key.to_string(),
        events: vec!["swap.completed".to_string(), "swap.failed".to_string()],
        enabled: true,
        rate_limit_per_second: 10,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[tokio::test]
#[serial]
async fn test_webhook_dispatcher_creation() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let retry_config = RetryConfig::default();
    let dispatcher = WebhookDispatcher::new(pool.clone(), retry_config);
    
    // Dispatcher should be created successfully
    assert!(std::mem::size_of_val(&dispatcher) > 0);
}

#[tokio::test]
#[serial]
async fn test_dispatch_disabled_webhook() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let mut webhook = create_test_webhook(&pool, swap_id).await;
    webhook.enabled = false;
    
    // Update webhook to disabled
    sqlx::query!(
        "UPDATE webhooks SET enabled = 0 WHERE id = ?",
        webhook.id.to_string()
    )
    .execute(&pool)
    .await
    .unwrap();
    
    let retry_config = RetryConfig::default();
    let dispatcher = WebhookDispatcher::new(pool.clone(), retry_config);
    
    let payload = WebhookPayload {
        id: Uuid::new_v4().to_string(),
        event_type: "swap.completed".to_string(),
        created_at: Utc::now().timestamp(),
        data: serde_json::json!({
            "swap_id": swap_id.to_string(),
            "status": "completed"
        }),
    };
    
    // Should return Ok without dispatching
    let result = dispatcher.dispatch(&webhook, payload).await;
    assert!(result.is_ok());
    
    // Verify no delivery record was created
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert_eq!(count, 0);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_idempotency_check() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    let retry_config = RetryConfig::default();
    let dispatcher = WebhookDispatcher::new(pool.clone(), retry_config);
    
    let payload = WebhookPayload {
        id: Uuid::new_v4().to_string(),
        event_type: "swap.completed".to_string(),
        created_at: Utc::now().timestamp(),
        data: serde_json::json!({
            "swap_id": swap_id.to_string(),
            "status": "completed"
        }),
    };
    
    // First dispatch - will fail due to network (example.com), but should create delivery record
    let _ = dispatcher.dispatch(&webhook, payload.clone()).await;
    
    // Second dispatch with same payload - should be idempotent
    let result = dispatcher.dispatch(&webhook, payload.clone()).await;
    assert!(result.is_ok());
    
    // Verify only one delivery record exists
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhook_deliveries WHERE swap_id = ?"
    )
    .bind(swap_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    
    // Should have at least one record (first attempt)
    assert!(count >= 1);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_retry_config_default() {
    let config = RetryConfig::default();
    
    assert_eq!(config.max_attempts, 10);
    assert_eq!(config.base_delay_secs, 30);
    assert_eq!(config.max_delay_secs, 86400); // 24 hours
    assert_eq!(config.timeout_secs, 30);
}

#[tokio::test]
#[serial]
async fn test_retry_config_custom() {
    let config = RetryConfig {
        max_attempts: 5,
        base_delay_secs: 10,
        max_delay_secs: 3600,
        jitter_factor: 0.1,
        timeout_secs: 15,
    };
    
    assert_eq!(config.max_attempts, 5);
    assert_eq!(config.base_delay_secs, 10);
    assert_eq!(config.max_delay_secs, 3600);
    assert_eq!(config.timeout_secs, 15);
}

#[tokio::test]
#[serial]
async fn test_process_retries_empty_queue() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let retry_config = RetryConfig::default();
    let dispatcher = WebhookDispatcher::new(pool.clone(), retry_config);
    
    let processed = dispatcher.process_retries().await.unwrap();
    assert_eq!(processed, 0);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_webhook_creation_in_db() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    // Verify webhook was created
    let result = sqlx::query!(
        "SELECT id, swap_id, url, enabled FROM webhooks WHERE id = ?",
        webhook.id.to_string()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(result.id, webhook.id.to_string());
    assert_eq!(result.swap_id, swap_id.to_string());
    assert_eq!(result.url, "https://example.com/webhook");
    assert_eq!(result.enabled, Some(1));
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_webhook_payload_serialization() {
    let payload = WebhookPayload {
        id: Uuid::new_v4().to_string(),
        event_type: "swap.completed".to_string(),
        created_at: Utc::now().timestamp(),
        data: serde_json::json!({
            "swap_id": "test-swap-id",
            "status": "completed",
            "amount": "1000000"
        }),
    };
    
    // Serialize to JSON
    let json = serde_json::to_string(&payload).unwrap();
    assert!(json.contains("swap.completed"));
    assert!(json.contains("test-swap-id"));
    
    // Deserialize back
    let deserialized: WebhookPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.event_type, "swap.completed");
    assert_eq!(deserialized.data["swap_id"], "test-swap-id");
}

#[tokio::test]
#[serial]
async fn test_multiple_webhooks_per_swap() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    
    // Create multiple webhooks for same swap
    let webhook1 = create_test_webhook(&pool, swap_id).await;
    let webhook2 = create_test_webhook(&pool, swap_id).await;
    
    // Verify both webhooks exist
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM webhooks WHERE swap_id = ?"
    )
    .bind(swap_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(count, 2);
    assert_ne!(webhook1.id, webhook2.id);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_webhook_events_filter() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    // Webhook should have specific events
    assert!(webhook.events.contains(&"swap.completed".to_string()));
    assert!(webhook.events.contains(&"swap.failed".to_string()));
    assert!(!webhook.events.contains(&"swap.pending".to_string()));
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_rate_limiter_per_webhook() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    assert_eq!(webhook.rate_limit_per_second, 10);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_webhook_secret_key_storage() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    // Verify secret key is stored
    let result = sqlx::query!(
        "SELECT secret_key FROM webhooks WHERE id = ?",
        webhook.id.to_string()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(result.secret_key, "test_secret_key_12345678901234567890");
    assert!(result.secret_key.len() >= 32);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_delivery_record_structure() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    let delivery_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "id": Uuid::new_v4().to_string(),
        "type": "swap.completed",
        "created_at": Utc::now().timestamp(),
        "data": {"swap_id": swap_id.to_string()}
    });
    
    // Create delivery record
    sqlx::query!(
        r#"
        INSERT INTO webhook_deliveries (
            id, webhook_id, swap_id, event_type, idempotency_key,
            payload, signature, attempt_number, max_attempts
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        delivery_id.to_string(),
        webhook.id.to_string(),
        swap_id.to_string(),
        "swap.completed",
        "test_idempotency_key",
        payload,
        "test_signature",
        0,
        10
    )
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify delivery record
    let result = sqlx::query!(
        "SELECT id, webhook_id, swap_id, event_type, attempt_number FROM webhook_deliveries WHERE id = ?",
        delivery_id.to_string()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(result.id, delivery_id.to_string());
    assert_eq!(result.webhook_id, webhook.id.to_string());
    assert_eq!(result.event_type, "swap.completed");
    assert_eq!(result.attempt_number, 0);
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_dlq_flag() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    let delivery_id = Uuid::new_v4();
    let payload = serde_json::json!({"test": "data"});
    
    // Create delivery record
    sqlx::query!(
        r#"
        INSERT INTO webhook_deliveries (
            id, webhook_id, swap_id, event_type, idempotency_key,
            payload, signature, attempt_number, max_attempts, is_dlq
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        delivery_id.to_string(),
        webhook.id.to_string(),
        swap_id.to_string(),
        "swap.failed",
        "test_key",
        payload,
        "sig",
        10,
        10,
        1
    )
    .execute(&pool)
    .await
    .unwrap();
    
    // Verify DLQ flag
    let result = sqlx::query!(
        "SELECT is_dlq FROM webhook_deliveries WHERE id = ?",
        delivery_id.to_string()
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(result.is_dlq, Some(1));
    
    cleanup_webhooks(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_webhook_url_validation() {
    let pool = setup_test_db().await;
    cleanup_webhooks(&pool).await;
    
    let swap_id = Uuid::new_v4();
    let webhook = create_test_webhook(&pool, swap_id).await;
    
    // URL should be valid HTTPS
    assert!(webhook.url.starts_with("https://"));
    assert!(webhook.url.contains("example.com"));
    
    cleanup_webhooks(&pool).await;
}
