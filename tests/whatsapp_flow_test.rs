use exchange_shared::modules::whatsapp::crud::WhatsAppCrud;
use exchange_shared::services::whatsapp::NormalizedWebhookEvent;
use serde_json::Value;
use sqlx::Row;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};

#[tokio::test]
async fn whatsapp_queue_encrypts_at_rest_and_decrypts_on_claim() {
    let Some(db) = setup_whatsapp_db().await else {
        eprintln!("skipping whatsapp queue test: database is unavailable");
        return;
    };
    let crud = WhatsAppCrud::new(db.clone());

    let original_text = "swap 100 usdc on stellar to bitcoin";
    let event = NormalizedWebhookEvent {
        dedupe_key: format!("whatsapp-test-{}", uuid::Uuid::new_v4()),
        phone_number_id: "pnid-1".to_string(),
        wa_id: Some("wa-123".to_string()),
        provider_message_id: Some("wamid-1".to_string()),
        event_kind: "message".to_string(),
        message_type: Some("text".to_string()),
        event_timestamp: Some("1711111111".to_string()),
        text_preview: Some(original_text.to_string()),
        payload: Value::Null,
    };

    let inserted = crud.insert_event(&event).await.expect("event insert");
    assert!(inserted);

    let row = sqlx::query(
        r#"
        SELECT text_preview, CAST(payload AS CHAR) AS payload_json
        FROM whatsapp_events
        WHERE dedupe_key = ?
        "#,
    )
    .bind(&event.dedupe_key)
    .fetch_one(&db)
    .await
    .expect("stored event row");

    let stored_preview: Option<String> = row.get("text_preview");
    let stored_payload: String = row.get("payload_json");

    assert_ne!(stored_preview.as_deref(), Some(original_text));
    assert!(!stored_payload.contains(original_text));

    let claimed = crud
        .claim_pending_message_events(10, 5, 90)
        .await
        .expect("claim queued events");

    let queued = claimed
        .into_iter()
        .find(|queued| queued.provider_message_id.as_deref() == Some("wamid-1"))
        .expect("claimed WhatsApp message event");

    assert_eq!(queued.text.as_deref(), Some(original_text));

    crud.mark_event_processed(&queued.id)
        .await
        .expect("mark processed");
}

#[tokio::test]
async fn whatsapp_session_lock_is_exclusive_per_sender() {
    let Some(db) = setup_whatsapp_db().await else {
        eprintln!("skipping whatsapp lock test: database is unavailable");
        return;
    };
    let crud = WhatsAppCrud::new(db);

    let first_lock = crud
        .acquire_session_lock("wa-lock-1", "pnid-lock-1", 1)
        .await
        .expect("first lock");

    let second_attempt = crud
        .acquire_session_lock("wa-lock-1", "pnid-lock-1", 0)
        .await;

    assert!(second_attempt.is_err());

    first_lock.release().await.expect("release first lock");
}

async fn setup_whatsapp_db() -> Option<Pool<MySql>> {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()?;

    let db = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .ok()?;

    sqlx::migrate!("./migrations").run(&db).await.ok()?;
    Some(db)
}
