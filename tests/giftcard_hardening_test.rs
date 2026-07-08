use exchange_shared::modules::giftcard::crud::GiftCardCrud;
use sqlx::{mysql::MySqlPoolOptions, MySql, Pool, Row};

#[tokio::test]
async fn giftcard_lock_is_exclusive_per_order() {
    let Some(db) = setup_db().await else {
        eprintln!("skipping giftcard lock test: database is unavailable");
        return;
    };

    let crud = GiftCardCrud::new(db);
    let first_lock = crud
        .acquire_named_lock("giftcard:test:lock", 1)
        .await
        .expect("first lock");
    let second_attempt = crud.acquire_named_lock("giftcard:test:lock", 0).await;

    assert!(second_attempt.is_err());
    first_lock.release().await.expect("release first lock");
}

#[tokio::test]
async fn giftcard_lock_supports_long_mysql_unsafe_keys() {
    let Some(db) = setup_db().await else {
        eprintln!("skipping giftcard long lock test: database is unavailable");
        return;
    };

    let crud = GiftCardCrud::new(db);
    let key = "giftcard:create:client:00798552-8bf8-48d8-8a14-15f48eb91804:0b2c34b9b7f037db390e0f487251bc31e30e8bd89b25b3b225c556ea2fd02355";

    let first_lock = crud
        .acquire_named_lock(key, 1)
        .await
        .expect("first long lock");
    let second_attempt = crud.acquire_named_lock(key, 0).await;

    assert!(second_attempt.is_err());
    first_lock.release().await.expect("release long lock");
}

#[tokio::test]
async fn giftcard_exhausted_retries_fail_and_terminal_records_redact() {
    let Some(db) = setup_db().await else {
        eprintln!("skipping giftcard hardening test: database is unavailable");
        return;
    };

    let crud = GiftCardCrud::new(db.clone());

    let retry_order_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO giftcard_orders (
            id, user_id, client_id, owner_key, request_hash, order_kind,
            source_ticker, source_network, amount, recipient_email,
            webhook_mode, status, attempt_count
        )
        VALUES (?, NULL, ?, ?, ?, 'giftcard', 'btc', 'Mainnet', 100.0, ?, 'managed', 'retry_pending', 5)
        "#,
    )
    .bind(&retry_order_id)
    .bind("client-a")
    .bind("client:client-a")
    .bind(format!("hash-{}", retry_order_id))
    .bind("buyer@example.com")
    .execute(&db)
    .await
    .expect("insert retry order");

    let failed = crud
        .mark_exhausted_pending_failed(5)
        .await
        .expect("mark exhausted pending failed");
    assert!(failed >= 1);

    let retry_row =
        sqlx::query("SELECT status, completed_at FROM giftcard_orders WHERE id = ? LIMIT 1")
            .bind(&retry_order_id)
            .fetch_one(&db)
            .await
            .expect("fetch retried order");
    let retry_status: String = retry_row.get("status");
    let retry_completed_at: Option<sqlx::types::chrono::NaiveDateTime> =
        retry_row.get("completed_at");

    assert_eq!(retry_status, "failed");
    assert!(retry_completed_at.is_some());

    let redaction_order_id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO giftcard_orders (
            id, user_id, client_id, owner_key, request_hash, order_kind,
            source_ticker, source_network, amount, recipient_email,
            webhook_mode, webhook_url, provider_password, deposit_address,
            settlement_address, refund_address, details_json, status,
            last_error, completed_at
        )
        VALUES (
            ?, NULL, ?, ?, ?, 'giftcard',
            'btc', 'Mainnet', 100.0, ?,
            'managed', ?, ?, ?, ?, ?, CAST(? AS JSON), 'completed',
            ?, DATE_SUB(UTC_TIMESTAMP(), INTERVAL 40 DAY)
        )
        "#,
    )
    .bind(&redaction_order_id)
    .bind("client-b")
    .bind("client:client-b")
    .bind(format!("hash-{}", redaction_order_id))
    .bind("sensitive@example.com")
    .bind("https://hook.example.com/secret")
    .bind("provider-secret")
    .bind("bc1depositsecret")
    .bind("bc1settlementsecret")
    .bind("bc1refundsecret")
    .bind(r#"{"redeem_code":"ABCD-1234","activation_link":"https://secret.example.com"}"#)
    .bind("upstream secret detail")
    .execute(&db)
    .await
    .expect("insert completed order");

    let redacted = crud
        .redact_terminal_orders(30)
        .await
        .expect("redact terminal orders");
    assert!(redacted >= 1);

    let redaction_row = sqlx::query(
        r#"
        SELECT recipient_email, webhook_url, provider_password, deposit_address,
               settlement_address, refund_address, CAST(details_json AS CHAR) AS details_json,
               last_error
        FROM giftcard_orders
        WHERE id = ?
        LIMIT 1
        "#,
    )
    .bind(&redaction_order_id)
    .fetch_one(&db)
    .await
    .expect("fetch redacted order");

    let recipient_email: String = redaction_row.get("recipient_email");
    let webhook_url: Option<String> = redaction_row.get("webhook_url");
    let provider_password: Option<String> = redaction_row.get("provider_password");
    let deposit_address: Option<String> = redaction_row.get("deposit_address");
    let settlement_address: Option<String> = redaction_row.get("settlement_address");
    let refund_address: Option<String> = redaction_row.get("refund_address");
    let details_json: Option<String> = redaction_row.get("details_json");
    let last_error: Option<String> = redaction_row.get("last_error");

    assert!(recipient_email.starts_with("[redacted:"));
    assert!(webhook_url.is_none());
    assert!(provider_password.is_none());
    assert!(deposit_address.is_none());
    assert!(settlement_address.is_none());
    assert!(refund_address.is_none());
    assert!(details_json.is_none());
    assert!(last_error
        .as_deref()
        .is_some_and(|value| value.starts_with("[redacted error:")));
}

async fn setup_db() -> Option<Pool<MySql>> {
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
