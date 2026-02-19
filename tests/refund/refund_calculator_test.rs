use exchange_shared::services::refund::{RefundCalculator, RefundConfig};
use sqlx::MySqlPool;
use serial_test::serial;
use uuid::Uuid;
use rust_decimal::Decimal;
use std::str::FromStr;

async fn setup_test_db() -> MySqlPool {
    dotenvy::dotenv().ok();
    
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"));
    
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Run migrations to ensure refund tables exist
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    
    pool
}

async fn cleanup_test_data(pool: &MySqlPool) {
    sqlx::query("DELETE FROM refunds")
        .execute(pool)
        .await
        .ok();
}

async fn create_test_swap(pool: &MySqlPool) -> Uuid {
    let swap_id = Uuid::new_v4();
    
    // First ensure a test provider exists
    sqlx::query(
        "INSERT IGNORE INTO providers (id, name, api_url, is_active) VALUES ('test_provider', 'Test Provider', 'https://test.com', 1)"
    )
    .execute(pool)
    .await
    .ok();
    
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address,
            status, platform_fee, total_fee
        )
        VALUES (?, 'test_provider', 'BTC', 'mainnet', 'ETH', 'mainnet',
                ?, ?, 15.0, 'test_deposit', 'test_recipient',
                'failed', ?, ?)
        "#
    )
    .bind(swap_id.to_string())
    .bind("0.1")
    .bind("1.5")
    .bind("0.001")
    .bind("0.002")
    .execute(pool)
    .await
    .expect("Failed to create test swap");
    
    swap_id
}

#[tokio::test]
#[serial]
async fn test_calculator_creation() {
    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;
    
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    // Calculator should be created successfully
    assert!(std::mem::size_of_val(&calculator) > 0);
}

#[tokio::test]
#[serial]
async fn test_calculate_refund_for_failed_swap() {
    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;
    
    let swap_id = create_test_swap(&pool).await;
    
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    // Calculate refund
    let result = calculator.calculate_refund(swap_id).await;
    assert!(result.is_ok());
    
    let calculation = result.unwrap();
    
    // Verify calculation
    assert_eq!(calculation.deposit_amount, Decimal::from_str("0.1").unwrap());
    assert!(calculation.refund_amount > Decimal::ZERO);
    assert!(calculation.is_economical); // 0.1 BTC is above threshold
    
    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_calculate_refund_nonexistent_swap() {
    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;
    
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    let nonexistent_id = Uuid::new_v4();
    let result = calculator.calculate_refund(nonexistent_id).await;
    
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Swap not found"));
    
    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_priority_score_calculation() {
    let pool = setup_test_db().await;
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    // Old, large amount, few retries = high priority
    let score1 = calculator.calculate_priority_score(6.0, 500.0, 1);
    // 0.5 * 6 + 0.3 * 5 + 0.2 * 9 = 3.0 + 1.5 + 1.8 = 6.3
    assert!((score1 - 6.3).abs() < 0.01);
    
    // New, small amount, many retries = low priority
    let score2 = calculator.calculate_priority_score(1.0, 50.0, 4);
    // 0.5 * 1 + 0.3 * 0.5 + 0.2 * 6 = 0.5 + 0.15 + 1.2 = 1.85
    assert!((score2 - 1.85).abs() < 0.01);
    
    // Verify score1 > score2 (higher priority)
    assert!(score1 > score2);
}

#[tokio::test]
#[serial]
async fn test_refund_amount_calculation() {
    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;
    
    let swap_id = create_test_swap(&pool).await;
    
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    let calculation = calculator.calculate_refund(swap_id).await.unwrap();
    
    // Refund = deposit - platform_fee - total_fee - gas_estimate
    // 0.1 - 0.001 - 0.002 - 0.0001 = 0.0969
    let expected_refund = Decimal::from_str("0.1").unwrap()
        - Decimal::from_str("0.001").unwrap()
        - Decimal::from_str("0.002").unwrap()
        - Decimal::from_str("0.0001").unwrap();
    
    assert!((calculation.refund_amount - expected_refund).abs() < Decimal::from_str("0.0001").unwrap());
    
    cleanup_test_data(&pool).await;
}

#[tokio::test]
#[serial]
async fn test_economic_threshold_check() {
    let pool = setup_test_db().await;
    cleanup_test_data(&pool).await;
    
    // Create swap with very small amount (below threshold)
    let swap_id = Uuid::new_v4();
    
    // Ensure test provider exists
    sqlx::query(
        "INSERT IGNORE INTO providers (id, name, api_url, is_active) VALUES ('test_provider', 'Test Provider', 'https://test.com', 1)"
    )
    .execute(&pool)
    .await
    .ok();
    
    sqlx::query(
        r#"
        INSERT INTO swaps (
            id, provider_id, from_currency, from_network, to_currency, to_network,
            amount, estimated_receive, rate, deposit_address, recipient_address,
            status, platform_fee, total_fee
        )
        VALUES (?, 'test_provider', 'BTC', 'mainnet', 'ETH', 'mainnet',
                ?, ?, 15.0, 'test_deposit', 'test_recipient',
                'failed', ?, ?)
        "#
    )
    .bind(swap_id.to_string())
    .bind("0.00005") // Very small amount
    .bind("0.00075")
    .bind("0.00001")
    .bind("0.00001")
    .execute(&pool)
    .await
    .unwrap();
    
    let config = RefundConfig::default();
    let calculator = RefundCalculator::new(pool.clone(), config);
    
    let calculation = calculator.calculate_refund(swap_id).await.unwrap();
    
    // Should not be economical (below 0.0001 BTC threshold)
    assert!(!calculation.is_economical);
    assert!(calculation.reason.contains("below minimum threshold"));
    
    cleanup_test_data(&pool).await;
}
