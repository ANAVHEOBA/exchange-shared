use axum_test::TestServer;
use exchange_shared::services::redis_cache::RedisService;
use sqlx::{MySql, Pool};
use async_trait::async_trait;
use exchange_shared::services::wallet::rpc::{BlockchainProvider, RpcError};

pub mod rate_limiter;

// Allow dead_code for utilities used by other test files
#[allow(dead_code)]
pub struct TestContext {
    pub server: TestServer,
    pub db: Pool<MySql>,
    pub redis: RedisService,
}

#[allow(dead_code)]
impl TestContext {
    pub async fn new() -> Self {
        dotenvy::dotenv().ok();

        let database_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"));

        let db = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        // Run migrations
        sqlx::migrate!("./migrations")
            .run(&db)
            .await
            .expect("Failed to run migrations");

        let jwt_secret = std::env::var("JWT_SECRET")
            .unwrap_or_else(|_| "test-secret-key-for-testing-only".to_string());
        let jwt_service = exchange_shared::services::jwt::JwtService::new(jwt_secret);

        let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
        let redis_service = RedisService::new(&redis_url);

        let wallet_mnemonic = std::env::var("WALLET_MNEMONIC")
            .unwrap_or_else(|_| "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about".to_string());

        let app = exchange_shared::create_app(db.clone(), redis_service.clone(), jwt_service, wallet_mnemonic).await;
        let server = TestServer::new(app).expect("Failed to create test server");

        Self { server, db, redis: redis_service }
    }

    pub async fn cleanup(&self) {
        // Clean up Redis
        if let Ok(mut conn) = self.redis.get_client().get_multiplexed_async_connection().await {
            let _: () = redis::cmd("FLUSHDB").query_async(&mut conn).await.unwrap_or(());
        }

        /* 
        // Clean up test data - Disabled to prevent cross-test interference
        sqlx::query("DELETE FROM polling_states").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM swap_address_info").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM swaps").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM refresh_tokens").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM backup_codes").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM password_resets").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM email_verifications").execute(&self.db).await.ok();
        sqlx::query("DELETE FROM users").execute(&self.db).await.ok();
        */
    }
}

// Helper to generate unique test email
#[allow(dead_code)]
pub fn test_email() -> String {
    format!("test_{}@example.com", uuid::Uuid::new_v4())
}

// Helper to generate test password
#[allow(dead_code)]
pub fn test_password() -> &'static str {
    "TestPassword123!"
}

// Helper to setup test server (simplified for swap tests)
#[allow(dead_code)]
pub async fn setup_test_server() -> TestServer {
    let ctx = TestContext::new().await;
    ctx.server
}

// Helper to measure and print request duration WITH SMART RATE LIMITING
// Rate-limits ALL /swap/* endpoints to prevent 429 errors
#[allow(dead_code)]
pub async fn timed_get(server: &TestServer, path: &str) -> axum_test::TestResponse {
    // Rate limit ALL swap endpoints that might hit Trocador API
    let needs_rate_limit = path.starts_with("/swap/");
    
    let _guard = if needs_rate_limit {
        Some(rate_limiter::TROCADOR_RATE_LIMITER.acquire().await)
    } else {
        None
    };
    
    let start = std::time::Instant::now();
    let response = server.get(path).await;
    let duration = start.elapsed();
    println!("⏱️ GET {} took {:?}", path, duration);
    response
}

#[allow(dead_code)]
pub async fn timed_post<T: serde::Serialize>(server: &TestServer, path: &str, body: &T) -> axum_test::TestResponse {
    // Rate limit ALL swap endpoints that might hit Trocador API
    let needs_rate_limit = path.starts_with("/swap/");
    
    let _guard = if needs_rate_limit {
        Some(rate_limiter::TROCADOR_RATE_LIMITER.acquire().await)
    } else {
        None
    };
    
    let start = std::time::Instant::now();
    let response = server.post(path).json(body).await;
    let duration = start.elapsed();
    println!("⏱️ POST {} took {:?}", path, duration);
    response
}

// =============================================================================
// MOCK PROVIDER (NO-OP)
// =============================================================================

#[allow(dead_code)]
#[derive(Clone)]
pub struct NoOpProvider;

#[async_trait]
impl BlockchainProvider for NoOpProvider {
    async fn get_transaction_count(&self, _address: &str) -> Result<u64, RpcError> { Ok(0) }
    async fn get_gas_price(&self) -> Result<u64, RpcError> { Ok(0) }
    async fn send_raw_transaction(&self, _signed_hex: &str) -> Result<String, RpcError> { Ok("".to_string()) }
    async fn get_balance(&self, _address: &str) -> Result<f64, RpcError> { Ok(0.0) }
}


// =============================================================================
// TEST HELPERS FOR HISTORY TESTS
// =============================================================================

#[allow(dead_code)]
pub async fn setup_test_app() -> axum::Router {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"));

    let db = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&db)
        .await
        .expect("Failed to run migrations");

    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "test-secret-key-for-testing-only".to_string());
    let jwt_service = exchange_shared::services::jwt::JwtService::new(jwt_secret);

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_service = RedisService::new(&redis_url);

    let wallet_mnemonic = std::env::var("WALLET_MNEMONIC")
        .unwrap_or_else(|_| "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about".to_string());

    exchange_shared::create_app(db, redis_service, jwt_service, wallet_mnemonic).await
}

#[allow(dead_code)]
pub async fn create_test_user(server: &TestServer, email: &str, password: &str) -> (String, String) {
    use serde_json::json;
    
    // Make email unique by adding random number before @
    let parts: Vec<&str> = email.split('@').collect();
    let unique_email = if parts.len() == 2 {
        format!("{}+{}@{}", parts[0], uuid::Uuid::new_v4().to_string().replace("-", "")[..8].to_string(), parts[1])
    } else {
        format!("test_{}@test.com", uuid::Uuid::new_v4().to_string().replace("-", "")[..8].to_string())
    };
    
    // Register user
    let register_body = json!({
        "email": unique_email,
        "password": password,
        "password_confirm": password
    });
    
    let register_response = server.post("/auth/register").json(&register_body).await;
    assert_eq!(register_response.status_code(), 201, "Failed to register user: {}", register_response.text());
    
    let register_json: serde_json::Value = register_response.json();
    let user_id = register_json["user"]["id"].as_str().unwrap().to_string();
    
    // Login to get token
    let login_body = json!({
        "email": unique_email,
        "password": password
    });
    
    let login_response = server.post("/auth/login").json(&login_body).await;
    assert_eq!(login_response.status_code(), 200, "Failed to login");
    
    let login_json: serde_json::Value = login_response.json();
    let token = login_json["access_token"].as_str().unwrap().to_string();
    
    (user_id, token)
}

#[allow(dead_code)]
pub async fn create_test_swap(_server: &TestServer, user_id: &str, from: &str, to: &str) -> String {
    // Create swap directly in database to avoid slow API calls
    let swap_id = uuid::Uuid::new_v4().to_string();
    let db_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"));
    let db = sqlx::mysql::MySqlPool::connect(&db_url).await.unwrap();
    
    sqlx::query(
        "INSERT INTO swaps (
            id, user_id, provider_id, from_currency, from_network, 
            to_currency, to_network, amount, estimated_receive, rate,
            deposit_address, recipient_address, status, rate_type,
            platform_fee, total_fee, is_sandbox, created_at
        ) VALUES (?, ?, 'changenow', ?, 'mainnet', ?, 'mainnet', 0.1, 1.5, 15.0, 
                  'test_deposit', 'test_recipient', 'waiting', 'floating',
                  0.01, 0.02, 1, NOW())"
    )
    .bind(&swap_id)
    .bind(user_id)
    .bind(from)
    .bind(to)
    .execute(&db)
    .await
    .expect("Failed to create test swap");
    
    swap_id
}
