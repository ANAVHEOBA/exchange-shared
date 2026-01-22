use axum_test::TestServer;
use exchange_shared::services::redis_cache::RedisService;
use sqlx::{MySql, Pool};

// Allow dead_code for utilities used by other test files
#[allow(dead_code)]
pub struct TestContext {
    pub server: TestServer,
    pub db: Pool<MySql>,
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
            .expect("Failed to connect to test database");

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

        let app = exchange_shared::create_app(db.clone(), redis_service, jwt_service).await;
        let server = TestServer::new(app).expect("Failed to create test server");

        Self { server, db }
    }

    pub async fn cleanup(&self) {
        // Clean up test data after each test
        sqlx::query("DELETE FROM refresh_tokens")
            .execute(&self.db)
            .await
            .ok();
        sqlx::query("DELETE FROM backup_codes")
            .execute(&self.db)
            .await
            .ok();
        sqlx::query("DELETE FROM password_resets")
            .execute(&self.db)
            .await
            .ok();
        sqlx::query("DELETE FROM email_verifications")
            .execute(&self.db)
            .await
            .ok();
        sqlx::query("DELETE FROM users")
            .execute(&self.db)
            .await
            .ok();
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
