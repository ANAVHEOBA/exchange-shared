use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};

pub type DbPool = Pool<MySql>;

pub async fn init_db() -> DbPool {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    MySqlPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to connect to MySQL")
}
