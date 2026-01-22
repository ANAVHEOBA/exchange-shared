use std::env;

/// Environment configuration
/// Loads and validates environment variables
pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub jwt_secret: String,
    pub trocador_api_key: String,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        dotenvy::dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .map_err(|_| "DATABASE_URL must be set".to_string())?;

        let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

        let jwt_secret = env::var("JWT_SECRET")
            .map_err(|_| "JWT_SECRET must be set".to_string())?;

        let trocador_api_key = env::var("TROCADOR_API_KEY")
            .map_err(|_| "TROCADOR_API_KEY must be set".to_string())?;

        Ok(Self {
            database_url,
            redis_url,
            jwt_secret,
            trocador_api_key,
        })
    }

    pub fn trocador_api_key(&self) -> &str {
        &self.trocador_api_key
    }
}
