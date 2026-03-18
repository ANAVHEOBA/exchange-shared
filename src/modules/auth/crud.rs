use sqlx::{MySql, Pool};
use crate::modules::auth::model::User;
use crate::services::{hashing, jwt::JwtService};

pub struct UserCrud<'a> {
    pool: Pool<MySql>,
    jwt_service: &'a JwtService,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidCredentials,
    UserNotFound,
    DatabaseError(String),
    HashingError(String),
    TokenError(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
            AuthError::UserNotFound => write!(f, "User not found"),
            AuthError::DatabaseError(e) => write!(f, "Database error: {}", e),
            AuthError::HashingError(e) => write!(f, "Hashing error: {}", e),
            AuthError::TokenError(e) => write!(f, "Token error: {}", e),
        }
    }
}

pub struct LoginResult {
    pub user: User,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
}

impl<'a> UserCrud<'a> {
    pub fn new(pool: Pool<MySql>, jwt_service: &'a JwtService) -> Self {
        Self {
            pool,
            jwt_service,
        }
    }

    pub async fn create(&self, user: &User) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO users (id, email, password_hash, email_verified, two_factor_enabled, two_factor_secret, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            user.id,
            user.email,
            user.password_hash,
            user.email_verified,
            user.two_factor_enabled,
            user.two_factor_secret,
            user.created_at,
            user.updated_at
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn find_by_id(&self, id: &str) -> Result<Option<User>, sqlx::Error> {
        sqlx::query_as!(
            User,
            r#"SELECT 
                id, email, password_hash,
                email_verified as "email_verified: bool",
                two_factor_enabled as "two_factor_enabled: bool",
                two_factor_secret,
                created_at, updated_at
            FROM users WHERE id = ?"#,
            id
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn find_by_email(&self, email: &str) -> Result<Option<User>, sqlx::Error> {
        sqlx::query_as!(
            User,
            r#"SELECT 
                id, email, password_hash,
                email_verified as "email_verified: bool",
                two_factor_enabled as "two_factor_enabled: bool",
                two_factor_secret,
                created_at, updated_at
            FROM users WHERE email = ?"#,
            email
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn email_exists(&self, email: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM users WHERE email = ?",
            email
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result > 0)
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<LoginResult, AuthError> {
        let user = self.find_by_email(email)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?
            .ok_or(AuthError::InvalidCredentials)?;

        let is_valid = hashing::verify_password(password, &user.password_hash)
            .map_err(|e| AuthError::HashingError(e.to_string()))?;

        if !is_valid {
            return Err(AuthError::InvalidCredentials);
        }

        let access_token = self.jwt_service
            .create_access_token(&user.id, &user.email)
            .map_err(|e| AuthError::TokenError(e.to_string()))?;

        let refresh_token = self.jwt_service
            .create_refresh_token(&user.id)
            .map_err(|e| AuthError::TokenError(e.to_string()))?;

        Ok(LoginResult {
            user,
            access_token,
            refresh_token,
            expires_in: self.jwt_service.get_access_token_duration_secs(),
        })
    }
}
