use crate::modules::auth::model::User;
use crate::services::{hashing, jwt::JwtService};
use sqlx::{MySql, Pool};

pub struct UserCrud<'a> {
    pool: Pool<MySql>,
    jwt_service: &'a JwtService,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidCredentials,
    UserNotFound,
    EmailNotVerified,
    DatabaseError(String),
    HashingError(String),
    TokenError(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::InvalidCredentials => write!(f, "Invalid credentials"),
            AuthError::UserNotFound => write!(f, "User not found"),
            AuthError::EmailNotVerified => write!(f, "Email not verified"),
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
        Self { pool, jwt_service }
    }

    pub async fn create(&self, user: &User) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO users (id, email, username, password_hash, email_verified, two_factor_enabled, two_factor_secret, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            user.id,
            user.email,
            user.username,
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
                id, email, username, password_hash,
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
                id, email, username, password_hash,
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
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM users WHERE email = ?", email)
            .fetch_one(&self.pool)
            .await?;

        Ok(result > 0)
    }

    pub async fn username_exists(&self, username: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query_scalar!("SELECT COUNT(*) FROM users WHERE username = ?", username)
            .fetch_one(&self.pool)
            .await?;

        Ok(result > 0)
    }

    pub async fn find_by_username(&self, username: &str) -> Result<Option<User>, sqlx::Error> {
        sqlx::query_as!(
            User,
            r#"SELECT 
                id, email, username, password_hash,
                email_verified as "email_verified: bool",
                two_factor_enabled as "two_factor_enabled: bool",
                two_factor_secret,
                created_at, updated_at
            FROM users WHERE username = ?"#,
            username
        )
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn delete_unverified_user(&self, user_id: &str) -> Result<(), sqlx::Error> {
        // Delete email verifications first (foreign key constraint)
        sqlx::query!("DELETE FROM email_verifications WHERE user_id = ?", user_id)
            .execute(&self.pool)
            .await?;

        // Delete the user
        sqlx::query!(
            "DELETE FROM users WHERE id = ? AND email_verified = FALSE",
            user_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn create_email_verification(
        &self,
        user_id: &str,
        token: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query!(
            r#"
            INSERT INTO email_verifications (id, user_id, token, expires_at, created_at)
            VALUES (?, ?, ?, ?, NOW())
            "#,
            id,
            user_id,
            token,
            expires_at
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn verify_email_token(&self, token: &str) -> Result<String, AuthError> {
        let verification = sqlx::query!(
            r#"
            SELECT user_id, expires_at
            FROM email_verifications
            WHERE token = ?
            "#,
            token
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(e.to_string()))?
        .ok_or(AuthError::TokenError(
            "Invalid verification token".to_string(),
        ))?;

        // Check if expired
        if verification.expires_at < chrono::Utc::now() {
            return Err(AuthError::TokenError(
                "Verification token expired".to_string(),
            ));
        }

        // Mark email as verified
        sqlx::query!(
            "UPDATE users SET email_verified = TRUE, updated_at = NOW() WHERE id = ?",
            verification.user_id
        )
        .execute(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        // Delete the verification token
        sqlx::query!("DELETE FROM email_verifications WHERE token = ?", token)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        Ok(verification.user_id)
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<LoginResult, AuthError> {
        let user = self
            .find_by_email(email)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?
            .ok_or(AuthError::InvalidCredentials)?;

        let is_valid = hashing::verify_password(password, &user.password_hash)
            .map_err(|e| AuthError::HashingError(e.to_string()))?;

        if !is_valid {
            return Err(AuthError::InvalidCredentials);
        }

        // Check if email is verified
        if !user.email_verified {
            return Err(AuthError::EmailNotVerified);
        }

        let access_token = self
            .jwt_service
            .create_access_token(&user.id, &user.email)
            .map_err(|e| AuthError::TokenError(e.to_string()))?;

        let refresh_token = self
            .jwt_service
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
