use crate::modules::auth::model::User;
use crate::services::{hashing, jwt::JwtService};
use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, MySql, Pool};
use uuid::Uuid;

pub struct UserCrud<'a> {
    pool: Pool<MySql>,
    jwt_service: &'a JwtService,
}

#[derive(Debug)]
pub enum AuthError {
    InvalidCredentials,
    UserNotFound,
    EmailNotVerified,
    InvalidVerificationToken,
    ExpiredVerificationToken,
    VerificationTokenAlreadyUsed,
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
            AuthError::InvalidVerificationToken => write!(f, "Invalid verification token"),
            AuthError::ExpiredVerificationToken => write!(f, "Verification token expired"),
            AuthError::VerificationTokenAlreadyUsed => {
                write!(f, "Verification token already used")
            }
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

#[derive(Debug, FromRow)]
pub struct UserDashboardStats {
    pub total_trades: i64,
    pub traded_value_btc: f64,
}

impl<'a> UserCrud<'a> {
    fn hash_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        format!("{:x}", hasher.finalize())
    }

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

    pub async fn get_dashboard_stats(
        &self,
        user_id: &str,
    ) -> Result<UserDashboardStats, sqlx::Error> {
        sqlx::query_as::<_, UserDashboardStats>(
            r#"
            SELECT
                COUNT(*) as total_trades,
                CAST(
                    COALESCE(
                        SUM(
                            CASE
                                WHEN LOWER(from_currency) = 'btc' AND status = 'completed'
                                    THEN amount
                                ELSE 0
                            END
                        ),
                        0
                    ) AS DOUBLE
                ) as traded_value_btc
            FROM swaps
            WHERE user_id = ?
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
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

    pub async fn store_refresh_token(
        &self,
        user_id: &str,
        refresh_token: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        let token_hash = Self::hash_token(refresh_token);
        sqlx::query(
            r#"
            INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at, revoked, created_at)
            VALUES (?, ?, ?, ?, FALSE, NOW())
            "#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(user_id)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn revoke_refresh_token(&self, refresh_token: &str) -> Result<u64, sqlx::Error> {
        let token_hash = Self::hash_token(refresh_token);
        let result = sqlx::query(
            "UPDATE refresh_tokens SET revoked = TRUE WHERE token_hash = ? AND revoked = FALSE",
        )
        .bind(token_hash)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn revoke_all_refresh_tokens_for_user(
        &self,
        user_id: &str,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE refresh_tokens SET revoked = TRUE WHERE user_id = ? AND revoked = FALSE",
        )
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn refresh_token_belongs_to_user(
        &self,
        user_id: &str,
        refresh_token: &str,
    ) -> Result<bool, sqlx::Error> {
        let token_hash = Self::hash_token(refresh_token);
        let exists = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM refresh_tokens
            WHERE user_id = ?
              AND token_hash = ?
              AND revoked = FALSE
              AND expires_at > NOW()
            "#,
        )
        .bind(user_id)
        .bind(token_hash)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists > 0)
    }

    pub async fn validate_refresh_token(&self, refresh_token: &str) -> Result<String, AuthError> {
        let claims = self
            .jwt_service
            .verify_refresh_token(refresh_token)
            .map_err(|_| AuthError::InvalidCredentials)?;

        let token_hash = Self::hash_token(refresh_token);
        let row = sqlx::query_as::<_, (String, bool, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT user_id, revoked, expires_at
            FROM refresh_tokens
            WHERE token_hash = ?
            LIMIT 1
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(e.to_string()))?
        .ok_or(AuthError::InvalidCredentials)?;

        let (user_id, revoked, expires_at) = row;
        if revoked || expires_at <= Utc::now() || claims.claims.sub != user_id {
            return Err(AuthError::InvalidCredentials);
        }

        Ok(user_id)
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
        let verification = sqlx::query_as::<
            _,
            (
                String,
                chrono::DateTime<chrono::Utc>,
                Option<chrono::DateTime<chrono::Utc>>,
                bool,
            ),
        >(
            r#"
            SELECT ev.user_id, ev.expires_at, ev.used_at, u.email_verified
            FROM email_verifications ev
            JOIN users u ON u.id = ev.user_id
            WHERE ev.token = ?
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(e.to_string()))?
        .ok_or(AuthError::InvalidVerificationToken)?;

        let (user_id, expires_at, used_at, email_verified) = verification;

        if used_at.is_some() || email_verified {
            return Err(AuthError::VerificationTokenAlreadyUsed);
        }

        if expires_at < chrono::Utc::now() {
            return Err(AuthError::ExpiredVerificationToken);
        }

        sqlx::query("UPDATE users SET email_verified = TRUE, updated_at = NOW() WHERE id = ?")
            .bind(&user_id)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        sqlx::query("UPDATE email_verifications SET used_at = NOW() WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        Ok(user_id)
    }

    pub async fn create_password_reset(
        &self,
        user_id: &str,
        token: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM password_resets WHERE user_id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO password_resets (id, user_id, token, expires_at, used, created_at)
            VALUES (?, ?, ?, ?, FALSE, NOW())
            "#,
        )
        .bind(Uuid::new_v4().to_string())
        .bind(user_id)
        .bind(token)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn consume_password_reset(&self, token: &str) -> Result<String, AuthError> {
        let row = sqlx::query_as::<_, (String, chrono::DateTime<chrono::Utc>, bool)>(
            r#"
            SELECT user_id, expires_at, used
            FROM password_resets
            WHERE token = ?
            LIMIT 1
            "#,
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| AuthError::DatabaseError(e.to_string()))?
        .ok_or(AuthError::InvalidVerificationToken)?;

        let (user_id, expires_at, used) = row;

        if used || expires_at <= Utc::now() {
            return Err(AuthError::ExpiredVerificationToken);
        }

        sqlx::query("UPDATE password_resets SET used = TRUE WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        Ok(user_id)
    }

    pub async fn update_password(
        &self,
        user_id: &str,
        password_hash: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE users SET password_hash = ?, updated_at = NOW() WHERE id = ?")
            .bind(password_hash)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn issue_token_pair(&self, user: &User) -> Result<LoginResult, AuthError> {
        let access_token = self
            .jwt_service
            .create_access_token(&user.id, &user.email)
            .map_err(|e| AuthError::TokenError(e.to_string()))?;

        let refresh_token = self
            .jwt_service
            .create_refresh_token(&user.id)
            .map_err(|e| AuthError::TokenError(e.to_string()))?;

        let refresh_expires_at = Utc::now() + Duration::days(7);
        self.store_refresh_token(&user.id, &refresh_token, refresh_expires_at)
            .await
            .map_err(|e| AuthError::DatabaseError(e.to_string()))?;

        Ok(LoginResult {
            user: user.clone(),
            access_token,
            refresh_token,
            expires_in: self.jwt_service.get_access_token_duration_secs(),
        })
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

        self.issue_token_pair(&user).await
    }
}
