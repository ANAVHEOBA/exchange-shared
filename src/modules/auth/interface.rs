use async_trait::async_trait;
use axum::{
    extract::{FromRequestParts, FromRef},
    http::request::Parts,
};
use std::sync::Arc;

use crate::AppState;
use super::model::{BackupCode, EmailVerification, PasswordReset, RefreshToken, User};

// =============================================================================
// EXTRACTORS
// =============================================================================

pub struct OptionalUser(pub Option<User>);

impl<S> FromRequestParts<S> for OptionalUser
where
    Arc<AppState>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        let state = Arc::from_ref(state);
        let auth_header = parts.headers.get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok());

        if let Some(auth_header) = auth_header {
            if auth_header.starts_with("Bearer ") {
                let token = &auth_header[7..];
                if let Ok(claims) = state.jwt_service.verify_access_token(token) {
                    let user_id = claims.claims.sub;
                    let crud = super::crud::UserCrud::new(state.db.clone(), &state.jwt_service);
                    if let Ok(user) = crud.find_by_id(&user_id).await {
                        return Ok(OptionalUser(user));
                    }
                }
            }
        }

        Ok(OptionalUser(None))
    }
}

// =============================================================================
// REPOSITORY TRAITS
// =============================================================================

pub type AuthResult<T> = std::result::Result<T, AuthError>;

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, user: &User) -> AuthResult<()>;
    async fn find_by_id(&self, id: &str) -> AuthResult<Option<User>>;
    async fn find_by_email(&self, email: &str) -> AuthResult<Option<User>>;
    async fn update(&self, user: &User) -> AuthResult<()>;
    async fn set_email_verified(&self, user_id: &str, verified: bool) -> AuthResult<()>;
    async fn set_two_factor(&self, user_id: &str, enabled: bool, secret: Option<&str>) -> AuthResult<()>;
    async fn update_password(&self, user_id: &str, password_hash: &str) -> AuthResult<()>;
}

#[async_trait]
pub trait RefreshTokenRepository: Send + Sync {
    async fn create(&self, token: &RefreshToken) -> AuthResult<()>;
    async fn find_by_token_hash(&self, token_hash: &str) -> AuthResult<Option<RefreshToken>>;
    async fn revoke(&self, token_hash: &str) -> AuthResult<()>;
    async fn revoke_all_for_user(&self, user_id: &str) -> AuthResult<()>;
    async fn delete_expired(&self) -> AuthResult<u64>;
}

#[async_trait]
pub trait EmailVerificationRepository: Send + Sync {
    async fn create(&self, verification: &EmailVerification) -> AuthResult<()>;
    async fn find_by_token(&self, token: &str) -> AuthResult<Option<EmailVerification>>;
    async fn delete(&self, id: &str) -> AuthResult<()>;
    async fn delete_for_user(&self, user_id: &str) -> AuthResult<()>;
    async fn delete_expired(&self) -> AuthResult<u64>;
}

#[async_trait]
pub trait PasswordResetRepository: Send + Sync {
    async fn create(&self, reset: &PasswordReset) -> AuthResult<()>;
    async fn find_by_token(&self, token: &str) -> AuthResult<Option<PasswordReset>>;
    async fn mark_used(&self, id: &str) -> AuthResult<()>;
    async fn delete_for_user(&self, user_id: &str) -> AuthResult<()>;
    async fn delete_expired(&self) -> AuthResult<u64>;
}

#[async_trait]
pub trait BackupCodeRepository: Send + Sync {
    async fn create_batch(&self, codes: &[BackupCode]) -> AuthResult<()>;
    async fn find_by_user(&self, user_id: &str) -> AuthResult<Vec<BackupCode>>;
    async fn find_unused_by_user(&self, user_id: &str) -> AuthResult<Vec<BackupCode>>;
    async fn mark_used(&self, id: &str) -> AuthResult<()>;
    async fn delete_for_user(&self, user_id: &str) -> AuthResult<()>;
}

// =============================================================================
// SERVICE TRAITS
// =============================================================================

#[async_trait]
pub trait AuthService: Send + Sync {
    async fn register(&self, email: &str, password: &str) -> AuthResult<User>;
    async fn login(&self, email: &str, password: &str, two_factor_code: Option<&str>, backup_code: Option<&str>) -> AuthResult<LoginResult>;
    async fn logout(&self, refresh_token: &str) -> AuthResult<()>;
    async fn refresh_tokens(&self, refresh_token: &str) -> AuthResult<TokenPair>;
    async fn get_current_user(&self, user_id: &str) -> AuthResult<User>;
}

#[async_trait]
pub trait PasswordService: Send + Sync {
    async fn request_reset(&self, email: &str) -> AuthResult<()>;
    async fn reset_password(&self, token: &str, new_password: &str) -> AuthResult<()>;
    fn hash_password(&self, password: &str) -> AuthResult<String>;
    fn verify_password(&self, password: &str, hash: &str) -> AuthResult<bool>;
}

#[async_trait]
pub trait EmailVerificationService: Send + Sync {
    async fn request_verification(&self, user_id: &str) -> AuthResult<()>;
    async fn verify_email(&self, token: &str) -> AuthResult<()>;
}

#[async_trait]
pub trait TwoFactorService: Send + Sync {
    async fn enable(&self, user_id: &str) -> AuthResult<TwoFactorSetup>;
    async fn verify_and_activate(&self, user_id: &str, code: &str) -> AuthResult<Vec<String>>;
    async fn disable(&self, user_id: &str, code: &str) -> AuthResult<()>;
    fn verify_code(&self, secret: &str, code: &str) -> AuthResult<bool>;
}

#[async_trait]
pub trait BackupCodeService: Send + Sync {
    async fn generate(&self, user_id: &str) -> AuthResult<Vec<String>>;
    async fn regenerate(&self, user_id: &str) -> AuthResult<Vec<String>>;
    async fn verify_and_consume(&self, user_id: &str, code: &str) -> AuthResult<bool>;
    async fn get_codes(&self, user_id: &str) -> AuthResult<Vec<String>>;
}

// =============================================================================
// SERVICE RESULT TYPES
// =============================================================================

#[derive(Debug)]
pub enum LoginResult {
    Success(TokenPair),
    Requires2fa(String), // two_factor_token for completing login
}

#[derive(Debug)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
}

#[derive(Debug)]
pub struct TwoFactorSetup {
    pub secret: String,
    pub qr_code: String, // Base64 encoded QR code or otpauth URL
}

// =============================================================================
// ERROR TYPES
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("User not found")]
    UserNotFound,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Email already exists")]
    EmailAlreadyExists,

    #[error("Invalid token")]
    InvalidToken,

    #[error("Token expired")]
    TokenExpired,

    #[error("Email not verified")]
    EmailNotVerified,

    #[error("2FA required")]
    TwoFactorRequired,

    #[error("Invalid 2FA code")]
    InvalidTwoFactorCode,

    #[error("2FA not enabled")]
    TwoFactorNotEnabled,

    #[error("2FA already enabled")]
    TwoFactorAlreadyEnabled,

    #[error("Invalid backup code")]
    InvalidBackupCode,

    #[error("Password too weak: {0}")]
    WeakPassword(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl AuthError {
    pub fn status_code(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            Self::UserNotFound => StatusCode::NOT_FOUND,
            Self::InvalidCredentials => StatusCode::UNAUTHORIZED,
            Self::EmailAlreadyExists => StatusCode::CONFLICT,
            Self::InvalidToken => StatusCode::BAD_REQUEST,
            Self::TokenExpired => StatusCode::BAD_REQUEST,
            Self::EmailNotVerified => StatusCode::FORBIDDEN,
            Self::TwoFactorRequired => StatusCode::OK, // Special case, not an error
            Self::InvalidTwoFactorCode => StatusCode::UNAUTHORIZED,
            Self::TwoFactorNotEnabled => StatusCode::BAD_REQUEST,
            Self::TwoFactorAlreadyEnabled => StatusCode::BAD_REQUEST,
            Self::InvalidBackupCode => StatusCode::UNAUTHORIZED,
            Self::WeakPassword(_) => StatusCode::BAD_REQUEST,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}