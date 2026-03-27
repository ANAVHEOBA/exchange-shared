use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

// =============================================================================
// REGISTER
// =============================================================================

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct RegisterRequest {
    #[validate(length(
        min = 3,
        max = 50,
        message = "Username must be between 3 and 50 characters"
    ))]
    pub username: String,
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub password: String,
    pub password_confirm: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RegisterResponse {
    pub user: UserResponse,
}

// =============================================================================
// LOGIN
// =============================================================================

#[derive(Debug, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    #[serde(default)]
    pub two_factor_code: Option<String>,
    #[serde(default)]
    pub backup_code: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: &'static str,
    pub expires_in: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct LoginRequires2faResponse {
    pub requires_2fa: bool,
    pub two_factor_token: String,
}

// =============================================================================
// LOGOUT
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct LogoutResponse {
    pub message: &'static str,
}

// =============================================================================
// REFRESH TOKEN
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Serialize)]
pub struct RefreshTokenResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: &'static str,
    pub expires_in: i64,
}

// =============================================================================
// ME (Current User)
// =============================================================================

#[derive(Debug, Serialize, ToSchema)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub username: Option<String>,
    pub email_verified: bool,
    pub two_factor_enabled: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

// =============================================================================
// PASSWORD RESET
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct ForgotPasswordRequest {
    pub email: String,
}

#[derive(Debug, Serialize)]
pub struct ForgotPasswordResponse {
    pub message: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct ResetPasswordRequest {
    pub token: String,
    pub password: String,
    pub password_confirm: String,
}

#[derive(Debug, Serialize)]
pub struct ResetPasswordResponse {
    pub message: &'static str,
}

// =============================================================================
// EMAIL VERIFICATION
// =============================================================================

#[derive(Debug, Serialize)]
pub struct RequestVerificationResponse {
    pub message: String,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct VerifyEmailQuery {
    pub token: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct VerifyEmailRequest {
    pub token: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct VerifyEmailResponse {
    pub message: &'static str,
}

// =============================================================================
// TWO-FACTOR AUTHENTICATION
// =============================================================================

#[derive(Debug, Serialize)]
pub struct Enable2faResponse {
    pub secret: String,
    pub qr_code: String,
}

#[derive(Debug, Deserialize)]
pub struct Verify2faRequest {
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct Verify2faResponse {
    pub message: &'static str,
    pub backup_codes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Disable2faRequest {
    pub code: String,
}

#[derive(Debug, Serialize)]
pub struct Disable2faResponse {
    pub message: &'static str,
}

// =============================================================================
// BACKUP CODES
// =============================================================================

#[derive(Debug, Serialize)]
pub struct BackupCodesResponse {
    pub codes: Vec<String>,
}

// =============================================================================
// ERROR RESPONSE
// =============================================================================

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            message: None,
        }
    }

    pub fn with_code(error: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: Some(code.into()),
            message: None,
        }
    }

    pub fn with_message(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            code: None,
            message: Some(message.into()),
        }
    }

    pub fn with_code_message(
        error: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            error: error.into(),
            code: Some(code.into()),
            message: Some(message.into()),
        }
    }
}
