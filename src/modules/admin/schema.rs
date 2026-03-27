use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct AdminLoginRequest {
    #[validate(email(message = "Invalid email format"))]
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminUserResponse {
    pub id: String,
    pub email: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminLoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: &'static str,
    pub expires_in: i64,
    pub admin: AdminUserResponse,
}

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct AdminSwapExportQuery {
    pub provider: Option<String>,
    pub provider_swap_id: Option<String>,
    pub status: Option<String>,
    pub from_currency: Option<String>,
    pub from_network: Option<String>,
    pub to_currency: Option<String>,
    pub to_network: Option<String>,
    pub user_id: Option<String>,
    pub client_id: Option<String>,
    pub date_from: Option<String>,
    pub date_to: Option<String>,
    pub is_sandbox: Option<bool>,
    pub is_payment: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminErrorResponse {
    pub error: String,
}

impl AdminErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}
