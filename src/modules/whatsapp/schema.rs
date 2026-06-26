use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiError {
    pub message: String,
}

impl ApiError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WebhookAcceptedResponse {
    pub status: String,
    pub received: usize,
    pub inserted: usize,
    pub duplicates: usize,
}
