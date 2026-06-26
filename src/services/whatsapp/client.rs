use std::sync::Arc;

use reqwest::StatusCode;
use thiserror::Error;

use crate::services::whatsapp::{
    verify_meta_signature, SendMessageResponse, SendTextBody, SendTextMessageRequest,
    WhatsAppConfig,
};

#[derive(Debug, Error)]
pub enum WhatsAppError {
    #[error("WhatsApp webhook verification failed")]
    WebhookVerificationFailed,
    #[error("WhatsApp signature missing")]
    MissingSignature,
    #[error("WhatsApp signature invalid")]
    InvalidSignature,
    #[error("Meta Graph API request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Meta Graph API returned {status}: {body}")]
    Api { status: StatusCode, body: String },
}

#[derive(Clone)]
pub struct WhatsAppService {
    config: Arc<WhatsAppConfig>,
    http_client: reqwest::Client,
}

impl WhatsAppService {
    pub fn from_env(http_client: reqwest::Client) -> Result<Option<Self>, String> {
        let config = match WhatsAppConfig::from_env()? {
            Some(config) => config,
            None => return Ok(None),
        };

        Ok(Some(Self {
            config: Arc::new(config),
            http_client,
        }))
    }

    pub fn config(&self) -> &WhatsAppConfig {
        &self.config
    }

    pub fn verify_webhook(
        &self,
        mode: Option<&str>,
        verify_token: Option<&str>,
        challenge: Option<&str>,
    ) -> Result<String, WhatsAppError> {
        if mode != Some("subscribe") {
            return Err(WhatsAppError::WebhookVerificationFailed);
        }

        if verify_token != Some(self.config.verify_token.as_str()) {
            return Err(WhatsAppError::WebhookVerificationFailed);
        }

        challenge
            .map(|value| value.to_string())
            .ok_or(WhatsAppError::WebhookVerificationFailed)
    }

    pub fn verify_signature(
        &self,
        signature_header: Option<&str>,
        body: &[u8],
    ) -> Result<(), WhatsAppError> {
        let signature_header = signature_header.ok_or(WhatsAppError::MissingSignature)?;

        if verify_meta_signature(&self.config.app_secret, signature_header, body) {
            Ok(())
        } else {
            Err(WhatsAppError::InvalidSignature)
        }
    }

    pub async fn send_text_message(
        &self,
        to: &str,
        body: &str,
    ) -> Result<SendMessageResponse, WhatsAppError> {
        let payload = SendTextMessageRequest {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to,
            message_type: "text",
            text: SendTextBody {
                preview_url: false,
                body,
            },
        };

        let response = self
            .http_client
            .post(self.config.messages_endpoint())
            .bearer_auth(&self.config.access_token)
            .json(&payload)
            .send()
            .await?;

        let status = response.status();
        let body_text = response.text().await?;
        if !status.is_success() {
            return Err(WhatsAppError::Api {
                status,
                body: body_text,
            });
        }

        serde_json::from_str(&body_text).map_err(|error| WhatsAppError::Api {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: error.to_string(),
        })
    }
}
