use std::sync::Arc;

use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

use crate::services::whatsapp::{
    verify_meta_signature, InteractiveButtonAction, InteractiveListAction, InteractiveListSection,
    InteractiveMediaHeader, InteractiveMediaLink, InteractiveReplyButton,
    InteractiveReplyButtonPayload, InteractiveTextBody, MarkMessageRequest, MarkMessageResponse,
    ReplyButtonOption, SendImageBody, SendImageMessageRequest, SendInteractiveButtonBody,
    SendInteractiveButtonMessageRequest, SendInteractiveListBody,
    SendInteractiveListMessageRequest, SendMessageResponse, SendTextBody, SendTextMessageRequest,
    TypingIndicatorBody, TypingIndicatorRequest, WhatsAppConfig,
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

        self.post_json(&payload).await
    }

    pub async fn send_interactive_list_message(
        &self,
        to: &str,
        body: &str,
        button: &str,
        sections: Vec<InteractiveListSection>,
    ) -> Result<SendMessageResponse, WhatsAppError> {
        let payload = SendInteractiveListMessageRequest {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: to.to_string(),
            message_type: "interactive",
            interactive: SendInteractiveListBody {
                interactive_type: "list",
                body: InteractiveTextBody {
                    text: body.to_string(),
                },
                footer: None,
                action: InteractiveListAction {
                    button: button.to_string(),
                    sections,
                },
            },
        };

        self.post_json(&payload).await
    }

    pub async fn send_image_message(
        &self,
        to: &str,
        image_link: &str,
        caption: Option<&str>,
    ) -> Result<SendMessageResponse, WhatsAppError> {
        let payload = SendImageMessageRequest {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: to.to_string(),
            message_type: "image",
            image: SendImageBody {
                link: image_link.to_string(),
                caption: caption.map(str::to_string),
            },
        };

        self.post_json(&payload).await
    }

    pub async fn send_interactive_button_message(
        &self,
        to: &str,
        body: &str,
        buttons: Vec<ReplyButtonOption>,
        header_image_link: Option<&str>,
    ) -> Result<SendMessageResponse, WhatsAppError> {
        let payload = SendInteractiveButtonMessageRequest {
            messaging_product: "whatsapp",
            recipient_type: "individual",
            to: to.to_string(),
            message_type: "interactive",
            interactive: SendInteractiveButtonBody {
                interactive_type: "button",
                header: header_image_link.map(|link| InteractiveMediaHeader {
                    header_type: "image",
                    image: InteractiveMediaLink {
                        link: link.to_string(),
                    },
                }),
                body: InteractiveTextBody {
                    text: body.to_string(),
                },
                footer: None,
                action: InteractiveButtonAction {
                    buttons: buttons
                        .into_iter()
                        .map(|button| InteractiveReplyButton {
                            button_type: "reply",
                            reply: InteractiveReplyButtonPayload {
                                id: button.id,
                                title: button.title,
                            },
                        })
                        .collect(),
                },
            },
        };

        self.post_json(&payload).await
    }

    pub async fn mark_message_read(&self, message_id: &str) -> Result<(), WhatsAppError> {
        let payload = MarkMessageRequest {
            messaging_product: "whatsapp",
            message_id,
            status: "read",
        };

        let response: MarkMessageResponse = self.post_json(&payload).await?;
        if response.success {
            Ok(())
        } else {
            Err(WhatsAppError::Api {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: "Meta Graph API returned success=false while marking message as read"
                    .to_string(),
            })
        }
    }

    pub async fn send_typing_indicator(&self, message_id: &str) -> Result<(), WhatsAppError> {
        let payload = TypingIndicatorRequest {
            messaging_product: "whatsapp",
            message_id,
            status: "read",
            typing_indicator: TypingIndicatorBody {
                indicator_type: "text",
            },
        };

        let response: MarkMessageResponse = self.post_json(&payload).await?;
        if response.success {
            Ok(())
        } else {
            Err(WhatsAppError::Api {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: "Meta Graph API returned success=false while sending typing indicator"
                    .to_string(),
            })
        }
    }

    async fn post_json<T, R>(&self, payload: &T) -> Result<R, WhatsAppError>
    where
        T: Serialize,
        R: DeserializeOwned,
    {
        let response = self
            .http_client
            .post(self.config.messages_endpoint())
            .bearer_auth(&self.config.access_token)
            .json(payload)
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
