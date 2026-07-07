use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
pub struct WebhookVerificationQuery {
    #[serde(rename = "hub.mode")]
    pub mode: Option<String>,
    #[serde(rename = "hub.verify_token")]
    pub verify_token: Option<String>,
    #[serde(rename = "hub.challenge")]
    pub challenge: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WhatsAppWebhookPayload {
    pub object: String,
    #[serde(default)]
    pub entry: Vec<WhatsAppEntry>,
}

#[derive(Debug, Deserialize)]
pub struct WhatsAppEntry {
    pub id: String,
    #[serde(default)]
    pub changes: Vec<WhatsAppChange>,
}

#[derive(Debug, Deserialize)]
pub struct WhatsAppChange {
    pub field: String,
    pub value: Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChangeEnvelope {
    pub metadata: Option<WebhookMetadata>,
    #[serde(default)]
    pub contacts: Vec<WebhookContact>,
    #[serde(default)]
    pub messages: Vec<InboundMessage>,
    #[serde(default)]
    pub statuses: Vec<StatusUpdate>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WebhookMetadata {
    pub phone_number_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WebhookContact {
    pub wa_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InboundMessage {
    pub from: String,
    pub id: String,
    pub timestamp: Option<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub text: Option<TextBody>,
    pub button: Option<ButtonPayload>,
    pub interactive: Option<InteractivePayload>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TextBody {
    pub body: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ButtonPayload {
    pub text: Option<String>,
    pub payload: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InteractivePayload {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub button_reply: Option<InteractiveReply>,
    pub list_reply: Option<InteractiveReply>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InteractiveReply {
    pub id: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StatusUpdate {
    pub id: String,
    pub status: String,
    pub timestamp: Option<String>,
    pub recipient_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NormalizedWebhookEvent {
    pub dedupe_key: String,
    pub phone_number_id: String,
    pub wa_id: Option<String>,
    pub provider_message_id: Option<String>,
    pub event_kind: String,
    pub message_type: Option<String>,
    pub event_timestamp: Option<String>,
    pub text_preview: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Serialize)]
pub struct SendTextMessageRequest<'a> {
    pub messaging_product: &'static str,
    pub recipient_type: &'static str,
    pub to: &'a str,
    #[serde(rename = "type")]
    pub message_type: &'static str,
    pub text: SendTextBody<'a>,
}

#[derive(Debug, Serialize)]
pub struct SendTextBody<'a> {
    pub preview_url: bool,
    pub body: &'a str,
}

#[derive(Debug, Serialize)]
pub struct MarkMessageRequest<'a> {
    pub messaging_product: &'static str,
    pub message_id: &'a str,
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct TypingIndicatorRequest<'a> {
    pub messaging_product: &'static str,
    pub message_id: &'a str,
    pub status: &'static str,
    pub typing_indicator: TypingIndicatorBody,
}

#[derive(Debug, Serialize)]
pub struct TypingIndicatorBody {
    #[serde(rename = "type")]
    pub indicator_type: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct InteractiveListOption {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReplyButtonOption {
    pub id: String,
    pub title: String,
}

#[derive(Debug, Serialize)]
pub struct SendInteractiveListMessageRequest {
    pub messaging_product: &'static str,
    pub recipient_type: &'static str,
    pub to: String,
    #[serde(rename = "type")]
    pub message_type: &'static str,
    pub interactive: SendInteractiveListBody,
}

#[derive(Debug, Serialize)]
pub struct SendInteractiveListBody {
    #[serde(rename = "type")]
    pub interactive_type: &'static str,
    pub body: InteractiveTextBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer: Option<InteractiveTextBody>,
    pub action: InteractiveListAction,
}

#[derive(Debug, Serialize)]
pub struct InteractiveTextBody {
    pub text: String,
}

#[derive(Debug, Serialize)]
pub struct InteractiveListAction {
    pub button: String,
    pub sections: Vec<InteractiveListSection>,
}

#[derive(Debug, Serialize)]
pub struct InteractiveListSection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub rows: Vec<InteractiveListOption>,
}

#[derive(Debug, Serialize)]
pub struct SendImageMessageRequest {
    pub messaging_product: &'static str,
    pub recipient_type: &'static str,
    pub to: String,
    #[serde(rename = "type")]
    pub message_type: &'static str,
    pub image: SendImageBody,
}

#[derive(Debug, Serialize)]
pub struct SendImageBody {
    pub link: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SendInteractiveButtonMessageRequest {
    pub messaging_product: &'static str,
    pub recipient_type: &'static str,
    pub to: String,
    #[serde(rename = "type")]
    pub message_type: &'static str,
    pub interactive: SendInteractiveButtonBody,
}

#[derive(Debug, Serialize)]
pub struct SendInteractiveButtonBody {
    #[serde(rename = "type")]
    pub interactive_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<InteractiveMediaHeader>,
    pub body: InteractiveTextBody,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer: Option<InteractiveTextBody>,
    pub action: InteractiveButtonAction,
}

#[derive(Debug, Serialize)]
pub struct InteractiveMediaHeader {
    #[serde(rename = "type")]
    pub header_type: &'static str,
    pub image: InteractiveMediaLink,
}

#[derive(Debug, Serialize)]
pub struct InteractiveMediaLink {
    pub link: String,
}

#[derive(Debug, Serialize)]
pub struct InteractiveButtonAction {
    pub buttons: Vec<InteractiveReplyButton>,
}

#[derive(Debug, Serialize)]
pub struct InteractiveReplyButton {
    #[serde(rename = "type")]
    pub button_type: &'static str,
    pub reply: InteractiveReplyButtonPayload,
}

#[derive(Debug, Serialize)]
pub struct InteractiveReplyButtonPayload {
    pub id: String,
    pub title: String,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageResponse {
    #[serde(default)]
    pub messages: Vec<SendMessageId>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageId {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct MarkMessageResponse {
    #[serde(default)]
    pub success: bool,
}

pub fn extract_normalized_events(payload: &WhatsAppWebhookPayload) -> Vec<NormalizedWebhookEvent> {
    let mut events = Vec::new();

    for entry in &payload.entry {
        for (change_index, change) in entry.changes.iter().enumerate() {
            let envelope = match serde_json::from_value::<ChangeEnvelope>(change.value.clone()) {
                Ok(value) => value,
                Err(_) => {
                    events.push(NormalizedWebhookEvent {
                        dedupe_key: sha256_hex(&format!(
                            "unknown:{}:{}:{}",
                            entry.id, change.field, change_index
                        )),
                        phone_number_id: String::new(),
                        wa_id: None,
                        provider_message_id: None,
                        event_kind: "unknown".to_string(),
                        message_type: Some(change.field.clone()),
                        event_timestamp: None,
                        text_preview: None,
                        payload: change.value.clone(),
                    });
                    continue;
                }
            };

            let phone_number_id = envelope
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.phone_number_id.clone())
                .unwrap_or_default();

            let fallback_wa_id = envelope
                .contacts
                .first()
                .and_then(|contact| contact.wa_id.clone());

            let had_messages = !envelope.messages.is_empty();
            let had_statuses = !envelope.statuses.is_empty();

            for message in envelope.messages.into_iter() {
                let text_preview = extract_message_preview(&message);
                let interactive_kind = message
                    .interactive
                    .as_ref()
                    .and_then(|payload| payload.kind.clone());
                let payload_json = serde_json::to_value(&message).unwrap_or(Value::Null);

                events.push(NormalizedWebhookEvent {
                    dedupe_key: sha256_hex(&format!("message:{}:{}", phone_number_id, message.id)),
                    phone_number_id: phone_number_id.clone(),
                    wa_id: Some(message.from.clone()),
                    provider_message_id: Some(message.id),
                    event_kind: "message".to_string(),
                    message_type: Some(interactive_kind.unwrap_or(message.kind)),
                    event_timestamp: message.timestamp,
                    text_preview,
                    payload: payload_json,
                });
            }

            for status in envelope.statuses.into_iter() {
                let payload_json = serde_json::to_value(&status).unwrap_or(Value::Null);

                events.push(NormalizedWebhookEvent {
                    dedupe_key: sha256_hex(&format!("status:{}:{}", phone_number_id, status.id)),
                    phone_number_id: phone_number_id.clone(),
                    wa_id: status
                        .recipient_id
                        .clone()
                        .or_else(|| fallback_wa_id.clone()),
                    provider_message_id: Some(status.id),
                    event_kind: "status".to_string(),
                    message_type: Some(status.status),
                    event_timestamp: status.timestamp,
                    text_preview: None,
                    payload: payload_json,
                });
            }

            if !had_messages && !had_statuses {
                events.push(NormalizedWebhookEvent {
                    dedupe_key: sha256_hex(&format!(
                        "change:{}:{}:{}",
                        entry.id, change.field, change_index
                    )),
                    phone_number_id: phone_number_id.clone(),
                    wa_id: fallback_wa_id.clone(),
                    provider_message_id: None,
                    event_kind: "unknown".to_string(),
                    message_type: Some(change.field.clone()),
                    event_timestamp: None,
                    text_preview: None,
                    payload: change.value.clone(),
                });
            }
        }
    }

    events
}

fn extract_message_preview(message: &InboundMessage) -> Option<String> {
    if let Some(text) = message
        .text
        .as_ref()
        .map(|value| value.body.trim().to_string())
    {
        return Some(text);
    }

    if let Some(button) = &message.button {
        if let Some(text) = button.text.clone().filter(|value| !value.trim().is_empty()) {
            return Some(text);
        }

        if let Some(payload) = button
            .payload
            .clone()
            .filter(|value| !value.trim().is_empty())
        {
            return Some(payload);
        }
    }

    if let Some(interactive) = &message.interactive {
        if let Some(reply) = &interactive.button_reply {
            if let Some(id) = reply.id.clone().filter(|value| !value.trim().is_empty()) {
                return Some(id);
            }

            if let Some(title) = reply.title.clone().filter(|value| !value.trim().is_empty()) {
                return Some(title);
            }
        }

        if let Some(reply) = &interactive.list_reply {
            if let Some(id) = reply.id.clone().filter(|value| !value.trim().is_empty()) {
                return Some(id);
            }

            if let Some(title) = reply.title.clone().filter(|value| !value.trim().is_empty()) {
                return Some(title);
            }

            if let Some(description) = reply
                .description
                .clone()
                .filter(|value| !value.trim().is_empty())
            {
                return Some(description);
            }
        }
    }

    None
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}
