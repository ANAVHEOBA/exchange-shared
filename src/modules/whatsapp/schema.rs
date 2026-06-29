use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

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

#[derive(Debug, Deserialize, Default, IntoParams, ToSchema)]
#[into_params(parameter_in = Query)]
pub struct AdminConversationQuery {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub admin_status: Option<String>,
    pub admin_tag: Option<String>,
    pub assigned_to: Option<String>,
    pub state: Option<String>,
    pub wa_id: Option<String>,
}

fn default_page() -> u32 {
    1
}

fn default_limit() -> u32 {
    20
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationSummary {
    pub wa_id: String,
    pub phone_number_id: String,
    pub locale: String,
    pub state: String,
    pub admin_status: String,
    pub admin_tag: Option<String>,
    pub assigned_to: Option<String>,
    pub internal_note: Option<String>,
    pub last_inbound_at: Option<DateTime<Utc>>,
    pub last_outbound_at: Option<DateTime<Utc>>,
    pub last_message_preview: Option<String>,
    pub last_outbound_status: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationListResponse {
    pub conversations: Vec<AdminConversationSummary>,
    pub pagination: AdminConversationPagination,
    pub filters_applied: AdminConversationFiltersApplied,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationPagination {
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    pub total_pages: u32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationFiltersApplied {
    pub admin_status: Option<String>,
    pub admin_tag: Option<String>,
    pub assigned_to: Option<String>,
    pub state: Option<String>,
    pub wa_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationEvent {
    pub id: String,
    pub event_kind: String,
    pub message_type: Option<String>,
    pub provider_message_id: Option<String>,
    pub text: Option<String>,
    pub processed: i32,
    pub attempt_count: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminOutboundMessage {
    pub id: String,
    pub message_kind: String,
    pub status: String,
    pub provider_message_id: Option<String>,
    pub body: String,
    pub error_message: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RelatedSwapSummary {
    pub id: String,
    pub status: String,
    pub from_currency: String,
    pub from_network: String,
    pub to_currency: String,
    pub to_network: String,
    pub amount: f64,
    pub estimated_receive: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminConversationDetailResponse {
    pub conversation: AdminConversationSummary,
    pub events: Vec<AdminConversationEvent>,
    pub outbound_messages: Vec<AdminOutboundMessage>,
    pub related_swaps: Vec<RelatedSwapSummary>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UpdateAdminConversationRequest {
    #[validate(length(max = 32))]
    pub admin_status: Option<String>,
    #[validate(length(max = 64))]
    pub admin_tag: Option<String>,
    #[validate(length(max = 128))]
    pub assigned_to: Option<String>,
    #[validate(length(max = 5000))]
    pub internal_note: Option<String>,
}
