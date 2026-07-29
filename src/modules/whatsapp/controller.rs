use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use validator::Validate;

use crate::{
    middleware::admin::Admin,
    modules::swap::{crud::SwapCrud, schema::HistoryQuery},
    modules::whatsapp::{
        crud::{AdminConversationRecord, WhatsAppCrud},
        schema::{
            AdminConversationDetailResponse, AdminConversationEvent,
            AdminConversationFiltersApplied, AdminConversationListResponse,
            AdminConversationPagination, AdminConversationQuery, AdminConversationSummary,
            AdminOutboundMessage, ApiError, RelatedSwapSummary, UpdateAdminConversationRequest,
            WebhookAcceptedResponse,
        },
    },
    services::whatsapp::{
        derive_whatsapp_client_id, extract_normalized_events, WebhookVerificationQuery,
        WhatsAppWebhookPayload,
    },
    AppState,
};

fn swap_crud(state: &Arc<AppState>) -> SwapCrud {
    SwapCrud::new(
        state.db.clone(),
        None,
        state.wallet_mnemonic.clone(),
        state.rpc_manager.clone(),
        state.payout_policy.clone(),
    )
}

fn map_conversation_summary(record: AdminConversationRecord) -> AdminConversationSummary {
    AdminConversationSummary {
        wa_id: record.wa_id,
        phone_number_id: record.phone_number_id,
        locale: record.locale,
        state: record.state,
        admin_status: record.admin_status,
        admin_tag: record.admin_tag,
        assigned_to: record.assigned_to,
        internal_note: record.internal_note,
        last_inbound_at: record.last_inbound_at,
        last_outbound_at: record.last_outbound_at,
        last_message_preview: record.last_message_preview,
        last_outbound_status: record.last_outbound_status,
        last_error: record.last_error,
        updated_at: record.updated_at,
    }
}

fn normalize_patch_field<'a>(value: Option<&'a String>) -> Option<Option<&'a str>> {
    value.map(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

#[utoipa::path(
    get,
    path = "/whatsapp/webhook",
    tag = "WhatsApp",
    responses(
        (status = 200, description = "Meta webhook verified", body = String),
        (status = 403, description = "Webhook verification failed")
    )
)]
pub async fn verify_webhook(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WebhookVerificationQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiError>)> {
    let service = state.whatsapp_service.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("WhatsApp is not configured")),
        )
    })?;

    let challenge = service
        .verify_webhook(
            query.mode.as_deref(),
            query.verify_token.as_deref(),
            query.challenge.as_deref(),
        )
        .map_err(|_| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError::new("Webhook verification failed")),
            )
        })?;

    Ok((StatusCode::OK, challenge))
}

#[utoipa::path(
    post,
    path = "/whatsapp/webhook",
    tag = "WhatsApp",
    request_body = Object,
    responses(
        (status = 200, description = "Webhook accepted", body = WebhookAcceptedResponse),
        (status = 400, description = "Invalid payload", body = ApiError),
        (status = 401, description = "Invalid signature", body = ApiError)
    )
)]
pub async fn receive_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<WebhookAcceptedResponse>, (StatusCode, Json<ApiError>)> {
    let service = state.whatsapp_service.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("WhatsApp is not configured")),
        )
    })?;

    service
        .verify_signature(
            headers
                .get("x-hub-signature-256")
                .and_then(|value| value.to_str().ok()),
            body.as_ref(),
        )
        .map_err(|error| {
            (
                StatusCode::UNAUTHORIZED,
                Json(ApiError::new(error.to_string())),
            )
        })?;

    let payload: WhatsAppWebhookPayload = serde_json::from_slice(&body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(format!(
                "invalid WhatsApp payload: {}",
                error
            ))),
        )
    })?;

    if payload.object != "whatsapp_business_account" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("unsupported webhook object")),
        ));
    }

    let events = extract_normalized_events(&payload);
    let crud = WhatsAppCrud::new(state.db.clone());

    let mut inserted = 0usize;
    let mut duplicates = 0usize;

    for event in &events {
        let inserted_event = match crud.insert_event(event).await {
            Ok(true) => {
                inserted += 1;
                true
            }
            Ok(false) => {
                duplicates += 1;
                false
            }
            Err(error) => {
                tracing::error!("failed to persist WhatsApp event: {}", error);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::new("failed to persist WhatsApp event")),
                ));
            }
        };

        if event.event_kind == "message" && inserted_event {
            if let Some(wa_id) = event.wa_id.as_deref() {
                if let Err(error) = crud
                    .touch_session(
                        wa_id,
                        &event.phone_number_id,
                        event.provider_message_id.as_deref(),
                    )
                    .await
                {
                    tracing::error!("failed to update WhatsApp session: {}", error);
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::new("failed to update WhatsApp session")),
                    ));
                }
            }
        } else if event.event_kind == "status"
            && inserted_event
            && event.provider_message_id.is_some()
            && event.message_type.is_some()
        {
            if let Err(error) = crud
                .mark_outbound_status(
                    event.provider_message_id.as_deref().unwrap_or_default(),
                    event.message_type.as_deref().unwrap_or("unknown"),
                )
                .await
            {
                tracing::warn!(
                    "failed to record WhatsApp outbound status update: {}",
                    error
                );
            }

            if let Err(error) = crud
                .mark_event_processed_by_dedupe_key(&event.dedupe_key)
                .await
            {
                tracing::warn!(
                    "failed to mark WhatsApp status event as processed: {}",
                    error
                );
            }
        } else if inserted_event && event.event_kind != "message" {
            if let Err(error) = crud
                .mark_event_processed_by_dedupe_key(&event.dedupe_key)
                .await
            {
                tracing::warn!("failed to mark WhatsApp event as processed: {}", error);
            }
        }
    }

    tracing::info!(
        "WhatsApp webhook accepted: received={} inserted={} duplicates={}",
        events.len(),
        inserted,
        duplicates
    );

    Ok(Json(WebhookAcceptedResponse {
        status: "accepted".to_string(),
        received: events.len(),
        inserted,
        duplicates,
    }))
}

#[utoipa::path(
    get,
    path = "/whatsapp/ops/conversations",
    tag = "Support Ops",
    params(AdminConversationQuery),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin WhatsApp conversation list", body = AdminConversationListResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 500, description = "Server error", body = ApiError)
    )
)]
pub async fn list_admin_conversations(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Query(query): Query<AdminConversationQuery>,
) -> Result<Json<AdminConversationListResponse>, (StatusCode, Json<ApiError>)> {
    let crud = WhatsAppCrud::new(state.db.clone());
    let (records, total) = crud
        .list_admin_conversations(&query)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new(error.to_string())),
            )
        })?;

    let limit = query.limit.clamp(1, 100);
    let total_pages = if total == 0 {
        0
    } else {
        ((total + limit as u64 - 1) / limit as u64) as u32
    };

    Ok(Json(AdminConversationListResponse {
        conversations: records.into_iter().map(map_conversation_summary).collect(),
        pagination: AdminConversationPagination {
            page: query.page.max(1),
            limit,
            total,
            total_pages,
        },
        filters_applied: AdminConversationFiltersApplied {
            admin_status: query.admin_status,
            admin_tag: query.admin_tag,
            assigned_to: query.assigned_to,
            state: query.state,
            wa_id: query.wa_id,
        },
    }))
}

#[utoipa::path(
    get,
    path = "/whatsapp/ops/conversations/{wa_id}",
    tag = "Support Ops",
    params(
        ("wa_id" = String, Path, description = "WhatsApp user id")
    ),
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Admin WhatsApp conversation detail", body = AdminConversationDetailResponse),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Conversation not found", body = ApiError),
        (status = 500, description = "Server error", body = ApiError)
    )
)]
pub async fn get_admin_conversation(
    State(state): State<Arc<AppState>>,
    _admin: Admin,
    Path(wa_id): Path<String>,
) -> Result<Json<AdminConversationDetailResponse>, (StatusCode, Json<ApiError>)> {
    let crud = WhatsAppCrud::new(state.db.clone());
    let record = crud.get_admin_conversation(&wa_id).await.map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::new(error.to_string())),
        )
    })?;

    let Some(record) = record else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::new("Conversation not found")),
        ));
    };

    let summary = map_conversation_summary(record);
    let events = crud
        .list_conversation_events(&summary.wa_id, 50)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new(error.to_string())),
            )
        })?
        .into_iter()
        .map(|event| AdminConversationEvent {
            id: event.id,
            event_kind: event.event_kind,
            message_type: event.message_type,
            provider_message_id: event.provider_message_id,
            text: event.text,
            processed: event.processed,
            attempt_count: event.attempt_count,
            last_error: event.last_error,
            created_at: event.created_at,
        })
        .collect();

    let outbound_messages = crud
        .list_conversation_outbound_messages(&summary.wa_id, 50)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new(error.to_string())),
            )
        })?
        .into_iter()
        .map(|message| AdminOutboundMessage {
            id: message.id,
            message_kind: message.message_kind,
            status: message.status,
            provider_message_id: message.provider_message_id,
            body: message.body,
            error_message: message.error_message,
            sent_at: message.sent_at,
            created_at: message.created_at,
        })
        .collect();

    let client_id = derive_whatsapp_client_id(&summary.phone_number_id, &summary.wa_id);
    let related_swaps = swap_crud(&state)
        .get_swap_history_for_client(
            &client_id,
            HistoryQuery {
                cursor: None,
                limit: 20,
                status: None,
                from_currency: None,
                to_currency: None,
                provider: None,
                date_from: None,
                date_to: None,
                sort_by: None,
                sort_order: None,
            },
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new(error.to_string())),
            )
        })?
        .swaps
        .into_iter()
        .map(|swap| RelatedSwapSummary {
            id: swap.id,
            status: swap.status.to_string(),
            from_currency: swap.from_currency,
            from_network: swap.from_network,
            to_currency: swap.to_currency,
            to_network: swap.to_network,
            amount: swap.amount,
            estimated_receive: swap.estimated_receive,
            created_at: swap.created_at,
        })
        .collect();

    Ok(Json(AdminConversationDetailResponse {
        conversation: summary,
        events,
        outbound_messages,
        related_swaps,
    }))
}

#[utoipa::path(
    patch,
    path = "/whatsapp/ops/conversations/{wa_id}",
    tag = "Support Ops",
    params(
        ("wa_id" = String, Path, description = "WhatsApp user id")
    ),
    request_body = UpdateAdminConversationRequest,
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Updated admin WhatsApp conversation", body = AdminConversationSummary),
        (status = 400, description = "Invalid update payload", body = ApiError),
        (status = 401, description = "Missing or invalid admin token", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 403, description = "Admin access required", body = crate::modules::admin::schema::AdminErrorResponse),
        (status = 404, description = "Conversation not found", body = ApiError),
        (status = 500, description = "Server error", body = ApiError)
    )
)]
pub async fn update_admin_conversation(
    State(state): State<Arc<AppState>>,
    admin: Admin,
    Path(wa_id): Path<String>,
    Json(payload): Json<UpdateAdminConversationRequest>,
) -> Result<Json<AdminConversationSummary>, (StatusCode, Json<ApiError>)> {
    if let Err(error) = payload.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(error.to_string())),
        ));
    }

    if let Some(admin_status) = payload.admin_status.as_deref() {
        let allowed = [
            "open",
            "contacted",
            "waiting_user",
            "pricing",
            "accepted",
            "rejected",
            "paid",
            "closed",
        ];
        if !allowed.contains(&admin_status.trim()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiError::new("Invalid admin_status value")),
            ));
        }
    }

    //aa

    let crud = WhatsAppCrud::new(state.db.clone());
    let assigned_to = match normalize_patch_field(payload.assigned_to.as_ref()) {
        Some(Some(value)) if value.eq_ignore_ascii_case("me") => Some(Some(admin.email.as_str())),
        other => other,
    };

    let updated = crud
        .update_admin_conversation(
            &wa_id,
            payload.admin_status.as_deref().map(str::trim),
            normalize_patch_field(payload.admin_tag.as_ref()),
            assigned_to,
            normalize_patch_field(payload.internal_note.as_ref()),
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new(error.to_string())),
            )
        })?;

    if !updated {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::new("Conversation not found")),
        ));
    }

    let record = crud.get_admin_conversation(&wa_id).await.map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::new(error.to_string())),
        )
    })?;

    let Some(record) = record else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::new("Conversation not found")),
        ));
    };

    Ok(Json(map_conversation_summary(record)))
}
