use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use crate::{
    modules::whatsapp::{
        crud::WhatsAppCrud,
        schema::{ApiError, WebhookAcceptedResponse},
    },
    services::whatsapp::{
        extract_normalized_events, WebhookVerificationQuery, WhatsAppWebhookPayload,
    },
    AppState,
};

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
