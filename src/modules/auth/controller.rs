use axum::{extract::State, http::StatusCode, Json};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;
use validator::Validate;

use crate::modules::auth::{
    crud::{AuthError, UserCrud},
    model::User,
    schema,
    schema::{
        ErrorResponse, LoginRequest, LoginResponse, RegisterRequest, RegisterResponse,
        UserResponse, VerifyEmailQuery,
    },
};
use crate::services::hashing;
use crate::AppState;

#[utoipa::path(
    post,
    path = "/auth/register",
    tag = "Auth",
    request_body = RegisterRequest,
    responses(
        (status = 201, description = "User registered successfully", body = RegisterResponse),
        (status = 400, description = "Validation error", body = ErrorResponse),
        (status = 409, description = "Email or username already exists", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<(StatusCode, Json<RegisterResponse>), (StatusCode, Json<ErrorResponse>)> {
    if let Err(e) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        ));
    }

    if req.password != req.password_confirm {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Passwords do not match")),
        ));
    }

    if req.password.len() < 8 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Password must be at least 8 characters")),
        ));
    }

    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);

    // Check if email exists and is verified
    if let Some(existing_user) = crud.find_by_email(&req.email).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })? {
        if existing_user.email_verified {
            // Verified account exists - cannot register again
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorResponse::new("Email already registered and verified")),
            ));
        } else {
            // Unverified account exists - delete it so user can start fresh
            crud.delete_unverified_user(&existing_user.id)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse::new(e.to_string())),
                    )
                })?;
            tracing::info!("🗑️  Deleted expired unverified account for: {}", req.email);
        }
    }

    // Check if username is taken by a verified user (unverified usernames are freed up above)
    if let Some(existing_user) = crud.find_by_username(&req.username).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })? {
        if existing_user.email_verified {
            return Err((
                StatusCode::CONFLICT,
                Json(ErrorResponse::new("Username already taken")),
            ));
        } else {
            // Unverified account with this username - delete it too
            crud.delete_unverified_user(&existing_user.id)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse::new(e.to_string())),
                    )
                })?;
            tracing::info!(
                "🗑️  Deleted unverified account with username: {}",
                req.username
            );
        }
    }

    let password_hash = hashing::hash_password(&req.password).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })?;

    let now = Utc::now();
    let user = User {
        id: Uuid::new_v4().to_string(),
        email: req.email.clone(),
        username: Some(req.username.clone()),
        password_hash,
        email_verified: false,
        two_factor_enabled: false,
        two_factor_secret: None,
        created_at: now,
        updated_at: now,
    };

    if let Err(e) = crud.create(&user).await {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        ));
    }

    // Send verification email if email service is configured
    if let Some(email_service) = &state.email_service {
        let verification_token = Uuid::new_v4().to_string();
        let expires_at = Utc::now() + chrono::Duration::hours(24); // 24 hours expiration

        // Save verification token to database
        if let Err(e) = crud
            .create_email_verification(&user.id, &verification_token, expires_at)
            .await
        {
            tracing::error!("Failed to create email verification: {}", e);
            // Don't fail registration, just log the error
        } else {
            // Send email
            if let Err(e) = email_service
                .send_verification_email(&user.email, &req.username, &verification_token)
                .await
            {
                tracing::error!("Failed to send verification email: {}", e);
                // Don't fail registration, just log the error
            } else {
                tracing::info!("✉️  Verification email sent to: {}", user.email);
            }
        }
    }

    Ok((
        StatusCode::CREATED,
        Json(RegisterResponse {
            user: UserResponse {
                id: user.id,
                username: user.username,
                email: user.email,
                email_verified: user.email_verified,
                two_factor_enabled: user.two_factor_enabled,
                created_at: user.created_at,
                updated_at: user.updated_at,
            },
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/auth/login",
    tag = "Auth",
    request_body = LoginRequest,
    responses(
        (status = 200, description = "Login succeeded", body = LoginResponse),
        (status = 401, description = "Invalid credentials", body = ErrorResponse),
        (status = 403, description = "Email not verified", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<(StatusCode, Json<LoginResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);

    let result = crud.login(&req.email, &req.password).await.map_err(|e| {
        match e {
            AuthError::InvalidCredentials => (
                StatusCode::UNAUTHORIZED,
                Json(ErrorResponse::new("Invalid email or password")),
            ),
            AuthError::EmailNotVerified => (
                StatusCode::FORBIDDEN,
                Json(ErrorResponse::new("Please verify your email address before logging in. Check your inbox for the verification link.")),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            ),
        }
    })?;

    Ok((
        StatusCode::OK,
        Json(LoginResponse {
            access_token: result.access_token,
            refresh_token: result.refresh_token,
            token_type: "Bearer",
            expires_in: result.expires_in,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/auth/verify-email",
    tag = "Auth",
    params(VerifyEmailQuery),
    responses(
        (status = 200, description = "Email verified", body = schema::VerifyEmailResponse),
        (status = 400, description = "Missing or invalid token", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn verify_email(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<VerifyEmailQuery>,
) -> Result<(StatusCode, Json<schema::VerifyEmailResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);

    crud.verify_email_token(&params.token).await.map_err(|e| {
        let status = match e {
            AuthError::TokenError(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(ErrorResponse::new(e.to_string())))
    })?;

    Ok((
        StatusCode::OK,
        Json(schema::VerifyEmailResponse {
            message: "Email verified successfully",
        }),
    ))
}
