use axum::{extract::State, http::StatusCode, Json};
use chrono::Utc;
use std::sync::Arc;
use uuid::Uuid;
use validator::Validate;

use crate::modules::auth::{
    crud::{AuthError, UserCrud},
    model::User as UserModel,
    schema,
    schema::{
        ErrorResponse, ForgotPasswordRequest, ForgotPasswordResponse, LoginRequest,
        LoginResponse, LogoutRequest, LogoutResponse, MeResponse, RefreshTokenRequest,
        RefreshTokenResponse, RegisterRequest, RegisterResponse, RequestVerificationRequest,
        RequestVerificationResponse, ResetPasswordRequest, ResetPasswordResponse, UserResponse,
        VerifyEmailQuery, VerifyEmailRequest,
    },
};
use crate::services::hashing;
use crate::{middleware::user::User as AuthenticatedUser, AppState};

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
    let user = UserModel {
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
    post,
    path = "/auth/refresh",
    tag = "Auth",
    request_body = RefreshTokenRequest,
    responses(
        (status = 200, description = "Tokens refreshed", body = RefreshTokenResponse),
        (status = 401, description = "Invalid refresh token", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn refresh(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RefreshTokenRequest>,
) -> Result<(StatusCode, Json<RefreshTokenResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
    let user_id = crud.validate_refresh_token(&req.refresh_token).await.map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::new("Invalid refresh token")),
        )
    })?;

    let user = crud.find_by_id(&user_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })?
    .ok_or((
        StatusCode::UNAUTHORIZED,
        Json(ErrorResponse::new("User not found")),
    ))?;

    crud.revoke_refresh_token(&req.refresh_token)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
        })?;

    let token_pair = crud.issue_token_pair(&user).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })?;

    Ok((
        StatusCode::OK,
        Json(RefreshTokenResponse {
            access_token: token_pair.access_token,
            refresh_token: token_pair.refresh_token,
            token_type: "Bearer",
            expires_in: token_pair.expires_in,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/auth/logout",
    tag = "Auth",
    security(
        ("bearer_auth" = [])
    ),
    request_body = LogoutRequest,
    responses(
        (status = 200, description = "Logout succeeded", body = LogoutResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn logout(
    State(state): State<Arc<AppState>>,
    AuthenticatedUser(user): AuthenticatedUser,
    Json(req): Json<LogoutRequest>,
) -> Result<(StatusCode, Json<LogoutResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
    let belongs_to_user = crud
        .refresh_token_belongs_to_user(&user.id, &req.refresh_token)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
        })?;

    if !belongs_to_user {
        return Err((
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse::new("Invalid refresh token")),
        ));
    }

    crud.revoke_refresh_token(&req.refresh_token)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
        })?;

    Ok((
        StatusCode::OK,
        Json(LogoutResponse {
            message: "Logged out successfully",
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/auth/me",
    tag = "Auth",
    security(
        ("bearer_auth" = [])
    ),
    responses(
        (status = 200, description = "Current user profile", body = MeResponse),
        (status = 401, description = "Missing or invalid token", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn me(
    State(state): State<Arc<AppState>>,
    AuthenticatedUser(user): AuthenticatedUser,
) -> Result<(StatusCode, Json<MeResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
    let stats = crud.get_dashboard_stats(&user.id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })?;

    Ok((
        StatusCode::OK,
        Json(MeResponse {
            email: user.email,
            username: user.username,
            total_trades: stats.total_trades.max(0) as u64,
            traded_value_btc: stats.traded_value_btc,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/auth/forgot-password",
    tag = "Auth",
    request_body = ForgotPasswordRequest,
    responses(
        (status = 200, description = "Reset requested", body = ForgotPasswordResponse),
        (status = 400, description = "Validation error", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn forgot_password(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ForgotPasswordRequest>,
) -> Result<(StatusCode, Json<ForgotPasswordResponse>), (StatusCode, Json<ErrorResponse>)> {
    if let Err(e) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        ));
    }

    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
    if let Some(user) = crud.find_by_email(&req.email).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })? {
        let reset_token = Uuid::new_v4().to_string();
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        if let Err(e) = crud
            .create_password_reset(&user.id, &reset_token, expires_at)
            .await
        {
            tracing::error!("Failed to create password reset token: {}", e);
        } else if let Some(email_service) = &state.email_service {
            let username = user.username.as_deref().unwrap_or("there");
            if let Err(e) = email_service
                .send_password_reset_email(&user.email, username, &reset_token)
                .await
            {
                tracing::error!("Failed to send password reset email: {}", e);
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(ForgotPasswordResponse {
            message: "If that email exists, a reset link has been sent.",
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/auth/reset-password",
    tag = "Auth",
    request_body = ResetPasswordRequest,
    responses(
        (status = 200, description = "Password reset succeeded", body = ResetPasswordResponse),
        (status = 400, description = "Invalid token or password", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn reset_password(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ResetPasswordRequest>,
) -> Result<(StatusCode, Json<ResetPasswordResponse>), (StatusCode, Json<ErrorResponse>)> {
    if req.token.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Token is required")),
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
    let user_id = crud.consume_password_reset(&req.token).await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("Invalid or expired reset token")),
        )
    })?;

    let password_hash = hashing::hash_password(&req.password).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })?;

    crud.update_password(&user_id, &password_hash)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
        })?;

    crud.revoke_all_refresh_tokens_for_user(&user_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(e.to_string())),
            )
        })?;

    Ok((
        StatusCode::OK,
        Json(ResetPasswordResponse {
            message: "Password reset successfully",
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/auth/request-verification",
    tag = "Auth",
    request_body = RequestVerificationRequest,
    responses(
        (status = 200, description = "Verification requested", body = RequestVerificationResponse),
        (status = 400, description = "Validation error", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn request_verification(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RequestVerificationRequest>,
) -> Result<(StatusCode, Json<RequestVerificationResponse>), (StatusCode, Json<ErrorResponse>)> {
    if let Err(e) = req.validate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(e.to_string())),
        ));
    }

    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);
    if let Some(user) = crud.find_by_email(&req.email).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(e.to_string())),
        )
    })? {
        if !user.email_verified {
            let verification_token = Uuid::new_v4().to_string();
            let expires_at = Utc::now() + chrono::Duration::hours(24);

            if let Err(e) = crud
                .create_email_verification(&user.id, &verification_token, expires_at)
                .await
            {
                tracing::error!("Failed to create verification token: {}", e);
            } else if let Some(email_service) = &state.email_service {
                let username = user.username.as_deref().unwrap_or("there");
                if let Err(e) = email_service
                    .send_verification_email(&user.email, username, &verification_token)
                    .await
                {
                    tracing::error!("Failed to resend verification email: {}", e);
                }
            }
        }
    }

    Ok((
        StatusCode::OK,
        Json(RequestVerificationResponse {
            message: "If the account exists and is unverified, a verification email has been sent."
                .to_string(),
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
        (status = 409, description = "Verification token already used", body = ErrorResponse),
        (status = 410, description = "Verification token expired", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn verify_email_get(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<VerifyEmailQuery>,
) -> Result<(StatusCode, Json<schema::VerifyEmailResponse>), (StatusCode, Json<ErrorResponse>)> {
    verify_email_token(state, params.token).await
}

#[utoipa::path(
    post,
    path = "/auth/verify-email",
    tag = "Auth",
    request_body = VerifyEmailRequest,
    responses(
        (status = 200, description = "Email verified", body = schema::VerifyEmailResponse),
        (status = 400, description = "Missing or invalid token", body = ErrorResponse),
        (status = 409, description = "Verification token already used", body = ErrorResponse),
        (status = 410, description = "Verification token expired", body = ErrorResponse),
        (status = 500, description = "Server error", body = ErrorResponse)
    )
)]
pub async fn verify_email(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VerifyEmailRequest>,
) -> Result<(StatusCode, Json<schema::VerifyEmailResponse>), (StatusCode, Json<ErrorResponse>)> {
    verify_email_token(state, req.token).await
}

async fn verify_email_token(
    state: Arc<AppState>,
    token: String,
) -> Result<(StatusCode, Json<schema::VerifyEmailResponse>), (StatusCode, Json<ErrorResponse>)> {
    let crud = UserCrud::new(state.db.clone(), &state.jwt_service);

    crud.verify_email_token(&token)
        .await
        .map_err(map_verify_email_error)?;

    Ok((
        StatusCode::OK,
        Json(schema::VerifyEmailResponse {
            message: "Email verified successfully",
        }),
    ))
}

fn map_verify_email_error(error: AuthError) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        AuthError::InvalidVerificationToken => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::with_code(
                "Invalid verification token",
                "invalid",
            )),
        ),
        AuthError::ExpiredVerificationToken => (
            StatusCode::GONE,
            Json(ErrorResponse::with_code(
                "Verification token expired",
                "expired",
            )),
        ),
        AuthError::VerificationTokenAlreadyUsed => (
            StatusCode::CONFLICT,
            Json(ErrorResponse::with_code(
                "Verification token already used",
                "already_used",
            )),
        ),
        other => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse::new(other.to_string())),
        ),
    }
}
