use utoipa::{
    openapi::{
        security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
        server::ServerBuilder,
        Server,
    },
    Modify, OpenApi,
};

use crate::{
    __path_assetar_logo, __path_assetar_logo_legacy, __path_health_check, __path_ping, __path_root,
    modules::{
        admin::{
            controller::{
                __path_create_note, __path_dashboard as __path_admin_dashboard,
                __path_export_swaps_csv as __path_admin_export_swaps_csv, __path_finance_summary,
                __path_get_asset_detail, __path_get_giftcard_catalog_item,
                __path_get_provider_detail, __path_global_search, __path_list_assets,
                __path_list_giftcard_catalog, __path_list_providers,
                __path_login as __path_admin_login, __path_ops_health,
                __path_overview as __path_admin_overview, __path_settings,
                __path_settings_diagnostics, __path_sync_assets, __path_sync_providers,
                __path_validate_asset_address, __path_webhook_detail, __path_webhook_monitor,
            },
            schema::{
                AdminErrorResponse, AdminLoginRequest, AdminLoginResponse, AdminOverviewResponse,
                AdminOverviewSwapMetrics, AdminOverviewWhatsAppMetrics, AdminSwapExportQuery,
                AdminUserResponse, OpsAssetDetailQuery, OpsAssetDetailResponse,
                OpsAssetListResponse, OpsAssetQuery, OpsAssetRow, OpsAssetValidateRequest,
                OpsAssetValidateResponse, OpsCreateNoteRequest, OpsDashboardKpis,
                OpsDashboardQuickAccessItem, OpsDashboardRecentActivityItem, OpsDashboardResponse,
                OpsDashboardTopPair, OpsDashboardVolumePoint, OpsFinanceDailyRow,
                OpsFinanceProviderRow, OpsFinanceQuery, OpsFinanceResponse, OpsFinanceTotals,
                OpsGiftCardCatalogDetailQuery, OpsGiftCardCatalogDetailResponse,
                OpsGiftCardCatalogQuery, OpsGiftCardCatalogResponse, OpsHealthResponse,
                OpsNoteResponse, OpsPayoutPolicySettings, OpsProviderDetailResponse,
                OpsProviderHealthRow, OpsProviderListQuery, OpsProviderListResponse,
                OpsProviderSummary, OpsRiskFlag, OpsSearchGiftCardResult, OpsSearchQuery,
                OpsSearchResponse, OpsSearchSupportResult, OpsSearchSwapResult,
                OpsSettingsDiagnosticsResponse, OpsSettingsResponse, OpsSyncResponse,
                OpsWebhookDeliveryRow, OpsWebhookDetailResponse, OpsWebhookMonitorResponse,
                OpsWebhookQuery, OpsWorkerHealth,
            },
        },
        auth::{
            controller::{
                __path_forgot_password, __path_login, __path_logout, __path_me, __path_refresh,
                __path_register, __path_request_verification, __path_reset_password,
                __path_verify_email, __path_verify_email_get,
            },
            schema::{
                ErrorResponse, ForgotPasswordRequest, ForgotPasswordResponse, LoginRequest,
                LoginResponse, LogoutRequest, LogoutResponse, MeResponse, RefreshTokenRequest,
                RefreshTokenResponse, RegisterRequest, RegisterResponse,
                RequestVerificationRequest, RequestVerificationResponse, ResetPasswordRequest,
                ResetPasswordResponse, UserResponse, VerifyEmailQuery, VerifyEmailRequest,
                VerifyEmailResponse,
            },
        },
        giftcard::{
            controller::{
                __path_admin_get_order as __path_giftcard_admin_get_order,
                __path_admin_list_orders as __path_giftcard_admin_list_orders,
                __path_admin_reconcile_order as __path_giftcard_admin_reconcile_order,
                __path_admin_retry_order as __path_giftcard_admin_retry_order,
                __path_admin_reveal_order as __path_giftcard_admin_reveal_order,
                __path_get_giftcard_catalog, __path_get_order_status, __path_get_prepaid_cards,
                __path_order_giftcard, __path_order_prepaid_card,
                __path_trocador_webhook as __path_giftcard_trocador_webhook,
            },
            schema::{
                AdminGiftCardActionResponse, AdminGiftCardOrderDetailResponse,
                AdminGiftCardOrderListResponse, AdminGiftCardOrderQuery, AdminGiftCardOrderSummary,
                AdminGiftCardRevealRequest, AdminGiftCardRevealResponse, CardOrderDetailsResponse,
                CardOrderResponse, CreateGiftCardOrderRequest, CreatePrepaidCardOrderRequest,
                GiftCardCatalogQuery, GiftCardCatalogResponse, GiftCardErrorResponse,
                GiftCardProductResponse, PrepaidCardResponse, PrepaidCardsResponse,
            },
        },
        swap::{
            controller::{
                __path_create_donation_swap, __path_create_swap, __path_get_admin_swap_history,
                __path_get_admin_swap_status, __path_get_admin_swap_timeline,
                __path_get_client_swap_history, __path_get_currencies, __path_get_donation_rates,
                __path_get_donation_target, __path_get_estimate, __path_get_pair_limits,
                __path_get_pairs, __path_get_providers, __path_get_rates, __path_get_swap_history,
                __path_get_swap_status, __path_reconcile_admin_swap,
                __path_refresh_admin_swap_status,
                __path_trocador_webhook as __path_swap_trocador_webhook, __path_validate_address,
            },
            schema::{
                ClientHistoryResponse, CreateDonationSwapRequest, CreateSwapRequest,
                CreateSwapResponse, CurrenciesQuery, CurrencyResponse, DonationRatesQuery,
                DonationTargetResponse, EstimateQuery, EstimateResponse, FiltersApplied,
                HistoryQuery, HistoryResponse, PaginationInfo, PairLimitsQuery, PairLimitsResponse,
                PairResponse, PairsPaginationInfo, PairsQuery, PairsResponse, ProviderResponse,
                ProvidersQuery, RateResponse, RateType, RatesQuery, RatesResponse,
                SwapErrorResponse, SwapOpsActionResponse, SwapStatusResponse, SwapSummary,
                SwapTimelineEvent, SwapTimelineResponse, ValidateAddressRequest,
                ValidateAddressResponse,
            },
            status::SwapStatus,
        },
        whatsapp::{
            controller::{
                __path_get_admin_conversation, __path_list_admin_conversations,
                __path_receive_webhook, __path_update_admin_conversation, __path_verify_webhook,
            },
            schema::{
                AdminConversationDetailResponse, AdminConversationEvent,
                AdminConversationFiltersApplied, AdminConversationListResponse,
                AdminConversationPagination, AdminConversationQuery, AdminConversationSummary,
                AdminOutboundMessage, ApiError, RelatedSwapSummary, UpdateAdminConversationRequest,
                WebhookAcceptedResponse,
            },
        },
    },
    DependencyHealth, HealthChecks, HealthResponse, RpcDependencyHealth,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Assetar API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Assetar backend API for auth, swaps, gift cards, operations, webhooks, and support workflows."
    ),
    paths(
        root,
        ping,
        assetar_logo,
        assetar_logo_legacy,
        health_check,
        admin_login,
        admin_dashboard,
        admin_overview,
        list_assets,
        get_asset_detail,
        sync_assets,
        validate_asset_address,
        list_giftcard_catalog,
        get_giftcard_catalog_item,
        list_providers,
        get_provider_detail,
        sync_providers,
        settings,
        settings_diagnostics,
        admin_export_swaps_csv,
        global_search,
        ops_health,
        finance_summary,
        webhook_monitor,
        webhook_detail,
        create_note,
        register,
        login,
        refresh,
        logout,
        me,
        forgot_password,
        reset_password,
        request_verification,
        verify_email_get,
        verify_email,
        get_prepaid_cards,
        get_giftcard_catalog,
        order_giftcard,
        order_prepaid_card,
        get_order_status,
        giftcard_trocador_webhook,
        giftcard_admin_list_orders,
        giftcard_admin_get_order,
        giftcard_admin_retry_order,
        giftcard_admin_reconcile_order,
        giftcard_admin_reveal_order,
        create_swap,
        get_donation_target,
        get_donation_rates,
        create_donation_swap,
        swap_trocador_webhook,
        get_currencies,
        get_providers,
        get_pairs,
        get_rates,
        get_estimate,
        get_pair_limits,
        get_swap_history,
        get_client_swap_history,
        get_swap_status,
        get_admin_swap_history,
        get_admin_swap_status,
        get_admin_swap_timeline,
        refresh_admin_swap_status,
        reconcile_admin_swap,
        validate_address,
        verify_webhook,
        receive_webhook,
        list_admin_conversations,
        get_admin_conversation,
        update_admin_conversation
    ),
    components(
        schemas(
            HealthResponse,
            HealthChecks,
            DependencyHealth,
            RpcDependencyHealth,
            AdminLoginRequest,
            AdminUserResponse,
            AdminLoginResponse,
            AdminOverviewSwapMetrics,
            AdminOverviewWhatsAppMetrics,
            AdminOverviewResponse,
            OpsDashboardResponse,
            OpsDashboardKpis,
            OpsDashboardQuickAccessItem,
            OpsDashboardRecentActivityItem,
            OpsDashboardVolumePoint,
            OpsDashboardTopPair,
            AdminSwapExportQuery,
            AdminErrorResponse,
            OpsAssetQuery,
            OpsAssetDetailQuery,
            OpsAssetRow,
            OpsAssetListResponse,
            OpsAssetDetailResponse,
            OpsAssetValidateRequest,
            OpsAssetValidateResponse,
            OpsSyncResponse,
            OpsGiftCardCatalogQuery,
            OpsGiftCardCatalogDetailQuery,
            OpsGiftCardCatalogResponse,
            OpsGiftCardCatalogDetailResponse,
            OpsProviderListQuery,
            OpsProviderSummary,
            OpsProviderListResponse,
            OpsProviderDetailResponse,
            OpsSearchQuery,
            OpsSearchSwapResult,
            OpsSearchGiftCardResult,
            OpsSearchSupportResult,
            OpsSearchResponse,
            OpsProviderHealthRow,
            OpsWorkerHealth,
            OpsRiskFlag,
            OpsHealthResponse,
            OpsFinanceQuery,
            OpsFinanceTotals,
            OpsFinanceDailyRow,
            OpsFinanceProviderRow,
            OpsFinanceResponse,
            OpsWebhookQuery,
            OpsWebhookDeliveryRow,
            OpsWebhookMonitorResponse,
            OpsWebhookDetailResponse,
            OpsCreateNoteRequest,
            OpsNoteResponse,
            OpsSettingsResponse,
            OpsSettingsDiagnosticsResponse,
            OpsPayoutPolicySettings,
            RegisterRequest,
            RegisterResponse,
            LoginRequest,
            LoginResponse,
            RefreshTokenRequest,
            RefreshTokenResponse,
            LogoutRequest,
            LogoutResponse,
            MeResponse,
            ForgotPasswordRequest,
            ForgotPasswordResponse,
            ResetPasswordRequest,
            ResetPasswordResponse,
            RequestVerificationRequest,
            RequestVerificationResponse,
            UserResponse,
            VerifyEmailQuery,
            VerifyEmailRequest,
            VerifyEmailResponse,
            ErrorResponse,
            GiftCardCatalogQuery,
            GiftCardCatalogResponse,
            GiftCardProductResponse,
            PrepaidCardsResponse,
            PrepaidCardResponse,
            CreateGiftCardOrderRequest,
            CreatePrepaidCardOrderRequest,
            CardOrderDetailsResponse,
            CardOrderResponse,
            AdminGiftCardOrderQuery,
            AdminGiftCardOrderSummary,
            AdminGiftCardOrderListResponse,
            AdminGiftCardOrderDetailResponse,
            AdminGiftCardRevealRequest,
            AdminGiftCardRevealResponse,
            AdminGiftCardActionResponse,
            GiftCardErrorResponse,
            ProvidersQuery,
            ProviderResponse,
            CurrenciesQuery,
            CurrencyResponse,
            PairsQuery,
            PairResponse,
            PairsPaginationInfo,
            PairsResponse,
            RatesQuery,
            RateType,
            RateResponse,
            RatesResponse,
            DonationTargetResponse,
            DonationRatesQuery,
            EstimateQuery,
            EstimateResponse,
            PairLimitsQuery,
            PairLimitsResponse,
            CreateDonationSwapRequest,
            CreateSwapRequest,
            CreateSwapResponse,
            SwapStatus,
            SwapStatusResponse,
            SwapTimelineEvent,
            SwapTimelineResponse,
            SwapOpsActionResponse,
            HistoryQuery,
            SwapSummary,
            PaginationInfo,
            FiltersApplied,
            HistoryResponse,
            ClientHistoryResponse,
            ValidateAddressRequest,
            ValidateAddressResponse,
            SwapErrorResponse,
            ApiError,
            WebhookAcceptedResponse,
            AdminConversationQuery,
            AdminConversationSummary,
            AdminConversationPagination,
            AdminConversationFiltersApplied,
            AdminConversationListResponse,
            AdminConversationEvent,
            AdminOutboundMessage,
            RelatedSwapSummary,
            AdminConversationDetailResponse,
            UpdateAdminConversationRequest
        )
    ),
    modifiers(&OpenApiAddon),
    tags(
        (name = "System", description = "Service root and health endpoints"),
        (name = "Admin", description = "Administrative authentication endpoints"),
        (name = "Operations", description = "Cross-product admin operations, health, search, reporting, and notes"),
        (name = "Auth", description = "Authentication and email verification endpoints"),
        (name = "Gift Cards", description = "Gift card and prepaid card catalog/order endpoints"),
        (name = "Gift Card Ops", description = "Admin gift card operations and audited sensitive-field access"),
        (name = "Swap", description = "Swap discovery, rate lookup, creation, and history endpoints"),
        (name = "Swap Ops", description = "Admin swap monitoring, timeline, refresh, and reconciliation endpoints"),
        (name = "WhatsApp", description = "WhatsApp webhook endpoints"),
        (name = "Support Ops", description = "Admin support inbox and WhatsApp conversation operations")
    )
)]
pub struct ApiDoc;

struct OpenApiAddon;

fn configured_public_base_url() -> Option<String> {
    std::env::var("PUBLIC_BACKEND_URL")
        .ok()
        .or_else(|| std::env::var("RENDER_EXTERNAL_URL").ok())
        .or_else(|| std::env::var("API_BASE_URL").ok())
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

fn configured_local_base_url() -> String {
    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(3000);
    format!("http://localhost:{}", port)
}

fn configured_servers() -> Vec<Server> {
    let local = configured_local_base_url();
    let mut servers = Vec::new();

    if let Some(public) = configured_public_base_url() {
        servers.push(
            ServerBuilder::new()
                .url(public.clone())
                .description(Some("Public deployment"))
                .build(),
        );

        if public != local {
            servers.push(
                ServerBuilder::new()
                    .url(local)
                    .description(Some("Local Rust backend"))
                    .build(),
            );
        }
    } else {
        servers.push(
            ServerBuilder::new()
                .url(local)
                .description(Some("Local Rust backend"))
                .build(),
        );
    }

    servers
}

impl Modify for OpenApiAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.servers = Some(configured_servers());

        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::ApiDoc;
    use utoipa::OpenApi;

    #[test]
    fn generated_openapi_contains_core_paths() {
        let document = ApiDoc::openapi();
        let paths = &document.paths.paths;

        assert!(paths.contains_key("/health"));
        assert!(paths.contains_key("/ping"));
        assert!(paths.contains_key("/branding/assetar-logo.png"));
        assert!(paths.contains_key("/branding/assetar-logo.jpg"));
        assert!(paths.contains_key("/ops/login"));
        assert!(paths.contains_key("/ops/swaps/export"));
        assert!(paths.contains_key("/ops/overview"));
        assert!(paths.contains_key("/ops/search"));
        assert!(paths.contains_key("/ops/health"));
        assert!(paths.contains_key("/ops/finance/summary"));
        assert!(paths.contains_key("/ops/webhooks"));
        assert!(paths.contains_key("/ops/notes"));
        assert!(paths.contains_key("/swap/ops"));
        assert!(paths.contains_key("/swap/ops/{id}"));
        assert!(paths.contains_key("/swap/ops/{id}/timeline"));
        assert!(paths.contains_key("/swap/ops/{id}/refresh"));
        assert!(paths.contains_key("/swap/ops/{id}/reconcile"));
        assert!(paths.contains_key("/whatsapp/ops/conversations"));
        assert!(paths.contains_key("/whatsapp/ops/conversations/{wa_id}"));
        assert!(paths.contains_key("/auth/register"));
        assert!(paths.contains_key("/auth/login"));
        assert!(paths.contains_key("/auth/refresh"));
        assert!(paths.contains_key("/auth/logout"));
        assert!(paths.contains_key("/auth/me"));
        assert!(paths.contains_key("/auth/forgot-password"));
        assert!(paths.contains_key("/auth/reset-password"));
        assert!(paths.contains_key("/auth/request-verification"));
        assert!(paths.contains_key("/auth/verify-email"));
        assert!(paths.contains_key("/giftcards"));
        assert!(paths.contains_key("/giftcards/prepaid"));
        assert!(paths.contains_key("/giftcards/order"));
        assert!(paths.contains_key("/giftcards/prepaid/order"));
        assert!(paths.contains_key("/giftcards/orders/{trade_id}"));
        assert!(paths.contains_key("/giftcards/webhooks/trocador"));
        assert!(paths.contains_key("/giftcards/ops/orders"));
        assert!(paths.contains_key("/giftcards/ops/orders/{order_ref}"));
        assert!(paths.contains_key("/giftcards/ops/orders/{order_ref}/retry"));
        assert!(paths.contains_key("/giftcards/ops/orders/{order_ref}/reconcile"));
        assert!(paths.contains_key("/giftcards/ops/orders/{order_ref}/reveal"));
        assert!(paths.contains_key("/swap/currencies"));
        assert!(paths.contains_key("/swap/providers"));
        assert!(paths.contains_key("/swap/pairs"));
        assert!(paths.contains_key("/swap/rates"));
        assert!(paths.contains_key("/swap/estimate"));
        assert!(paths.contains_key("/swap/create"));
        assert!(paths.contains_key("/swap/donation/target"));
        assert!(paths.contains_key("/swap/donation/rates"));
        assert!(paths.contains_key("/swap/donation/create"));
        assert!(paths.contains_key("/swap/webhooks/trocador"));
        assert!(paths.contains_key("/swap/{id}"));
        assert!(paths.contains_key("/swap/validate-address"));
        assert!(paths.contains_key("/swap/history"));
        assert!(paths.contains_key("/swap/history/client"));
        assert!(paths.contains_key("/whatsapp/webhook"));
    }

    #[test]
    fn generated_openapi_registers_bearer_auth_scheme() {
        let document = ApiDoc::openapi();
        let components = document.components.expect("components present");

        assert!(components.security_schemes.contains_key("bearer_auth"));
    }

    #[test]
    fn generated_openapi_includes_server_entries() {
        let document = ApiDoc::openapi();
        let servers = document.servers.expect("servers present");

        assert!(!servers.is_empty());
    }
}
