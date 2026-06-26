use utoipa::{
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    Modify, OpenApi,
};

use crate::{
    __path_health_check, __path_ping, __path_root,
    modules::{
        admin::{
            controller::{
                __path_export_swaps_csv as __path_admin_export_swaps_csv,
                __path_login as __path_admin_login,
            },
            schema::{
                AdminErrorResponse, AdminLoginRequest, AdminLoginResponse, AdminSwapExportQuery,
                AdminUserResponse,
            },
        },
        auth::{
            controller::{__path_login, __path_register, __path_verify_email},
            schema::{
                ErrorResponse, LoginRequest, LoginResponse, RegisterRequest, RegisterResponse,
                UserResponse, VerifyEmailQuery, VerifyEmailResponse,
            },
        },
        giftcard::{
            controller::{
                __path_get_giftcard_catalog, __path_get_order_status, __path_get_prepaid_cards,
                __path_order_giftcard, __path_order_prepaid_card,
            },
            schema::{
                CardOrderDetailsResponse, CardOrderResponse, CreateGiftCardOrderRequest,
                CreatePrepaidCardOrderRequest, GiftCardCatalogQuery, GiftCardCatalogResponse,
                GiftCardErrorResponse, GiftCardProductResponse, PrepaidCardResponse,
                PrepaidCardsResponse,
            },
        },
        swap::{
            controller::{
                __path_create_donation_swap, __path_create_swap, __path_get_client_swap_history,
                __path_get_currencies, __path_get_donation_rates, __path_get_donation_target,
                __path_get_estimate, __path_get_pairs, __path_get_providers, __path_get_rates,
                __path_get_swap_history, __path_get_swap_status, __path_validate_address,
            },
            schema::{
                ClientHistoryResponse, CreateDonationSwapRequest, CreateSwapRequest,
                CreateSwapResponse, CurrenciesQuery, CurrencyResponse, DonationRatesQuery,
                DonationTargetResponse, EstimateQuery, EstimateResponse, FiltersApplied,
                HistoryQuery, HistoryResponse, PaginationInfo, PairResponse, PairsPaginationInfo,
                PairsQuery, PairsResponse, ProviderResponse, ProvidersQuery, RateResponse,
                RateType, RatesQuery, RatesResponse, SwapErrorResponse, SwapStatusResponse,
                SwapSummary, ValidateAddressRequest, ValidateAddressResponse,
            },
            status::SwapStatus,
        },
    },
    DependencyHealth, HealthChecks, HealthResponse, RpcDependencyHealth,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        root,
        ping,
        health_check,
        admin_login,
        admin_export_swaps_csv,
        register,
        login,
        verify_email,
        get_prepaid_cards,
        get_giftcard_catalog,
        order_giftcard,
        order_prepaid_card,
        get_order_status,
        create_swap,
        get_donation_target,
        get_donation_rates,
        create_donation_swap,
        get_currencies,
        get_providers,
        get_pairs,
        get_rates,
        get_estimate,
        get_swap_history,
        get_client_swap_history,
        get_swap_status,
        validate_address
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
            AdminSwapExportQuery,
            AdminErrorResponse,
            RegisterRequest,
            RegisterResponse,
            LoginRequest,
            LoginResponse,
            UserResponse,
            VerifyEmailQuery,
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
            CreateDonationSwapRequest,
            CreateSwapRequest,
            CreateSwapResponse,
            SwapStatus,
            SwapStatusResponse,
            HistoryQuery,
            SwapSummary,
            PaginationInfo,
            FiltersApplied,
            HistoryResponse,
            ClientHistoryResponse,
            ValidateAddressRequest,
            ValidateAddressResponse,
            SwapErrorResponse
        )
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "System", description = "Service root and health endpoints"),
        (name = "Admin", description = "Administrative authentication endpoints"),
        (name = "Auth", description = "Authentication and email verification endpoints"),
        (name = "Gift Cards", description = "Gift card and prepaid card catalog/order endpoints"),
        (name = "Swap", description = "Swap discovery, rate lookup, creation, and history endpoints")
    )
)]
pub struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
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
        assert!(paths.contains_key("/admin/swaps/export"));
        assert!(paths.contains_key("/auth/login"));
        assert!(paths.contains_key("/giftcards"));
        assert!(paths.contains_key("/giftcards/prepaid"));
        assert!(paths.contains_key("/giftcards/order"));
        assert!(paths.contains_key("/giftcards/prepaid/order"));
        assert!(paths.contains_key("/swap/create"));
        assert!(paths.contains_key("/swap/donation/target"));
        assert!(paths.contains_key("/swap/donation/rates"));
        assert!(paths.contains_key("/swap/donation/create"));
        assert!(paths.contains_key("/swap/history"));
        assert!(paths.contains_key("/swap/history/client"));
    }

    #[test]
    fn generated_openapi_registers_bearer_auth_scheme() {
        let document = ApiDoc::openapi();
        let components = document.components.expect("components present");

        assert!(components.security_schemes.contains_key("bearer_auth"));
    }
}
