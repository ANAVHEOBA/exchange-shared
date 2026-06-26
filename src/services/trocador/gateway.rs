use crate::modules::{
    giftcard::schema::{TrocadorGiftCardProduct, TrocadorPrepaidCard},
    swap::schema::{
        TrocadorCurrency, TrocadorProvider, TrocadorRatesResponse, TrocadorTradeResponse,
    },
};

use super::{TrocadorClient, TrocadorError};

const ALLOWED_SWAP_MARKUPS: &[&str] = &["0", "1", "1.65", "3"];
const ALLOWED_CARD_MARKUPS: &[&str] = &["1", "2", "3"];

/// Application-facing boundary for Trocador operations.
/// Keeps swap orchestration off the raw HTTP client type.
pub struct TrocadorGateway {
    client: TrocadorClient,
}

impl TrocadorGateway {
    pub fn new(api_key: String) -> Self {
        Self {
            client: TrocadorClient::new(api_key),
        }
    }

    pub fn from_env() -> Result<Self, std::env::VarError> {
        std::env::var("TROCADOR_API_KEY").map(Self::new)
    }

    pub async fn fetch_currencies(&self) -> Result<Vec<TrocadorCurrency>, TrocadorError> {
        self.client.get_currencies().await
    }

    pub async fn fetch_providers(&self) -> Result<Vec<TrocadorProvider>, TrocadorError> {
        self.client.get_providers().await
    }

    pub async fn fetch_prepaid_cards(&self) -> Result<Vec<TrocadorPrepaidCard>, TrocadorError> {
        self.client.get_prepaid_cards().await
    }

    pub async fn fetch_giftcards(
        &self,
        country: Option<&str>,
    ) -> Result<Vec<TrocadorGiftCardProduct>, TrocadorError> {
        self.client.get_giftcards(country).await
    }

    pub async fn fetch_rates(
        &self,
        ticker_from: &str,
        network_from: &str,
        ticker_to: &str,
        network_to: &str,
        amount: f64,
        min_kycrating: Option<&str>,
        markup: Option<&str>,
    ) -> Result<TrocadorRatesResponse, TrocadorError> {
        self.client
            .get_rates(
                ticker_from,
                network_from,
                ticker_to,
                network_to,
                amount,
                min_kycrating,
                markup,
            )
            .await
    }

    pub async fn create_trade(
        &self,
        trade_id: Option<&str>,
        ticker_from: &str,
        network_from: &str,
        ticker_to: &str,
        network_to: &str,
        amount: f64,
        address: &str,
        address_memo: Option<&str>,
        refund: Option<&str>,
        refund_memo: Option<&str>,
        provider: &str,
        fixed: bool,
        payment: bool,
        min_kycrating: Option<&str>,
        webhook: Option<&str>,
        webhook_key: Option<&str>,
        markup: Option<&str>,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        self.client
            .create_trade(
                trade_id,
                ticker_from,
                network_from,
                ticker_to,
                network_to,
                amount,
                address,
                address_memo,
                refund,
                refund_memo,
                provider,
                fixed,
                payment,
                min_kycrating,
                webhook,
                webhook_key,
                markup,
            )
            .await
    }

    pub async fn fetch_trade_status(
        &self,
        trade_id: &str,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        self.client.get_trade_status(trade_id).await
    }

    pub async fn validate_address(
        &self,
        ticker: &str,
        network: &str,
        address: &str,
    ) -> Result<bool, TrocadorError> {
        self.client.validate_address(ticker, network, address).await
    }

    pub async fn order_prepaid_card(
        &self,
        provider: &str,
        currency_code: &str,
        ticker_from: &str,
        network_from: &str,
        amount: f64,
        email: &str,
        webhook: Option<&str>,
        webhook_key: Option<&str>,
        card_markup: Option<&str>,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        self.client
            .order_prepaid_card(
                provider,
                currency_code,
                ticker_from,
                network_from,
                amount,
                email,
                webhook,
                webhook_key,
                card_markup,
            )
            .await
    }

    pub async fn order_giftcard(
        &self,
        product_id: &str,
        ticker_from: &str,
        network_from: &str,
        amount: f64,
        email: &str,
        webhook: Option<&str>,
        webhook_key: Option<&str>,
        card_markup: Option<&str>,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        self.client
            .order_giftcard(
                product_id,
                ticker_from,
                network_from,
                amount,
                email,
                webhook,
                webhook_key,
                card_markup,
            )
            .await
    }
}

pub fn swap_markup_from_env() -> Result<Option<String>, String> {
    normalize_swap_markup(std::env::var("TROCADOR_SWAP_MARKUP").ok().as_deref())
}

pub fn normalize_card_markup(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if ALLOWED_CARD_MARKUPS.contains(&trimmed) {
        return Ok(Some(trimmed.to_string()));
    }

    Err(format!(
        "Invalid card markup '{}'. Allowed values: {}",
        trimmed,
        ALLOWED_CARD_MARKUPS.join(", ")
    ))
}

fn normalize_swap_markup(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if ALLOWED_SWAP_MARKUPS.contains(&trimmed) {
        return Ok(Some(trimmed.to_string()));
    }

    Err(format!(
        "Invalid TROCADOR_SWAP_MARKUP '{}'. Allowed values: {}",
        trimmed,
        ALLOWED_SWAP_MARKUPS.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::{normalize_card_markup, normalize_swap_markup};

    #[test]
    fn empty_markup_is_disabled() {
        assert_eq!(normalize_swap_markup(None).unwrap(), None);
        assert_eq!(normalize_swap_markup(Some("")).unwrap(), None);
        assert_eq!(normalize_swap_markup(Some("   ")).unwrap(), None);
    }

    #[test]
    fn documented_markup_values_are_accepted() {
        assert_eq!(
            normalize_swap_markup(Some("0")).unwrap().as_deref(),
            Some("0")
        );
        assert_eq!(
            normalize_swap_markup(Some("1")).unwrap().as_deref(),
            Some("1")
        );
        assert_eq!(
            normalize_swap_markup(Some("1.65")).unwrap().as_deref(),
            Some("1.65")
        );
        assert_eq!(
            normalize_swap_markup(Some("3")).unwrap().as_deref(),
            Some("3")
        );
    }

    #[test]
    fn invalid_markup_is_rejected() {
        let err = normalize_swap_markup(Some("2")).expect_err("2 is not documented by Trocador");
        assert!(err.contains("TROCADOR_SWAP_MARKUP"));
    }

    #[test]
    fn documented_card_markup_values_are_accepted() {
        assert_eq!(
            normalize_card_markup(Some("1")).unwrap().as_deref(),
            Some("1")
        );
        assert_eq!(
            normalize_card_markup(Some("2")).unwrap().as_deref(),
            Some("2")
        );
        assert_eq!(
            normalize_card_markup(Some("3")).unwrap().as_deref(),
            Some("3")
        );
    }

    #[test]
    fn invalid_card_markup_is_rejected() {
        let err = normalize_card_markup(Some("1.65"))
            .expect_err("1.65 is not documented for gift card markup");
        assert!(err.contains("Invalid card markup"));
    }
}
