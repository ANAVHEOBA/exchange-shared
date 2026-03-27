use crate::modules::swap::schema::{
    TrocadorCurrency, TrocadorProvider, TrocadorRatesResponse, TrocadorTradeResponse,
};

use super::{TrocadorClient, TrocadorError};

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

    pub async fn fetch_rates(
        &self,
        ticker_from: &str,
        network_from: &str,
        ticker_to: &str,
        network_to: &str,
        amount: f64,
        min_kycrating: Option<&str>,
    ) -> Result<TrocadorRatesResponse, TrocadorError> {
        self.client
            .get_rates(
                ticker_from,
                network_from,
                ticker_to,
                network_to,
                amount,
                min_kycrating,
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
}
