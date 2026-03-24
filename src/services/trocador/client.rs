use reqwest::Client;

use crate::modules::swap::schema::{TrocadorCurrency, TrocadorProvider, TrocadorTradeResponse};

/// Trocador API client
/// Handles all communication with Trocador.app API
pub struct TrocadorClient {
    client: Client,
    api_key: String,
    base_url: String,
}

#[derive(Debug)]
pub enum TrocadorError {
    HttpError(String),
    ParseError(String),
    ApiError(String),
}

impl std::fmt::Display for TrocadorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrocadorError::HttpError(e) => write!(f, "HTTP error: {}", e),
            TrocadorError::ParseError(e) => write!(f, "Parse error: {}", e),
            TrocadorError::ApiError(e) => write!(f, "API error: {}", e),
        }
    }
}

impl std::error::Error for TrocadorError {}

impl TrocadorClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url: "https://api.trocador.app".to_string(),
        }
    }

    /// Fetch all currencies from Trocador /coins endpoint
    pub async fn get_currencies(&self) -> Result<Vec<TrocadorCurrency>, TrocadorError> {
        let url = format!("{}/coins", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TrocadorError::ApiError(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        let currencies: Vec<TrocadorCurrency> = response
            .json()
            .await
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        Ok(currencies)
    }

    /// Fetch all providers from Trocador /exchanges endpoint
    pub async fn get_providers(&self) -> Result<Vec<TrocadorProvider>, TrocadorError> {
        let url = format!("{}/exchanges", self.base_url);

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TrocadorError::ApiError(format!(
                "API returned status: {}",
                response.status()
            )));
        }

        // Trocador returns { "list": [...] } not a direct array
        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        let providers_array = response_json
            .get("list")
            .ok_or_else(|| TrocadorError::ParseError("Missing 'list' key".to_string()))?;

        let providers: Vec<TrocadorProvider> = serde_json::from_value(providers_array.clone())
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        Ok(providers)
    }

    /// Get rates from Trocador (new_rate)
    pub async fn get_rates(
        &self,
        ticker_from: &str,
        network_from: &str,
        ticker_to: &str,
        network_to: &str,
        amount: f64,
        min_kycrating: Option<&str>,
    ) -> Result<crate::modules::swap::schema::TrocadorRatesResponse, TrocadorError> {
        let url = format!("{}/new_rate", self.base_url);

        let mut params = vec![
            ("ticker_from", ticker_from.to_string()),
            ("network_from", network_from.to_string()),
            ("ticker_to", ticker_to.to_string()),
            ("network_to", network_to.to_string()),
            ("amount_from", amount.to_string()),
            ("best_only", "false".to_string()),
        ];

        if let Some(rating) = min_kycrating {
            params.push(("min_kycrating", rating.to_string()));
        }

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .query(&params)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(TrocadorError::ApiError(format!(
                "API returned error: {}",
                error_text
            )));
        }

        let rates_response: crate::modules::swap::schema::TrocadorRatesResponse = response
            .json()
            .await
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        Ok(rates_response)
    }

    /// Create a new trade on Trocador (new_trade)
    pub async fn create_trade(
        &self,
        trade_id: Option<&str>,
        ticker_from: &str,
        network_from: &str,
        ticker_to: &str,
        network_to: &str,
        amount: f64,
        address: &str,
        refund: Option<&str>,
        provider: &str,
        fixed: bool,
        payment: bool,
        min_kycrating: Option<&str>,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        let url = format!("{}/new_trade", self.base_url);

        let mut params = vec![
            ("ticker_from", ticker_from.to_string()),
            ("network_from", network_from.to_string()),
            ("ticker_to", ticker_to.to_string()),
            ("network_to", network_to.to_string()),
            ("amount_from", amount.to_string()),
            ("address", address.to_string()),
            ("provider", provider.to_string()),
            ("fixed", fixed.to_string()),
            ("payment", payment.to_string()),
        ];

        if let Some(id) = trade_id {
            params.push(("id", id.to_string()));
        }

        if let Some(r) = refund {
            params.push(("refund", r.to_string()));
        }

        if let Some(rating) = min_kycrating {
            params.push(("min_kycrating", rating.to_string()));
        }

        // Log the full request details
        tracing::info!("🔵 Trocador create_trade request:");
        tracing::info!("  URL: {}", url);
        tracing::info!("  Parameters:");
        for (key, value) in &params {
            if key == &"address" || key == &"refund" {
                tracing::info!(
                    "    {} = {}...{}",
                    key,
                    &value[..value.len().min(8)],
                    &value[value.len().saturating_sub(4)..]
                );
            } else {
                tracing::info!("    {} = {}", key, value);
            }
        }

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .query(&params)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        let status = response.status();
        tracing::info!("🔵 Trocador response status: {}", status);

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            tracing::error!("🔴 Trocador API error response: {}", error_text);
            return Err(TrocadorError::ApiError(format!(
                "API returned error: {}",
                error_text
            )));
        }

        let response_text = response
            .text()
            .await
            .map_err(|e| TrocadorError::ParseError(format!("Failed to read response: {}", e)))?;

        tracing::info!("🔵 Trocador raw response: {}", response_text);

        let trade_response: TrocadorTradeResponse = serde_json::from_str(&response_text)
            .map_err(|e| TrocadorError::ParseError(format!("Failed to parse response: {}", e)))?;

        tracing::info!(
            "✅ Trocador trade created successfully: trade_id={}",
            trade_response.trade_id
        );

        Ok(trade_response)
    }

    /// Get trade status from Trocador (trade)
    pub async fn get_trade_status(
        &self,
        trade_id: &str,
    ) -> Result<TrocadorTradeResponse, TrocadorError> {
        let url = format!("{}/trade", self.base_url);

        let params = [("id", trade_id.to_string())];

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .query(&params)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(TrocadorError::ApiError(format!(
                "API returned error: {}",
                error_text
            )));
        }

        let trade_response: TrocadorTradeResponse = response
            .json()
            .await
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        Ok(trade_response)
    }

    /// Validate address for a specific coin and network
    pub async fn validate_address(
        &self,
        ticker: &str,
        network: &str,
        address: &str,
    ) -> Result<bool, TrocadorError> {
        let url = format!("{}/validateaddress", self.base_url);

        let params = [
            ("ticker", ticker.to_string()),
            ("network", network.to_string()),
            ("address", address.to_string()),
        ];

        let response = self
            .client
            .get(&url)
            .header("API-Key", &self.api_key)
            .query(&params)
            .send()
            .await
            .map_err(|e| TrocadorError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(TrocadorError::ApiError(format!(
                "API returned error: {}",
                error_text
            )));
        }

        let response_json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| TrocadorError::ParseError(e.to_string()))?;

        let is_valid = response_json
            .get("result")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(is_valid)
    }
}
