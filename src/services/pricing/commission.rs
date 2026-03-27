use super::strategy::{AdaptivePricingStrategy, PricingContext, PricingStrategy};
use crate::modules::swap::schema::TrocadorQuote;
use crate::services::trocador::{TrocadorError, TrocadorGateway};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy)]
pub struct CommissionBreakdown {
    pub amount_usd: f64,
    pub commission_rate: f64,
    pub gas_floor: f64,
    pub platform_fee: f64,
    pub user_receive: f64,
}

pub struct CommissionService {
    strategy: AdaptivePricingStrategy,
}

impl CommissionService {
    pub fn new() -> Self {
        Self {
            strategy: AdaptivePricingStrategy::default(),
        }
    }

    pub fn calculate_quote_spread(&self, quotes: &[TrocadorQuote]) -> f64 {
        let amounts: Vec<f64> = quotes
            .iter()
            .filter_map(|quote| quote.amount_to.parse::<f64>().ok())
            .filter(|amount| *amount > 0.0)
            .collect();

        self.calculate_amount_spread(&amounts)
    }

    pub fn calculate_amount_spread(&self, amounts: &[f64]) -> f64 {
        if amounts.is_empty() {
            return 0.0;
        }

        let max_amount = amounts.iter().copied().fold(0.0f64, f64::max);
        let min_amount = amounts.iter().copied().fold(f64::MAX, f64::min);

        if max_amount > 0.0 {
            (max_amount - min_amount) / max_amount
        } else {
            0.0
        }
    }

    pub async fn resolve_live_amount_usd(
        &self,
        trocador_gateway: &TrocadorGateway,
        ticker_from: &str,
        network_from: &str,
        amount_from: f64,
    ) -> Result<f64, TrocadorError> {
        if amount_from <= 0.0 {
            return Ok(0.0);
        }

        if Self::is_usd_pegged(ticker_from) {
            return Ok(amount_from);
        }

        let mut last_error = None;

        for (reference_ticker, reference_network) in Self::usd_reference_markets() {
            match trocador_gateway
                .fetch_rates(
                    ticker_from,
                    network_from,
                    reference_ticker,
                    reference_network,
                    amount_from,
                    None,
                )
                .await
            {
                Ok(response) => {
                    if let Some(amount_usd) = Self::best_reference_amount(&response.quotes.quotes) {
                        return Ok(amount_usd);
                    }

                    if let Some(amount_to) = response.amount_to {
                        if amount_to.is_finite() && amount_to > 0.0 {
                            return Ok(amount_to);
                        }
                    }
                }
                Err(err) => last_error = Some(err),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            TrocadorError::ApiError(format!(
                "Unable to resolve live USD reference rate for {} on {}",
                ticker_from, network_from
            ))
        }))
    }

    pub fn calculate_commission(
        &self,
        amount_usd: f64,
        amount_to: f64,
        gas_cost_native: f64,
        provider_spread_percentage: f64,
    ) -> CommissionBreakdown {
        let ctx = PricingContext {
            amount_usd,
            network_gas_cost_native: gas_cost_native,
            provider_spread_percentage,
        };

        let (commission_rate, gas_floor) = self.strategy.calculate_fees(&ctx);
        let platform_fee = (amount_to * commission_rate).max(gas_floor).max(0.0);
        let user_receive = (amount_to - platform_fee).max(0.0);

        CommissionBreakdown {
            amount_usd,
            commission_rate,
            gas_floor,
            platform_fee,
            user_receive,
        }
    }

    pub fn estimate_slippage(&self, amount_usd: f64, provider_spread: f64) -> f64 {
        self.strategy.estimate_slippage(amount_usd, provider_spread)
    }

    fn usd_reference_markets() -> &'static [(&'static str, &'static str)] {
        &[
            // Trocador expects its own network labels here, not our internal RPC aliases.
            ("usdt", "ERC20"),
            ("usdt", "TRC20"),
            ("usdt", "MATIC"),
            ("usdt", "BEP20"),
            ("usdc", "ERC20"),
            ("usdc", "MATIC"),
            ("usdc", "BEP20"),
            ("dai", "ERC20"),
            ("dai", "MATIC"),
        ]
    }

    fn is_usd_pegged(ticker: &str) -> bool {
        matches!(ticker.to_lowercase().as_str(), "usdt" | "usdc" | "dai")
    }

    fn best_reference_amount(quotes: &[TrocadorQuote]) -> Option<f64> {
        quotes
            .iter()
            .filter_map(|quote| quote.amount_to.parse::<f64>().ok())
            .filter(|amount| amount.is_finite() && *amount > 0.0)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
    }
}

impl Default for CommissionService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::CommissionService;
    use crate::services::trocador::TrocadorGateway;

    #[tokio::test]
    async fn usd_pegged_assets_use_nominal_amount_without_static_price_table() {
        let service = CommissionService::new();
        let gateway = TrocadorGateway::new("test-api-key".to_string());

        let amount_usd = service
            .resolve_live_amount_usd(&gateway, "USDT", "ethereum", 123.45)
            .await
            .expect("Stablecoin amount should not require an external lookup");

        assert_eq!(amount_usd, 123.45);
    }

    #[test]
    fn commission_uses_supplied_live_amount_usd() {
        let service = CommissionService::new();

        let commission = service.calculate_commission(500.0, 1.0, 0.01, 0.015);

        assert!(commission.amount_usd == 500.0);
        assert!(commission.platform_fee >= 0.01);
    }

    #[test]
    fn usd_reference_markets_use_trocador_network_labels() {
        let markets = CommissionService::usd_reference_markets();

        assert!(markets.contains(&("usdt", "ERC20")));
        assert!(markets.contains(&("usdt", "TRC20")));
        assert!(markets.contains(&("usdc", "MATIC")));
        assert!(markets.contains(&("dai", "ERC20")));

        for (_, network) in markets {
            assert_ne!(*network, "ethereum");
            assert_ne!(*network, "tron");
            assert_ne!(*network, "polygon");
            assert_ne!(*network, "solana");
        }
    }
}
