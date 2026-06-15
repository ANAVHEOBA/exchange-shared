use super::{CommissionService, PricingEngine};
use crate::modules::swap::schema::{
    EstimateQuery, EstimateResponse, RateResponse, RatesQuery, RatesResponse, TrocadorRatesResponse,
};
use crate::services::trocador::{TrocadorError, TrocadorGateway};

pub struct PricedRates {
    pub response: RatesResponse,
    pub provider_spread: f64,
    pub amount_usd: f64,
}

pub struct QuoteService {
    commission_service: CommissionService,
    pricing_engine: PricingEngine,
}

impl QuoteService {
    pub fn new() -> Self {
        Self {
            commission_service: CommissionService::new(),
            pricing_engine: PricingEngine::new(),
        }
    }

    pub async fn price_rates(
        &self,
        query: &RatesQuery,
        trocador_gateway: &TrocadorGateway,
        trocador_response: TrocadorRatesResponse,
        gas_cost_native: f64,
        apply_platform_fee: bool,
    ) -> Result<PricedRates, TrocadorError> {
        let filtered_quotes = trocador_response
            .quotes
            .quotes
            .iter()
            .filter(|quote| {
                let provider_matches = query
                    .provider
                    .as_ref()
                    .map(|provider| quote.provider.eq_ignore_ascii_case(provider))
                    .unwrap_or(true);
                let rate_type_matches = query
                    .rate_type
                    .as_ref()
                    .map(|rate_type| quote.rate_type() == *rate_type)
                    .unwrap_or(true);

                provider_matches && rate_type_matches
            })
            .cloned()
            .collect::<Vec<_>>();

        let provider_spread = self
            .commission_service
            .calculate_quote_spread(&filtered_quotes);

        let amount_usd = self
            .commission_service
            .resolve_live_amount_usd(
                trocador_gateway,
                &query.from,
                &query.network_from,
                query.amount,
            )
            .await?;

        let rates = self.pricing_engine.apply_optimal_markup_with_mode(
            &filtered_quotes,
            query.amount,
            amount_usd,
            &query.from,
            gas_cost_native,
            provider_spread,
            apply_platform_fee,
        );
        let best_rate = rates.first();

        Ok(PricedRates {
            response: RatesResponse {
                trade_id: trocador_response.trade_id,
                from: query.from.clone(),
                network_from: query.network_from.clone(),
                to: query.to.clone(),
                network_to: query.network_to.clone(),
                amount: query.amount,
                amount_to: best_rate.map(|rate| rate.amount_to),
                best_provider: best_rate.map(|rate| rate.provider.clone()),
                best_rate_type: best_rate.map(|rate| rate.rate_type.clone()),
                status: trocador_response.status,
                payment: trocador_response.payment,
                markup: trocador_response.quotes.markup,
                best_only: trocador_response.quotes.best_only,
                min_deposit: trocador_response.quotes.min_deposit,
                max_deposit: trocador_response.quotes.max_deposit,
                kyc_list: trocador_response.quotes.kyc_list,
                logpolicy_list: trocador_response.quotes.logpolicy_list,
                rates,
            },
            provider_spread,
            amount_usd,
        })
    }

    pub fn build_estimate(
        &self,
        query: &EstimateQuery,
        trade_id: Option<String>,
        rates: Vec<RateResponse>,
        provider_spread: f64,
        amount_usd: f64,
        cached: bool,
        cache_age_seconds: i64,
        expires_in_seconds: i64,
    ) -> EstimateResponse {
        self.pricing_engine.build_estimate_response(
            rates,
            query,
            trade_id,
            provider_spread,
            amount_usd,
            cached,
            cache_age_seconds,
            expires_in_seconds,
        )
    }
}

impl Default for QuoteService {
    fn default() -> Self {
        Self::new()
    }
}
