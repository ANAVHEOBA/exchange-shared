use super::commission::CommissionService;
use crate::modules::swap::schema::{
    EstimateQuery, EstimateResponse, RateResponse, RateType, TrocadorQuote,
};

pub struct PricingEngine {
    commission_service: CommissionService,
}

impl PricingEngine {
    fn parse_optional_f64(raw: Option<&str>) -> Option<f64> {
        raw.and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite())
    }

    pub fn new() -> Self {
        Self {
            commission_service: CommissionService::new(),
        }
    }

    /// Takes raw provider quotes and applies the optimal markup algorithm
    pub fn apply_optimal_markup(
        &self,
        quotes: &[TrocadorQuote],
        amount_from: f64,
        amount_usd: f64,
        ticker_from: &str,
        gas_cost_native: f64,
    ) -> Vec<RateResponse> {
        if quotes.is_empty() {
            return vec![];
        }

        let provider_spread = self.commission_service.calculate_quote_spread(quotes);

        self.apply_optimal_markup_with_spread(
            quotes,
            amount_from,
            amount_usd,
            ticker_from,
            gas_cost_native,
            provider_spread,
        )
    }

    pub fn apply_optimal_markup_with_spread(
        &self,
        quotes: &[TrocadorQuote],
        amount_from: f64,
        amount_usd: f64,
        ticker_from: &str,
        gas_cost_native: f64,
        provider_spread: f64,
    ) -> Vec<RateResponse> {
        self.apply_optimal_markup_with_mode(
            quotes,
            amount_from,
            amount_usd,
            ticker_from,
            gas_cost_native,
            provider_spread,
            true,
        )
    }

    pub fn apply_optimal_markup_with_mode(
        &self,
        quotes: &[TrocadorQuote],
        amount_from: f64,
        amount_usd: f64,
        _ticker_from: &str,
        gas_cost_native: f64,
        provider_spread: f64,
        apply_platform_fee: bool,
    ) -> Vec<RateResponse> {
        if quotes.is_empty() {
            return vec![];
        }

        // Transform and sort with a shared commission calculation.
        let mut results: Vec<RateResponse> = quotes
            .iter()
            .map(|quote| {
                let amount_to = quote.amount_to.parse::<f64>().unwrap_or(0.0);
                let spread_percentage = quote
                    .waste
                    .as_deref()
                    .and_then(|value| value.parse::<f64>().ok())
                    .filter(|value| value.is_finite());
                let waste = spread_percentage.unwrap_or(0.0);

                // Ensure provider fee is never negative (Trocador backend issue)
                let provider_fee = waste.max(0.0);
                let commission = self.commission_service.calculate_commission(
                    amount_usd,
                    amount_to,
                    gas_cost_native,
                    provider_spread,
                );
                let network_fee = gas_cost_native.max(0.0);
                // Keep the local fee engine intact for direct-settlement routes only.
                let platform_fee = if apply_platform_fee {
                    commission.platform_fee
                } else {
                    0.0
                };
                let estimated_amount = if apply_platform_fee {
                    (commission.user_receive - network_fee).max(0.0)
                } else {
                    (amount_to - network_fee).max(0.0)
                };
                let amount_to_usd = Self::parse_optional_f64(quote.amount_to_usd.as_deref());
                let estimated_amount_usd = amount_to_usd.and_then(|quoted_usd| {
                    if amount_to > 0.0 {
                        Some((quoted_usd * estimated_amount / amount_to).max(0.0))
                    } else {
                        None
                    }
                });
                let rate_type = quote.rate_type();

                RateResponse {
                    provider: quote.provider.clone(),
                    provider_name: quote.provider.clone(),
                    rate: if amount_from > 0.0 {
                        estimated_amount / amount_from
                    } else {
                        0.0
                    },
                    amount_to,
                    estimated_amount,
                    min_amount: quote.min_amount.unwrap_or(0.0),
                    max_amount: quote.max_amount.unwrap_or(0.0),
                    network_fee,
                    provider_fee,
                    platform_fee,
                    total_fee: provider_fee + platform_fee + network_fee,
                    spread_percentage,
                    rate_type: rate_type.clone(),
                    fixed: rate_type == RateType::Fixed,
                    kyc_required: quote.kycrating.as_deref().unwrap_or("D") != "A",
                    kyc_rating: quote.kycrating.clone(),
                    privacy_rating: quote.kycrating.clone(),
                    logpolicy: quote.logpolicy.clone(),
                    insurance: quote.insurance,
                    provider_logo: quote.provider_logo.clone(),
                    rate_id: quote.rate_id.clone(),
                    amount_from_usd: Self::parse_optional_f64(quote.amount_from_usd.as_deref()),
                    amount_to_usd,
                    estimated_amount_usd,
                    unadjusted_amount_to: quote.unadjusted_amount_to,
                    usd_total_cost_percentage: Self::parse_optional_f64(
                        quote.usd_total_cost_percentage.as_deref(),
                    ),
                    eta_minutes: quote.eta.map(|e| e as u32).or(Some(15)),
                }
            })
            .collect();

        // Sort by best rate for user
        results.sort_by(|a, b| {
            b.estimated_amount
                .partial_cmp(&a.estimated_amount)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results
    }

    /// Generate warnings based on trade conditions
    pub fn generate_warnings(
        &self,
        amount_usd: f64,
        slippage_pct: f64,
        provider_count: usize,
        spread: f64,
    ) -> Vec<String> {
        let mut warnings = Vec::new();

        // High slippage warning
        if slippage_pct > 2.0 {
            warnings.push(format!(
                "High slippage expected: {:.2}%. Consider splitting into smaller trades.",
                slippage_pct
            ));
        }

        // Large trade warning
        if amount_usd > 10000.0 {
            warnings
                .push("Large trade detected. Actual execution may vary significantly.".to_string());
        }

        // Low liquidity warning
        if provider_count < 2 {
            warnings.push("Limited liquidity. Only one provider available.".to_string());
        }

        // High volatility warning
        if spread > 0.05 {
            warnings.push(format!(
                "High price variance across providers ({:.1}%). Market may be volatile.",
                spread * 100.0
            ));
        }

        warnings
    }

    /// Build estimate response from rate responses
    pub fn build_estimate_response(
        &self,
        rates: Vec<RateResponse>,
        query: &EstimateQuery,
        trade_id: Option<String>,
        provider_spread: f64,
        amount_usd: f64,
        cached: bool,
        cache_age_seconds: i64,
        expires_in_seconds: i64,
    ) -> EstimateResponse {
        let best_rate = rates.first().expect("No rates available");

        // Calculate slippage
        let slippage_pct = self
            .commission_service
            .estimate_slippage(amount_usd, provider_spread);
        let slippage_amount = best_rate.estimated_amount * slippage_pct;
        let estimated_receive_usd = best_rate.amount_to_usd.and_then(|quoted_usd| {
            if best_rate.amount_to > 0.0 {
                Some((quoted_usd * best_rate.estimated_amount / best_rate.amount_to).max(0.0))
            } else {
                None
            }
        });

        // Generate warnings
        let warnings = self.generate_warnings(
            amount_usd,
            slippage_pct * 100.0,
            rates.len(),
            provider_spread,
        );

        EstimateResponse {
            from: query.from.clone(),
            to: query.to.clone(),
            amount: query.amount,
            network_from: query.network_from.clone(),
            network_to: query.network_to.clone(),
            best_rate: best_rate.rate,
            estimated_receive: best_rate.estimated_amount,
            estimated_receive_min: (best_rate.estimated_amount - slippage_amount).max(0.0),
            estimated_receive_max: best_rate.estimated_amount + (slippage_amount * 0.5),
            network_fee: best_rate.network_fee,
            provider_fee: best_rate.provider_fee,
            platform_fee: best_rate.platform_fee,
            total_fee: best_rate.total_fee,
            slippage_percentage: slippage_pct * 100.0,
            price_impact: provider_spread * 100.0,
            best_provider: best_rate.provider.clone(),
            provider_count: rates.len(),
            trade_id,
            rate_type: Some(best_rate.rate_type.clone()),
            fixed: Some(best_rate.fixed),
            kyc_required: Some(best_rate.kyc_required),
            kyc_rating: best_rate.kyc_rating.clone(),
            privacy_rating: best_rate.privacy_rating.clone(),
            logpolicy: best_rate.logpolicy.clone(),
            insurance: best_rate.insurance,
            provider_logo: best_rate.provider_logo.clone(),
            rate_id: best_rate.rate_id.clone(),
            spread_percentage: best_rate.spread_percentage,
            amount_from_usd: best_rate.amount_from_usd.or(Some(amount_usd)),
            amount_to: Some(best_rate.amount_to),
            amount_to_usd: best_rate.amount_to_usd,
            estimated_receive_usd,
            unadjusted_amount_to: best_rate.unadjusted_amount_to,
            usd_total_cost_percentage: best_rate.usd_total_cost_percentage,
            cached,
            cache_age_seconds,
            expires_in_seconds,
            warnings,
        }
    }
}
