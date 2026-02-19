use crate::modules::swap::schema::{TrocadorQuote, RateResponse, RateType, EstimateQuery, EstimateResponse};
use super::strategy::{PricingStrategy, PricingContext, AdaptivePricingStrategy};

pub struct PricingEngine {
    strategy: Box<dyn PricingStrategy>,
}

impl PricingEngine {
    pub fn new() -> Self {
        Self {
            strategy: Box::new(AdaptivePricingStrategy::default()),
        }
    }

    /// Takes raw provider quotes and applies the optimal markup algorithm
    pub fn apply_optimal_markup(
        &self,
        quotes: &[TrocadorQuote],
        amount_from: f64,
        ticker_from: &str, // Changed from _network_to to ticker_from
        gas_cost_native: f64, // Fetched from RpcClient
    ) -> Vec<RateResponse> {
        if quotes.is_empty() {
            return vec![];
        }

        // 1. Calculate Provider Spread (Volatility Index)
        let amounts: Vec<f64> = quotes.iter()
            .map(|q| q.amount_to.parse::<f64>().unwrap_or(0.0))
            .filter(|&a| a > 0.0)
            .collect();

        let max_amount = amounts.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_amount = amounts.iter().fold(f64::MAX, |a, &b| a.min(b));
        let spread = if max_amount > 0.0 { (max_amount - min_amount) / max_amount } else { 0.0 };

        // 2. USD Price Estimation (Heuristic for tiering)
        let usd_price = match ticker_from.to_lowercase().as_str() {
            "btc" => 60000.0,
            "eth" => 3000.0,
            "xmr" => 150.0,
            "usdt" | "usdc" | "dai" => 1.0,
            _ => 1.0, // Default to 1.0 for others (safe side)
        };
        let amount_usd = amount_from * usd_price;

        // 3. Prepare Context
        let ctx = PricingContext {
            amount_usd,
            network_gas_cost_native: gas_cost_native,
            provider_spread_percentage: spread,
        };

        // 4. Get Optimal Rates from Strategy
        let (commission_rate, gas_floor) = self.strategy.calculate_fees(&ctx);

        // 4. Transform and Sort
        let mut results: Vec<RateResponse> = quotes.iter().map(|quote| {
            let amount_to = quote.amount_to.parse::<f64>().unwrap_or(0.0);
            let waste = quote.waste.as_deref().unwrap_or("0.0").parse::<f64>().unwrap_or(0.0);
            
            // MATH: User_Receive = Max(0, Amount_To * (1 - Rate) - Gas_Floor)
            let mut platform_fee = amount_to * commission_rate;
            
            // If platform fee doesn't cover gas floor, bump it up
            if platform_fee < gas_floor {
                platform_fee = gas_floor;
            }

            let final_user_receive = (amount_to - platform_fee).max(0.0);
            
            RateResponse {
                provider: quote.provider.clone(),
                provider_name: quote.provider.clone(),
                rate: if amount_from > 0.0 { final_user_receive / amount_from } else { 0.0 },
                estimated_amount: final_user_receive,
                min_amount: quote.min_amount.unwrap_or(0.0),
                max_amount: quote.max_amount.unwrap_or(0.0),
                network_fee: 0.0,
                provider_fee: waste,
                platform_fee,
                total_fee: waste + platform_fee,
                rate_type: RateType::Floating, // Default
                kyc_required: quote.kycrating.as_deref().unwrap_or("D") != "A",
                kyc_rating: quote.kycrating.clone(),
                eta_minutes: quote.eta.map(|e| e as u32).or(Some(15)),
            }
        }).collect();

        // Sort by best rate for user
        results.sort_by(|a, b| b.estimated_amount.partial_cmp(&a.estimated_amount).unwrap_or(std::cmp::Ordering::Equal));
        
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
            warnings.push(
                "Large trade detected. Actual execution may vary significantly.".to_string()
            );
        }
        
        // Low liquidity warning
        if provider_count < 2 {
            warnings.push(
                "Limited liquidity. Only one provider available.".to_string()
            );
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
        provider_spread: f64,
        amount_usd: f64,
        cached: bool,
        cache_age_seconds: i64,
        expires_in_seconds: i64,
    ) -> EstimateResponse {
        let best_rate = rates.first().expect("No rates available");
        
        // Calculate slippage
        let slippage_pct = self.strategy.estimate_slippage(amount_usd, provider_spread);
        let slippage_amount = best_rate.estimated_amount * slippage_pct;
        
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
            cached,
            cache_age_seconds,
            expires_in_seconds,
            warnings,
        }
    }
}
