use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct PricingContext {
    pub amount_usd: f64,
    pub network_gas_cost_native: f64,
    pub provider_spread_percentage: f64,
}

#[async_trait]
pub trait PricingStrategy: Send + Sync {
    /// Calculate the optimal commission rate and fixed fee
    /// Returns (percentage_rate, absolute_premium_in_native)
    fn calculate_fees(&self, ctx: &PricingContext) -> (f64, f64);
    
    /// Estimate slippage percentage for a trade
    fn estimate_slippage(&self, amount_usd: f64, provider_spread: f64) -> f64;
}

pub struct AdaptivePricingStrategy {
    pub gas_safety_buffer: f64, // e.g. 1.5 (50% extra to cover gas)
    pub volatility_threshold: f64, // e.g. 0.02 (2%)
    pub volatility_premium: f64, // e.g. 0.005 (0.5%)
}

impl Default for AdaptivePricingStrategy {
    fn default() -> Self {
        Self {
            gas_safety_buffer: 1.5,
            volatility_threshold: 0.02,
            volatility_premium: 0.005,
        }
    }
}

impl AdaptivePricingStrategy {
    fn get_tier_rate(&self, amount_usd: f64) -> f64 {
        if amount_usd < 200.0 {
            0.012 // 1.2% for small trades
        } else if amount_usd < 2000.0 {
            0.007 // 0.7% for medium trades
        } else {
            0.004 // 0.4% for whales
        }
    }
}

#[async_trait]
impl PricingStrategy for AdaptivePricingStrategy {
    fn calculate_fees(&self, ctx: &PricingContext) -> (f64, f64) {
        // 1. Determine base rate from volume tiers
        let mut rate = self.get_tier_rate(ctx.amount_usd);

        // 2. Add Volatility Premium if providers are erratic
        if ctx.provider_spread_percentage > self.volatility_threshold {
            rate += self.volatility_premium;
        }

        // 3. Ensure Gas Floor protection
        // We calculate the required "Minimum Premium" to cover gas with a safety buffer
        let gas_floor_native = ctx.network_gas_cost_native * self.gas_safety_buffer;

        (rate, gas_floor_native)
    }
    
    fn estimate_slippage(&self, amount_usd: f64, provider_spread: f64) -> f64 {
        // Base slippage: 0.1% minimum
        let base = 0.001;
        
        // Volume impact (tiered based on trade size)
        let volume = if amount_usd < 100.0 {
            0.0005  // 0.05%
        } else if amount_usd < 1000.0 {
            0.001   // 0.1%
        } else if amount_usd < 10000.0 {
            0.002   // 0.2%
        } else {
            0.005   // 0.5%
        };
        
        // Volatility impact (use provider spread as proxy)
        // High spread = high volatility = more slippage
        let volatility = provider_spread * 0.5;
        
        base + volume + volatility
    }
}
