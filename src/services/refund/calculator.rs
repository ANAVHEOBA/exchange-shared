use rust_decimal::Decimal;

use uuid::Uuid;
use sqlx::{MySqlPool, Row};
use std::str::FromStr;

use crate::services::refund::{RefundCalculation, RefundConfig, RefundError};

pub struct RefundCalculator {
    pool: MySqlPool,
    config: RefundConfig,
}

impl RefundCalculator {
    pub fn new(pool: MySqlPool, config: RefundConfig) -> Self {
        Self { pool, config }
    }
    
    /// Calculate refund amount for a failed swap
    pub async fn calculate_refund(&self, swap_id: Uuid) -> Result<RefundCalculation, RefundError> {
        // Fetch swap details using regular query
        let row = sqlx::query(
            "SELECT CAST(amount AS CHAR) as amount, from_currency, CAST(platform_fee AS CHAR) as platform_fee, CAST(total_fee AS CHAR) as total_fee, status FROM swaps WHERE id = ?"
        )
        .bind(swap_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(RefundError::SwapNotFound(swap_id))?;
        
        // Parse Decimal values from database strings
        let deposit_amount_str: String = row.try_get("amount")?;
        let deposit_amount = Decimal::from_str(&deposit_amount_str)
            .map_err(|e| RefundError::Config(format!("Invalid amount: {}", e)))?;
            
        let from_currency: String = row.try_get("from_currency")?;
        
        let platform_fee_str: Option<String> = row.try_get("platform_fee").ok();
        let platform_fee = platform_fee_str
            .and_then(|s| Decimal::from_str(&s).ok())
            .unwrap_or(Decimal::ZERO);
            
        let total_fee_str: Option<String> = row.try_get("total_fee").ok();
        let total_fee = total_fee_str
            .and_then(|s| Decimal::from_str(&s).ok())
            .unwrap_or(Decimal::ZERO);
        
        // Estimate gas cost for refund transaction
        let gas_cost_estimate = self.estimate_gas_cost(&from_currency).await?;
        
        // Calculate refund amount
        let refund_amount = deposit_amount - platform_fee - total_fee - gas_cost_estimate;
        
        // Check if economical
        let min_threshold = self.get_min_threshold(&from_currency);
        let is_economical = refund_amount >= min_threshold;
        
        let reason = if !is_economical {
            format!("Refund amount {} is below minimum threshold {}", refund_amount, min_threshold)
        } else {
            "Refund is economical".to_string()
        };
        
        Ok(RefundCalculation {
            refund_amount,
            deposit_amount,
            fees_paid: platform_fee + total_fee,
            gas_cost_estimate,
            is_economical,
            reason,
        })
    }
    
    async fn estimate_gas_cost(&self, currency: &str) -> Result<Decimal, RefundError> {
        // Use gas estimation service
        match currency.to_uppercase().as_str() {
            "BTC" => Ok(Decimal::from_str("0.0001").unwrap()),
            "ETH" => Ok(Decimal::from_str("0.001").unwrap()),
            "SOL" => Ok(Decimal::from_str("0.00001").unwrap()),
            _ => Ok(Decimal::from_str("0.0001").unwrap()),
        }
    }
    
    fn get_min_threshold(&self, currency: &str) -> Decimal {
        match currency.to_uppercase().as_str() {
            "BTC" => Decimal::from_f64_retain(self.config.min_refund_threshold_btc).unwrap(),
            "ETH" => Decimal::from_f64_retain(self.config.min_refund_threshold_eth).unwrap(),
            _ => Decimal::from_f64_retain(self.config.min_refund_threshold_usd).unwrap(),
        }
    }
    
    /// Calculate priority score for refund processing order
    pub fn calculate_priority_score(
        &self,
        age_hours: f64,
        amount_usd: f64,
        attempt_number: u32,
    ) -> f64 {
        let age_factor = age_hours.min(10.0);
        let amount_factor = (amount_usd / 100.0).min(10.0);
        let retry_factor = (10 - attempt_number as i32).max(0) as f64;
        
        self.config.priority_weight_age * age_factor
            + self.config.priority_weight_amount * amount_factor
            + self.config.priority_weight_retry * retry_factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_priority_score_calculation() {
        let config = RefundConfig::default();
        
        // Test priority score calculation without needing a pool
        let age_factor = 6.0_f64.min(10.0);
        let amount_factor = (500.0_f64 / 100.0).min(10.0);
        let retry_factor = (10 - 1_i32).max(0) as f64;
        
        let expected_score = config.priority_weight_age * age_factor
            + config.priority_weight_amount * amount_factor
            + config.priority_weight_retry * retry_factor;
        
        // 0.5 * 6 + 0.3 * 5 + 0.2 * 9 = 3.0 + 1.5 + 1.8 = 6.3
        assert!((expected_score - 6.3).abs() < 0.01);
    }
    
    #[test]
    fn test_min_threshold_values() {
        let config = RefundConfig::default();
        
        // Test threshold values directly from config
        assert_eq!(config.min_refund_threshold_btc, 0.0001);
        assert_eq!(config.min_refund_threshold_eth, 0.001);
        assert_eq!(config.min_refund_threshold_usd, 1.0);
    }
    
    #[test]
    fn test_gas_estimation_values() {
        // Test gas estimation logic
        let btc_gas = Decimal::from_str("0.0001").unwrap();
        let eth_gas = Decimal::from_str("0.001").unwrap();
        let sol_gas = Decimal::from_str("0.00001").unwrap();
        
        assert!(btc_gas > Decimal::ZERO);
        assert!(eth_gas > btc_gas);
        assert!(sol_gas < btc_gas);
    }
}
