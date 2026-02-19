use alloy::primitives::U256;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::services::token::{Token, TokenType};

pub struct TokenGasEstimator;

impl TokenGasEstimator {
    /// Estimate gas limit for token transfer
    pub fn estimate_transfer_gas(token: &Token) -> u64 {
        match token.token_type {
            TokenType::Native => 21_000,
            TokenType::Erc20 | TokenType::Bep20 => {
                let base_gas = 65_000u64;
                let multiplier = token.gas_multiplier.to_f64().unwrap_or(3.0);
                let safety_margin = 1.2;
                (base_gas as f64 * multiplier * safety_margin) as u64
            }
            TokenType::Trc20 => 100_000,  // Tron uses different gas model
            TokenType::Spl => 5_000,      // Solana uses compute units
        }
    }
    
    /// Estimate gas limit for approval
    pub fn estimate_approval_gas(token: &Token) -> u64 {
        match token.token_type {
            TokenType::Native => 0,  // No approval needed
            TokenType::Erc20 | TokenType::Bep20 => {
                let base_gas = 50_000u64;
                let safety_margin = 1.2;
                (base_gas as f64 * safety_margin) as u64
            }
            TokenType::Trc20 => 80_000,
            TokenType::Spl => 0,  // SPL uses different approval model
        }
    }
    
    /// Estimate total gas cost in native currency (wei for EVM)
    pub fn estimate_gas_cost(
        token: &Token,
        gas_price: U256,
        include_approval: bool,
    ) -> U256 {
        let transfer_gas = Self::estimate_transfer_gas(token);
        let approval_gas = if include_approval {
            Self::estimate_approval_gas(token)
        } else {
            0
        };
        
        let total_gas = U256::from(transfer_gas + approval_gas);
        total_gas * gas_price
    }
    
    /// Calculate gas multiplier based on token complexity
    pub fn calculate_gas_multiplier(token: &Token) -> Decimal {
        // Base multiplier
        let mut multiplier = Decimal::from(3);
        
        // Adjust for token characteristics
        if !token.is_verified {
            multiplier += Decimal::from_str_exact("0.5").unwrap();  // Unverified tokens may be complex
        }
        
        // Cap at reasonable maximum
        multiplier.min(Decimal::from(5))
    }
    
    /// Estimate gas cost in USD
    pub fn estimate_gas_cost_usd(
        token: &Token,
        gas_price_gwei: Decimal,
        eth_price_usd: Decimal,
        include_approval: bool,
    ) -> Decimal {
        let transfer_gas = Decimal::from(Self::estimate_transfer_gas(token));
        let approval_gas = if include_approval {
            Decimal::from(Self::estimate_approval_gas(token))
        } else {
            Decimal::ZERO
        };
        
        let total_gas = transfer_gas + approval_gas;
        
        // Convert gwei to ETH: gas * gwei * 10^-9
        let gas_cost_eth = total_gas * gas_price_gwei / Decimal::from(1_000_000_000u64);
        
        // Convert to USD
        gas_cost_eth * eth_price_usd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_token(token_type: TokenType, gas_multiplier: f64, is_verified: bool) -> Token {
        Token {
            id: 1,
            symbol: "TEST".to_string(),
            name: "Test Token".to_string(),
            network: "ethereum".to_string(),
            contract_address: Some("0x0000000000000000000000000000000000000000".to_string()),
            decimals: 18,
            token_type,
            logo_url: None,
            coingecko_id: None,
            coinmarketcap_id: None,
            is_active: true,
            is_verified,
            min_swap_amount: None,
            max_swap_amount: None,
            gas_multiplier: Decimal::from_f64(gas_multiplier).unwrap(),
        }
    }
    
    #[test]
    fn test_native_gas_estimation() {
        let token = create_test_token(TokenType::Native, 1.0, true);
        
        assert_eq!(TokenGasEstimator::estimate_transfer_gas(&token), 21_000);
        assert_eq!(TokenGasEstimator::estimate_approval_gas(&token), 0);
    }
    
    #[test]
    fn test_erc20_gas_estimation() {
        let token = create_test_token(TokenType::Erc20, 3.0, true);
        
        let transfer_gas = TokenGasEstimator::estimate_transfer_gas(&token);
        assert!(transfer_gas > 65_000 && transfer_gas < 300_000);
        
        let approval_gas = TokenGasEstimator::estimate_approval_gas(&token);
        assert!(approval_gas > 50_000 && approval_gas < 100_000);
    }
    
    #[test]
    fn test_gas_cost_calculation() {
        let token = create_test_token(TokenType::Erc20, 3.0, true);
        let gas_price = U256::from(50_000_000_000u64);  // 50 gwei
        
        let cost_without_approval = TokenGasEstimator::estimate_gas_cost(&token, gas_price, false);
        let cost_with_approval = TokenGasEstimator::estimate_gas_cost(&token, gas_price, true);
        
        assert!(cost_with_approval > cost_without_approval);
    }
    
    #[test]
    fn test_gas_multiplier_calculation() {
        let verified_token = create_test_token(TokenType::Erc20, 3.0, true);
        let unverified_token = create_test_token(TokenType::Erc20, 3.0, false);
        
        let verified_multiplier = TokenGasEstimator::calculate_gas_multiplier(&verified_token);
        let unverified_multiplier = TokenGasEstimator::calculate_gas_multiplier(&unverified_token);
        
        assert_eq!(verified_multiplier, Decimal::from(3));
        assert_eq!(unverified_multiplier, Decimal::from_str_exact("3.5").unwrap());
    }
    
    #[test]
    fn test_gas_cost_usd() {
        let token = create_test_token(TokenType::Erc20, 3.0, true);
        let gas_price_gwei = Decimal::from(50);  // 50 gwei
        let eth_price_usd = Decimal::from(3000);  // $3000 per ETH
        
        let cost_usd = TokenGasEstimator::estimate_gas_cost_usd(
            &token,
            gas_price_gwei,
            eth_price_usd,
            false
        );
        
        // Should be reasonable (a few dollars)
        assert!(cost_usd > Decimal::ZERO);
        assert!(cost_usd < Decimal::from(100));  // Less than $100
    }
}
