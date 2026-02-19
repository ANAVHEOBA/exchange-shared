use serde::{Deserialize, Serialize};
use alloy::primitives::{Address, U256};
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

/// Token type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "VARCHAR", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TokenType {
    Native,    // ETH, BTC, SOL
    Erc20,     // Ethereum ERC-20
    Bep20,     // BSC BEP-20
    Trc20,     // Tron TRC-20
    Spl,       // Solana SPL
}

impl TokenType {
    pub fn is_evm_token(&self) -> bool {
        matches!(self, Self::Erc20 | Self::Bep20)
    }
}

/// Token metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub id: i64,
    pub symbol: String,
    pub name: String,
    pub network: String,
    pub contract_address: Option<String>,
    pub decimals: u8,
    pub token_type: TokenType,
    
    // Metadata
    pub logo_url: Option<String>,
    pub coingecko_id: Option<String>,
    pub coinmarketcap_id: Option<String>,
    
    // Status
    pub is_active: bool,
    pub is_verified: bool,
    
    // Limits
    pub min_swap_amount: Option<Decimal>,
    pub max_swap_amount: Option<Decimal>,
    
    // Gas configuration
    pub gas_multiplier: Decimal,
}

impl Token {
    pub fn is_native(&self) -> bool {
        self.token_type == TokenType::Native
    }
    
    pub fn is_erc20(&self) -> bool {
        self.token_type.is_evm_token()
    }
    
    pub fn requires_approval(&self) -> bool {
        self.is_erc20()
    }
    
    pub fn contract_address_parsed(&self) -> Result<Address, TokenError> {
        self.contract_address
            .as_ref()
            .ok_or_else(|| TokenError::InvalidContractAddress("No contract address".to_string()))?
            .parse()
            .map_err(|e| TokenError::InvalidContractAddress(format!("Invalid address: {}", e)))
    }
}

/// Token balance information
#[derive(Debug, Clone)]
pub struct TokenBalance {
    pub token_address: Address,
    pub owner_address: Address,
    pub balance: U256,
    pub decimals: u8,
    pub balance_decimal: Decimal,
}

/// Token approval information
#[derive(Debug, Clone)]
pub struct TokenApproval {
    pub token_address: Address,
    pub owner_address: Address,
    pub spender_address: Address,
    pub allowance: U256,
    pub decimals: u8,
    pub allowance_decimal: Decimal,
}

/// Token transfer request
#[derive(Debug, Clone)]
pub struct TokenTransferRequest {
    pub token_address: Address,
    pub from_address: Address,
    pub to_address: Address,
    pub amount: U256,
    pub decimals: u8,
    pub network: String,
    pub gas_price: Option<U256>,
    pub gas_limit: Option<u64>,
}

/// Token transfer status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "VARCHAR", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TokenTransferStatus {
    Pending,
    Confirmed,
    Failed,
}

/// Token transfer record
#[derive(Debug, Clone)]
pub struct TokenTransfer {
    pub id: i64,
    pub swap_id: Option<String>,
    pub token_address: String,
    pub network: String,
    pub from_address: String,
    pub to_address: String,
    pub amount: Decimal,
    pub decimals: u8,
    pub tx_hash: String,
    pub block_number: Option<i64>,
    pub gas_used: Option<i64>,
    pub gas_price: Option<Decimal>,
    pub status: TokenTransferStatus,
    pub confirmations: i32,
    pub error_message: Option<String>,
    pub submitted_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

/// Token errors
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("Token not found: {0}")]
    TokenNotFound(String),
    
    #[error("Invalid contract address: {0}")]
    InvalidContractAddress(String),
    
    #[error("Insufficient token balance: required {required}, available {available}")]
    InsufficientBalance { required: String, available: String },
    
    #[error("Insufficient allowance: required {required}, approved {approved}")]
    InsufficientAllowance { required: String, approved: String },
    
    #[error("Token not active: {0}")]
    TokenNotActive(String),
    
    #[error("Invalid decimals: {0}")]
    InvalidDecimals(u8),
    
    #[error("Contract call failed: {0}")]
    ContractCallFailed(String),
    
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    
    #[error("RPC error: {0}")]
    Rpc(String),
    
    #[error("Conversion error: {0}")]
    Conversion(String),
}

/// Helper functions for decimal conversion
pub fn to_base_units(amount: Decimal, decimals: u8) -> Result<U256, TokenError> {
    let multiplier = Decimal::from(10u64.pow(decimals as u32));
    let base_amount = amount * multiplier;
    
    // Convert to string and parse as U256
    let amount_str = base_amount.to_string();
    // Remove decimal point if present
    let amount_str = amount_str.split('.').next().unwrap_or(&amount_str);
    
    U256::from_str_radix(amount_str, 10)
        .map_err(|e| TokenError::Conversion(format!("Failed to convert to base units: {}", e)))
}

pub fn from_base_units(amount: U256, decimals: u8) -> Result<Decimal, TokenError> {
    let amount_str = amount.to_string();
    let amount_dec = Decimal::from_str_exact(&amount_str)
        .map_err(|e| TokenError::Conversion(format!("Failed to parse amount: {}", e)))?;
    let divisor = Decimal::from(10u64.pow(decimals as u32));
    Ok(amount_dec / divisor)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_to_base_units() {
        // 1.5 USDT (6 decimals) = 1,500,000
        let amount = Decimal::from_str_exact("1.5").unwrap();
        let base = to_base_units(amount, 6).unwrap();
        assert_eq!(base, U256::from(1_500_000u64));
        
        // 0.5 DAI (18 decimals)
        let amount = Decimal::from_str_exact("0.5").unwrap();
        let base = to_base_units(amount, 18).unwrap();
        assert_eq!(base, U256::from_str_radix("500000000000000000", 10).unwrap());
    }
    
    #[test]
    fn test_from_base_units() {
        // 1,500,000 base units with 6 decimals = 1.5
        let base = U256::from(1_500_000u64);
        let amount = from_base_units(base, 6).unwrap();
        assert_eq!(amount, Decimal::from_str_exact("1.5").unwrap());
        
        // 18 decimals
        let base = U256::from_str_radix("500000000000000000", 10).unwrap();
        let amount = from_base_units(base, 18).unwrap();
        assert_eq!(amount, Decimal::from_str_exact("0.5").unwrap());
    }
    
    #[test]
    fn test_token_type_is_evm() {
        assert!(TokenType::Erc20.is_evm_token());
        assert!(TokenType::Bep20.is_evm_token());
        assert!(!TokenType::Native.is_evm_token());
        assert!(!TokenType::Trc20.is_evm_token());
        assert!(!TokenType::Spl.is_evm_token());
    }
}
