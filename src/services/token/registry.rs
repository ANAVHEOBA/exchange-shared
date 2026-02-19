use sqlx::MySqlPool;
use alloy::primitives::Address;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal::Decimal;

use crate::services::token::{Token, TokenType, TokenError};

pub struct TokenRegistry {
    pool: MySqlPool,
    cache: Arc<RwLock<HashMap<String, Token>>>,  // key: "network:contract_address" or "network:symbol"
}

impl TokenRegistry {
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get token by contract address and network
    pub async fn get_token(&self, contract_address: &str, network: &str) -> Result<Token, TokenError> {
        let cache_key = format!("{}:{}", network, contract_address.to_lowercase());
        
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(token) = cache.get(&cache_key) {
                return Ok(token.clone());
            }
        }
        
        // Query database
        let token = sqlx::query!(
            r#"
            SELECT 
                id,
                symbol,
                name,
                network,
                contract_address,
                decimals,
                token_type as "token_type: TokenType",
                logo_url,
                coingecko_id,
                coinmarketcap_id,
                is_active,
                is_verified,
                CAST(min_swap_amount AS CHAR) as min_swap_amount,
                CAST(max_swap_amount AS CHAR) as max_swap_amount,
                CAST(gas_multiplier AS CHAR) as gas_multiplier
            FROM tokens
            WHERE LOWER(contract_address) = LOWER(?) AND network = ? AND is_active = TRUE
            "#,
            contract_address,
            network
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| TokenError::TokenNotFound(format!("{} on {}", contract_address, network)))?;
        
        let token_obj = Token {
            id: token.id,
            symbol: token.symbol,
            name: token.name,
            network: token.network,
            contract_address: token.contract_address,
            decimals: token.decimals as u8,
            token_type: token.token_type,
            logo_url: token.logo_url,
            coingecko_id: token.coingecko_id,
            coinmarketcap_id: token.coinmarketcap_id,
            is_active: token.is_active != 0,
            is_verified: token.is_verified != 0,
            min_swap_amount: token.min_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            max_swap_amount: token.max_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            gas_multiplier: Decimal::from_str_exact(&token.gas_multiplier.unwrap_or("3.0".to_string())).unwrap_or(Decimal::from(3)),
        };
        
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, token_obj.clone());
        }
        
        Ok(token_obj)
    }
    
    /// Get token by symbol and network
    pub async fn get_token_by_symbol(&self, symbol: &str, network: &str) -> Result<Token, TokenError> {
        let cache_key = format!("{}:symbol:{}", network, symbol.to_uppercase());
        
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(token) = cache.get(&cache_key) {
                return Ok(token.clone());
            }
        }
        
        let token = sqlx::query!(
            r#"
            SELECT 
                id,
                symbol,
                name,
                network,
                contract_address,
                decimals,
                token_type as "token_type: TokenType",
                logo_url,
                coingecko_id,
                coinmarketcap_id,
                is_active,
                is_verified,
                CAST(min_swap_amount AS CHAR) as min_swap_amount,
                CAST(max_swap_amount AS CHAR) as max_swap_amount,
                CAST(gas_multiplier AS CHAR) as gas_multiplier
            FROM tokens
            WHERE symbol = ? AND network = ? AND is_active = TRUE
            ORDER BY is_verified DESC
            LIMIT 1
            "#,
            symbol.to_uppercase(),
            network
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| TokenError::TokenNotFound(format!("{} on {}", symbol, network)))?;
        
        let token_obj = Token {
            id: token.id,
            symbol: token.symbol,
            name: token.name,
            network: token.network,
            contract_address: token.contract_address,
            decimals: token.decimals as u8,
            token_type: token.token_type,
            logo_url: token.logo_url,
            coingecko_id: token.coingecko_id,
            coinmarketcap_id: token.coinmarketcap_id,
            is_active: token.is_active != 0,
            is_verified: token.is_verified != 0,
            min_swap_amount: token.min_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            max_swap_amount: token.max_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            gas_multiplier: Decimal::from_str_exact(&token.gas_multiplier.unwrap_or("3.0".to_string())).unwrap_or(Decimal::from(3)),
        };
        
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, token_obj.clone());
        }
        
        Ok(token_obj)
    }
    
    /// Register a new token
    pub async fn register_token(
        &self,
        symbol: &str,
        name: &str,
        network: &str,
        contract_address: Option<Address>,
        decimals: u8,
        token_type: TokenType,
    ) -> Result<i64, TokenError> {
        let contract_addr_str = contract_address.map(|addr| format!("{:?}", addr));
        
        let result = sqlx::query!(
            r#"
            INSERT INTO tokens (symbol, name, network, contract_address, decimals, token_type, is_verified)
            VALUES (?, ?, ?, ?, ?, ?, FALSE)
            "#,
            symbol.to_uppercase(),
            name,
            network,
            contract_addr_str,
            decimals,
            token_type
        )
        .execute(&self.pool)
        .await?;
        
        // Clear cache for this network
        self.clear_cache_for_network(network).await;
        
        Ok(result.last_insert_id() as i64)
    }
    
    /// List all active tokens for a network
    pub async fn list_tokens(&self, network: &str) -> Result<Vec<Token>, TokenError> {
        let tokens = sqlx::query!(
            r#"
            SELECT 
                id,
                symbol,
                name,
                network,
                contract_address,
                decimals,
                token_type as "token_type: TokenType",
                logo_url,
                coingecko_id,
                coinmarketcap_id,
                is_active,
                is_verified,
                CAST(min_swap_amount AS CHAR) as min_swap_amount,
                CAST(max_swap_amount AS CHAR) as max_swap_amount,
                CAST(gas_multiplier AS CHAR) as gas_multiplier
            FROM tokens
            WHERE network = ? AND is_active = TRUE
            ORDER BY is_verified DESC, symbol ASC
            "#,
            network
        )
        .fetch_all(&self.pool)
        .await?;
        
        Ok(tokens.into_iter().map(|t| Token {
            id: t.id,
            symbol: t.symbol,
            name: t.name,
            network: t.network,
            contract_address: t.contract_address,
            decimals: t.decimals as u8,
            token_type: t.token_type,
            logo_url: t.logo_url,
            coingecko_id: t.coingecko_id,
            coinmarketcap_id: t.coinmarketcap_id,
            is_active: t.is_active != 0,
            is_verified: t.is_verified != 0,
            min_swap_amount: t.min_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            max_swap_amount: t.max_swap_amount.and_then(|s| Decimal::from_str_exact(&s).ok()),
            gas_multiplier: Decimal::from_str_exact(&t.gas_multiplier.unwrap_or("3.0".to_string())).unwrap_or(Decimal::from(3)),
        }).collect())
    }
    
    /// Update token metadata
    pub async fn update_token(
        &self,
        token_id: i64,
        logo_url: Option<String>,
        coingecko_id: Option<String>,
        is_verified: bool,
    ) -> Result<(), TokenError> {
        sqlx::query!(
            r#"
            UPDATE tokens
            SET logo_url = ?,
                coingecko_id = ?,
                is_verified = ?
            WHERE id = ?
            "#,
            logo_url,
            coingecko_id,
            is_verified,
            token_id
        )
        .execute(&self.pool)
        .await?;
        
        // Clear entire cache since we don't know which keys to invalidate
        self.clear_cache().await;
        
        Ok(())
    }
    
    /// Clear cache for a specific network
    async fn clear_cache_for_network(&self, network: &str) {
        let mut cache = self.cache.write().await;
        cache.retain(|key, _| !key.starts_with(&format!("{}:", network)));
    }
    
    /// Clear entire cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Integration tests would require database connection
    // These are unit tests for the cache key logic
    
    #[test]
    fn test_cache_key_format() {
        let network = "ethereum";
        let contract = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
        let key = format!("{}:{}", network, contract.to_lowercase());
        assert_eq!(key, "ethereum:0xdac17f958d2ee523a2206206994597c13d831ec7");
    }
    
    #[test]
    fn test_symbol_cache_key() {
        let network = "ethereum";
        let symbol = "usdt";
        let key = format!("{}:symbol:{}", network, symbol.to_uppercase());
        assert_eq!(key, "ethereum:symbol:USDT");
    }
}
