use alloy::primitives::{Address, U256};
use alloy::sol;
use alloy::providers::{ProviderBuilder, RootProvider};
use alloy::transports::http::{Client, Http};
use std::sync::Arc;

use crate::services::token::{TokenError, TokenBalance, TokenApproval, from_base_units};

// Define ERC-20 interface using sol! macro with inline Solidity
sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    contract IERC20 {
        function name() external view returns (string memory);
        function symbol() external view returns (string memory);
        function decimals() external view returns (uint8);
        function totalSupply() external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
        function transfer(address recipient, uint256 amount) external returns (bool);
        function approve(address spender, uint256 amount) external returns (bool);
        function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
        
        event Transfer(address indexed from, address indexed to, uint256 value);
        event Approval(address indexed owner, address indexed spender, uint256 value);
    }
}

pub struct Erc20Client {
    provider: Arc<RootProvider<Http<Client>>>,
}

impl Erc20Client {
    pub fn new(provider: Arc<RootProvider<Http<Client>>>) -> Self {
        Self { provider }
    }
    
    /// Create a new client from RPC URL
    pub async fn from_rpc_url(rpc_url: &str) -> Result<Self, TokenError> {
        let provider = ProviderBuilder::new()
            .on_http(rpc_url.parse().map_err(|e| TokenError::Rpc(format!("Invalid RPC URL: {}", e)))?);
        
        Ok(Self {
            provider: Arc::new(provider),
        })
    }
    
    /// Get token metadata (name, symbol, decimals)
    pub async fn get_metadata(&self, token_address: Address) -> Result<(String, String, u8), TokenError> {
        let contract = IERC20::new(token_address, self.provider.clone());
        
        let name = contract.name().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("name(): {}", e)))?
            ._0;
        
        let symbol = contract.symbol().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("symbol(): {}", e)))?
            ._0;
        
        let decimals = contract.decimals().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("decimals(): {}", e)))?
            ._0;
        
        Ok((name, symbol, decimals))
    }
    
    /// Get token balance for an address
    pub async fn get_balance(&self, token_address: Address, owner: Address) -> Result<TokenBalance, TokenError> {
        let contract = IERC20::new(token_address, self.provider.clone());
        
        let balance = contract.balanceOf(owner).call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("balanceOf(): {}", e)))?
            ._0;
        
        let decimals = contract.decimals().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("decimals(): {}", e)))?
            ._0;
        
        let balance_decimal = from_base_units(balance, decimals)?;
        
        Ok(TokenBalance {
            token_address,
            owner_address: owner,
            balance,
            decimals,
            balance_decimal,
        })
    }
    
    /// Get token allowance
    pub async fn get_allowance(
        &self,
        token_address: Address,
        owner: Address,
        spender: Address,
    ) -> Result<TokenApproval, TokenError> {
        let contract = IERC20::new(token_address, self.provider.clone());
        
        let allowance = contract.allowance(owner, spender).call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("allowance(): {}", e)))?
            ._0;
        
        let decimals = contract.decimals().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("decimals(): {}", e)))?
            ._0;
        
        let allowance_decimal = from_base_units(allowance, decimals)?;
        
        Ok(TokenApproval {
            token_address,
            owner_address: owner,
            spender_address: spender,
            allowance,
            decimals,
            allowance_decimal,
        })
    }
    
    /// Get total supply
    pub async fn get_total_supply(&self, token_address: Address) -> Result<U256, TokenError> {
        let contract = IERC20::new(token_address, self.provider.clone());
        
        let total_supply = contract.totalSupply().call().await
            .map_err(|e| TokenError::ContractCallFailed(format!("totalSupply(): {}", e)))?
            ._0;
        
        Ok(total_supply)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: These tests require a running Ethereum node or testnet
    // They are marked as ignored by default
    
    #[tokio::test]
    #[ignore]
    async fn test_get_metadata() {
        // This would require a real provider connection
        // Example with USDT on Ethereum mainnet
        let client = Erc20Client::from_rpc_url("https://eth.llamarpc.com").await.unwrap();
        let usdt_address: Address = "0xdAC17F958D2ee523a2206206994597C13D831ec7".parse().unwrap();
        let (name, symbol, decimals) = client.get_metadata(usdt_address).await.unwrap();
        assert_eq!(symbol, "USDT");
        assert_eq!(decimals, 6);
    }
    
    #[tokio::test]
    #[ignore]
    async fn test_get_balance() {
        let client = Erc20Client::from_rpc_url("https://eth.llamarpc.com").await.unwrap();
        let usdt_address: Address = "0xdAC17F958D2ee523a2206206994597C13D831ec7".parse().unwrap();
        let vitalik: Address = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045".parse().unwrap();
        
        let balance = client.get_balance(usdt_address, vitalik).await.unwrap();
        assert!(balance.balance >= U256::ZERO);
    }
}
