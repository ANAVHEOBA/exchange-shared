use sqlx::MySqlPool;
use alloy::primitives::{Address, U256};

use rust_decimal::Decimal;

use crate::services::token::TokenError;

pub struct ApprovalManager {
    pool: MySqlPool,
}

impl ApprovalManager {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
    
    /// Check if approval is needed
    pub async fn needs_approval(
        &self,
        user_address: Address,
        token_address: Address,
        spender_address: Address,
        required_amount: U256,
        network: &str,
    ) -> Result<bool, TokenError> {
        let user_addr_str = format!("{:?}", user_address);
        let token_addr_str = format!("{:?}", token_address);
        let spender_addr_str = format!("{:?}", spender_address);
        
        // Check database cache first
        let cached_approval = sqlx::query!(
            r#"
            SELECT CAST(remaining_amount AS CHAR) as remaining_amount
            FROM token_approvals
            WHERE LOWER(user_address) = LOWER(?)
              AND LOWER(token_address) = LOWER(?)
              AND LOWER(spender_address) = LOWER(?)
              AND network = ?
              AND is_active = TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
            "#,
            user_addr_str,
            token_addr_str,
            spender_addr_str,
            network
        )
        .fetch_optional(&self.pool)
        .await?;
        
        if let Some(approval) = cached_approval {
            // Parse remaining amount from database (it's a Decimal stored as string)
            let remaining_str = approval.remaining_amount.unwrap_or_else(|| "0".to_string());
            let remaining = U256::from_str_radix(&remaining_str, 10)
                .map_err(|e| TokenError::ContractCallFailed(format!("Failed to parse remaining amount: {}", e)))?;
            
            if remaining >= required_amount {
                return Ok(false);  // Sufficient approval exists
            }
        }
        
        Ok(true)  // Need new approval
    }
    
    /// Calculate optimal approval amount (2x required for future swaps)
    pub fn calculate_approval_amount(&self, required_amount: U256) -> U256 {
        // Approve 2x the required amount for future swaps
        // Check for overflow
        if let Some(doubled) = required_amount.checked_mul(U256::from(2)) {
            doubled
        } else {
            // If overflow, just approve the required amount
            required_amount
        }
    }
    
    /// Record approval in database
    pub async fn record_approval(
        &self,
        user_address: Address,
        token_address: Address,
        spender_address: Address,
        approved_amount: U256,
        network: &str,
        tx_hash: &str,
        block_number: u64,
    ) -> Result<(), TokenError> {
        let user_addr_str = format!("{:?}", user_address);
        let token_addr_str = format!("{:?}", token_address);
        let spender_addr_str = format!("{:?}", spender_address);
        let approved_str = approved_amount.to_string();
        
        sqlx::query!(
            r#"
            INSERT INTO token_approvals 
            (user_address, token_address, spender_address, network, approved_amount, remaining_amount, tx_hash, block_number, approved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON DUPLICATE KEY UPDATE
                approved_amount = VALUES(approved_amount),
                remaining_amount = VALUES(remaining_amount),
                tx_hash = VALUES(tx_hash),
                block_number = VALUES(block_number),
                approved_at = NOW(),
                is_active = TRUE
            "#,
            user_addr_str,
            token_addr_str,
            spender_addr_str,
            network,
            approved_str,
            approved_str,
            tx_hash,
            block_number as i64
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    /// Update remaining allowance after transfer
    pub async fn update_allowance(
        &self,
        user_address: Address,
        token_address: Address,
        spender_address: Address,
        used_amount: U256,
        network: &str,
    ) -> Result<(), TokenError> {
        let user_addr_str = format!("{:?}", user_address);
        let token_addr_str = format!("{:?}", token_address);
        let spender_addr_str = format!("{:?}", spender_address);
        let used_str = used_amount.to_string();
        
        sqlx::query!(
            r#"
            UPDATE token_approvals
            SET remaining_amount = GREATEST(0, CAST(remaining_amount AS SIGNED) - CAST(? AS SIGNED)),
                last_used_at = NOW()
            WHERE LOWER(user_address) = LOWER(?)
              AND LOWER(token_address) = LOWER(?)
              AND LOWER(spender_address) = LOWER(?)
              AND network = ?
              AND is_active = TRUE
            "#,
            used_str,
            user_addr_str,
            token_addr_str,
            spender_addr_str,
            network
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    /// Revoke approval (set remaining to 0)
    pub async fn revoke_approval(
        &self,
        user_address: Address,
        token_address: Address,
        spender_address: Address,
        network: &str,
    ) -> Result<(), TokenError> {
        let user_addr_str = format!("{:?}", user_address);
        let token_addr_str = format!("{:?}", token_address);
        let spender_addr_str = format!("{:?}", spender_address);
        
        sqlx::query!(
            r#"
            UPDATE token_approvals
            SET is_active = FALSE,
                remaining_amount = 0
            WHERE LOWER(user_address) = LOWER(?)
              AND LOWER(token_address) = LOWER(?)
              AND LOWER(spender_address) = LOWER(?)
              AND network = ?
            "#,
            user_addr_str,
            token_addr_str,
            spender_addr_str,
            network
        )
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }
    
    /// Get approval statistics for monitoring
    pub async fn get_approval_stats(&self, network: &str) -> Result<ApprovalStats, TokenError> {
        let stats = sqlx::query!(
            r#"
            SELECT 
                COUNT(*) as total_approvals,
                COUNT(CASE WHEN last_used_at IS NOT NULL THEN 1 END) as reused_approvals,
                CAST(AVG(CAST(remaining_amount AS SIGNED)) AS CHAR) as avg_remaining
            FROM token_approvals
            WHERE network = ? AND is_active = TRUE
            "#,
            network
        )
        .fetch_one(&self.pool)
        .await?;
        
        let reuse_rate = if stats.total_approvals > 0 {
            stats.reused_approvals as f64 / stats.total_approvals as f64
        } else {
            0.0
        };
        
        let avg_remaining = stats.avg_remaining
            .and_then(|s| Decimal::from_str_exact(&s).ok())
            .unwrap_or(Decimal::ZERO);
        
        Ok(ApprovalStats {
            total_approvals: stats.total_approvals as u64,
            reused_approvals: stats.reused_approvals as u64,
            reuse_rate,
            avg_remaining,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApprovalStats {
    pub total_approvals: u64,
    pub reused_approvals: u64,
    pub reuse_rate: f64,
    pub avg_remaining: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_approval_amount() {
        let manager = ApprovalManager::new(MySqlPool::connect("mysql://localhost").await.unwrap());
        
        let required = U256::from(1000u64);
        let approved = manager.calculate_approval_amount(required);
        assert_eq!(approved, U256::from(2000u64));
    }
    
    #[test]
    fn test_calculate_approval_amount_overflow() {
        let manager = ApprovalManager::new(MySqlPool::connect("mysql://localhost").await.unwrap());
        
        // Test with max value
        let required = U256::MAX;
        let approved = manager.calculate_approval_amount(required);
        assert_eq!(approved, required);  // Should not overflow, returns original
    }
}
