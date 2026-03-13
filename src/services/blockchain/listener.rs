use std::sync::Arc;
use tokio::time::{interval, Duration};
use sqlx::{MySql, Pool};
use crate::services::wallet::rpc::BlockchainProvider;
use crate::services::wallet::manager::WalletManager;
use crate::modules::wallet::crud::WalletCrud;
use crate::modules::wallet::schema::PayoutRequest;
use crate::config::RpcProviderConfig;

/// Blockchain event listener that monitors addresses for incoming funds
/// This is the optimal approach - detects funds immediately without polling Trocador
pub struct BlockchainListener {
    db: Pool<MySql>,
    rpc_config: RpcProviderConfig,
    check_interval: Duration,
    wallet_mnemonic: Option<String>,
}

impl BlockchainListener {
    /// Create a new blockchain listener with RPC providers from config
    pub fn new(db: Pool<MySql>) -> Self {
        // Load RPC providers from centralized config
        let rpc_config = RpcProviderConfig::from_env();
        
        Self {
            db,
            rpc_config,
            check_interval: Duration::from_secs(30), // Check every 30 seconds
            wallet_mnemonic: None,
        }
    }
    
    /// Set wallet mnemonic for payout processing
    pub fn with_wallet_mnemonic(mut self, mnemonic: String) -> Self {
        self.wallet_mnemonic = Some(mnemonic);
        self
    }
    
    /// Main monitoring loop - runs continuously in background
    pub async fn run(&self) {
        tracing::info!("🚀 Blockchain listener started");
        let mut tick = interval(self.check_interval);
        
        loop {
            tick.tick().await;
            
            if let Err(e) = self.check_pending_swaps().await {
                tracing::error!("Blockchain listener error: {}", e);
            }
        }
    }
    
    /// Check all pending swaps for incoming funds on blockchain
    pub async fn check_pending_swaps(&self) -> Result<(), String> {
        // Get swaps that are in progress and waiting for funds
        let pending: Vec<(String, String, String, f64, f64)> = sqlx::query_as(
            r#"
            SELECT 
                s.id,
                sa.our_address,
                s.to_network,
                CAST(s.estimated_receive AS DOUBLE) as estimated_receive,
                CAST(s.platform_fee AS DOUBLE) as platform_fee
            FROM swaps s
            JOIN swap_address_info sa ON s.id = sa.swap_id
            WHERE s.status IN ('sending', 'exchanging', 'confirming')
            AND sa.status = 'pending'
            AND s.created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY s.created_at DESC
            LIMIT 100
            "#
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| format!("Database error: {}", e))?;
        
        if !pending.is_empty() {
            tracing::debug!("Checking {} pending swaps for blockchain funds", pending.len());
        }
        
        for (swap_id, our_address, network, estimated_receive, platform_fee) in pending {
            // Expected amount is what user gets + our commission
            let expected_amount = estimated_receive + platform_fee;
            
            // Get the appropriate RPC provider for this network
            let provider = match self.get_provider_for_network(&network) {
                Some(p) => p,
                None => {
                    tracing::warn!("No RPC provider configured for network: {}", network);
                    continue;
                }
            };
            
            // Check blockchain balance
            match provider.get_balance(&our_address).await {
                Ok(balance) if balance >= expected_amount * 0.95 => {
                    // Funds detected! (95% threshold to account for small discrepancies)
                    tracing::info!(
                        "✅ Blockchain funds detected for swap {}: {} {} (expected {})",
                        swap_id, balance, network, expected_amount
                    );
                    
                    // Trigger payout
                    if let Err(e) = self.trigger_payout(&swap_id, balance).await {
                        tracing::error!("Failed to trigger payout for {}: {}", swap_id, e);
                    }
                }
                Ok(balance) if balance > 0.0001 => {
                    // Some funds detected but not enough yet
                    tracing::debug!(
                        "⏳ Partial funds for swap {}: {} / {} {}",
                        swap_id, balance, expected_amount, network
                    );
                    
                    // Update last balance check timestamp
                    self.update_balance_check(&swap_id).await.ok();
                }
                Ok(_) => {
                    // No funds yet, keep waiting
                    tracing::trace!("Waiting for funds: swap {} on {}", swap_id, network);
                }
                Err(e) => {
                    tracing::error!(
                        "RPC error checking balance for swap {} on {}: {}",
                        swap_id, network, e
                    );
                }
            }
        }
        
        Ok(())
    }
    
    /// Get RPC provider for a specific network
    fn get_provider_for_network(&self, network: &str) -> Option<Arc<dyn BlockchainProvider>> {
        self.rpc_config.get_provider(network)
    }
    
    /// Trigger payout by updating swap status and executing the payout
    async fn trigger_payout(&self, swap_id: &str, actual_balance: f64) -> Result<(), String> {
        // Update swap status to 'funds_received'
        sqlx::query(
            r#"
            UPDATE swaps 
            SET status = 'funds_received', updated_at = NOW() 
            WHERE id = ? AND status IN ('sending', 'exchanging', 'confirming')
            "#
        )
        .bind(swap_id)
        .execute(&self.db)
        .await
        .map_err(|e| format!("Failed to update swap status: {}", e))?;
        
        tracing::info!(
            "🎯 Funds detected for swap {}: {} received on blockchain",
            swap_id, actual_balance
        );
        
        // CRITICAL: Execute the actual payout to user
        if let Some(ref mnemonic) = self.wallet_mnemonic {
            tracing::info!("💸 Initiating payout for swap {}", swap_id);
            
            // Get the provider for this swap's network
            let provider = self.get_provider_for_swap(swap_id).await?;
            
            // Create WalletManager
            let crud = WalletCrud::new(self.db.clone());
            let wallet_manager = WalletManager::new(
                crud,
                mnemonic.clone(),
                provider,
            );
            
            // Execute payout with retry logic (3 attempts with exponential backoff)
            let payout_request = PayoutRequest {
                swap_id: swap_id.to_string(),
            };
            
            match wallet_manager.process_payout_with_retry(payout_request, 3).await {
                Ok(response) => {
                    tracing::info!(
                        "✅ Payout successful for swap {}: {} (tx: {})",
                        swap_id, response.amount, response.tx_hash
                    );
                    
                    // Update swap status to completed
                    sqlx::query(
                        r#"
                        UPDATE swaps 
                        SET status = 'completed', updated_at = NOW() 
                        WHERE id = ?
                        "#
                    )
                    .bind(swap_id)
                    .execute(&self.db)
                    .await
                    .map_err(|e| format!("Failed to update swap to completed: {}", e))?;
                    
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("❌ Payout failed after retries for swap {}: {}", swap_id, e);
                    
                    // Update swap status to failed
                    sqlx::query(
                        r#"
                        UPDATE swaps 
                        SET status = 'failed', updated_at = NOW() 
                        WHERE id = ?
                        "#
                    )
                    .bind(swap_id)
                    .execute(&self.db)
                    .await
                    .ok();
                    
                    Err(format!("Payout execution failed after retries: {}", e))
                }
            }
        } else {
            tracing::warn!(
                "⚠️  Wallet mnemonic not configured - cannot execute payout for swap {}",
                swap_id
            );
            tracing::warn!("   Status updated to 'funds_received' but payout not executed");
            Ok(())
        }
    }
    
    /// Get the appropriate blockchain provider for a swap
    async fn get_provider_for_swap(&self, swap_id: &str) -> Result<Arc<dyn BlockchainProvider>, String> {
        // Get swap network from database
        let (network,): (String,) = sqlx::query_as(
            "SELECT to_network FROM swaps WHERE id = ?"
        )
        .bind(swap_id)
        .fetch_one(&self.db)
        .await
        .map_err(|e| format!("Failed to get swap network: {}", e))?;
        
        self.get_provider_for_network(&network)
            .ok_or_else(|| format!("No RPC provider configured for network: {}", network))
    }
    
    /// Update last balance check timestamp
    async fn update_balance_check(&self, swap_id: &str) -> Result<(), String> {
        sqlx::query(
            "UPDATE swap_address_info SET last_balance_check = NOW() WHERE swap_id = ?"
        )
        .bind(swap_id)
        .execute(&self.db)
        .await
        .map_err(|e| format!("Failed to update balance check: {}", e))?;
        
        Ok(())
    }
    
    /// Get statistics about pending swaps
    pub async fn get_stats(&self) -> Result<ListenerStats, String> {
        let (total_pending, oldest_pending): (i64, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*) as total,
                MIN(s.created_at) as oldest
            FROM swaps s
            JOIN swap_address_info sa ON s.id = sa.swap_id
            WHERE s.status IN ('sending', 'exchanging', 'confirming')
            AND sa.status = 'pending'
            "#
        )
        .fetch_one(&self.db)
        .await
        .map_err(|e| format!("Failed to get stats: {}", e))?;
        
        Ok(ListenerStats {
            total_pending: total_pending as u64,
            oldest_pending,
            active_chains: self.rpc_config.provider_count(),
        })
    }
}

#[derive(Debug)]
pub struct ListenerStats {
    pub total_pending: u64,
    pub oldest_pending: Option<chrono::DateTime<chrono::Utc>>,
    pub active_chains: usize,
}
