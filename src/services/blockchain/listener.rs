use crate::modules::swap::status::SwapStatus;
use crate::services::rpc::{build_provider_for_network, RpcManager};
use crate::services::settlement::{SettlementOutcome, SettlementService};
use crate::services::wallet::rpc::BlockchainProvider;
use sqlx::{MySql, Pool};
use std::sync::Arc;
use tokio::time::{interval, Duration};

/// Blockchain event listener that monitors addresses for incoming funds
/// This is the optimal approach - detects funds immediately without polling Trocador
pub struct BlockchainListener {
    db: Pool<MySql>,
    rpc_manager: Arc<RpcManager>,
    check_interval: Duration,
    wallet_mnemonic: Option<String>,
}

impl BlockchainListener {
    /// Create a new blockchain listener with production RPC manager
    pub fn new(db: Pool<MySql>, rpc_manager: Arc<RpcManager>) -> Self {
        Self {
            db,
            rpc_manager,
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
        let pending: Vec<(String, String, String, f64, f64, f64)> = sqlx::query_as(
            r#"
            SELECT 
                s.id,
                sa.our_address,
                s.to_network,
                CAST(s.estimated_receive AS DOUBLE) as estimated_receive,
                CAST(s.network_fee AS DOUBLE) as network_fee,
                CAST(s.platform_fee AS DOUBLE) as platform_fee
            FROM swaps s
            JOIN swap_address_info sa ON s.id = sa.swap_id
            WHERE s.status IN ('sending', 'exchanging', 'confirming')
            AND sa.status = 'pending'
            AND s.created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY s.created_at DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

        if !pending.is_empty() {
            tracing::debug!(
                "Checking {} pending swaps for blockchain funds",
                pending.len()
            );
        }

        for (swap_id, our_address, network, estimated_receive, network_fee, platform_fee) in pending
        {
            // Expected amount is user payout + platform fee + payout-side network fee.
            let expected_amount = estimated_receive + platform_fee + network_fee;

            // Get the appropriate RPC provider for this network
            let provider = match self.get_provider_for_network(&network).await {
                Ok(provider) => provider,
                Err(e) => {
                    tracing::warn!("{}", e);
                    continue;
                }
            };

            // Check blockchain balance
            match provider.get_balance(&our_address).await {
                Ok(balance) if balance >= expected_amount * 0.95 => {
                    // Funds detected! (95% threshold to account for small discrepancies)
                    tracing::info!(
                        "✅ Blockchain funds detected for swap {}: {} {} (expected {})",
                        swap_id,
                        balance,
                        network,
                        expected_amount
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
                        swap_id,
                        balance,
                        expected_amount,
                        network
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
                        swap_id,
                        network,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Get RPC provider for a specific network
    async fn get_provider_for_network(
        &self,
        network: &str,
    ) -> Result<Arc<dyn BlockchainProvider>, String> {
        build_provider_for_network(self.rpc_manager.clone(), network).await
    }

    /// Trigger payout by updating swap status and executing the payout
    async fn trigger_payout(&self, swap_id: &str, actual_balance: f64) -> Result<(), String> {
        tracing::info!(
            "🎯 Funds detected for swap {}: {} received on blockchain",
            swap_id,
            actual_balance
        );

        let provider = self.get_provider_for_swap(swap_id).await?;
        let settlement_service =
            SettlementService::new(self.db.clone(), self.wallet_mnemonic.clone());

        match settlement_service
            .settle_swap(swap_id, provider, Some(actual_balance))
            .await?
        {
            SettlementOutcome::Completed(response) => {
                tracing::info!(
                    "✅ Payout successful for swap {}: {} (tx: {})",
                    swap_id,
                    response.amount,
                    response.tx_hash
                );
                Ok(())
            }
            SettlementOutcome::AlreadyCompleted => {
                tracing::info!("Swap {} was already completed before settlement", swap_id);
                Ok(())
            }
            SettlementOutcome::AwaitingPayout => {
                tracing::warn!(
                    "Wallet mnemonic not configured - swap {} remains in {}",
                    swap_id,
                    SwapStatus::FundsReceived
                );
                Ok(())
            }
            SettlementOutcome::PayoutInProgress => {
                tracing::info!(
                    "Payout already in progress for swap {}. Skipping duplicate trigger.",
                    swap_id
                );
                Ok(())
            }
            SettlementOutcome::PendingRetry { reason } => {
                tracing::error!("❌ Payout failed for swap {}: {}", swap_id, reason);
                Err(reason)
            }
        }
    }

    /// Get the appropriate blockchain provider for a swap
    async fn get_provider_for_swap(
        &self,
        swap_id: &str,
    ) -> Result<Arc<dyn BlockchainProvider>, String> {
        // Get swap network from database
        let (network,): (String,) = sqlx::query_as("SELECT to_network FROM swaps WHERE id = ?")
            .bind(swap_id)
            .fetch_one(&self.db)
            .await
            .map_err(|e| format!("Failed to get swap network: {}", e))?;

        self.get_provider_for_network(&network).await
    }

    /// Update last balance check timestamp
    async fn update_balance_check(&self, swap_id: &str) -> Result<(), String> {
        sqlx::query("UPDATE swap_address_info SET last_balance_check = NOW() WHERE swap_id = ?")
            .bind(swap_id)
            .execute(&self.db)
            .await
            .map_err(|e| format!("Failed to update balance check: {}", e))?;

        Ok(())
    }

    /// Get statistics about pending swaps
    pub async fn get_stats(&self) -> Result<ListenerStats, String> {
        let (total_pending, oldest_pending): (i64, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                r#"
            SELECT 
                COUNT(*) as total,
                MIN(s.created_at) as oldest
            FROM swaps s
            JOIN swap_address_info sa ON s.id = sa.swap_id
            WHERE s.status IN ('sending', 'exchanging', 'confirming')
            AND sa.status = 'pending'
            "#,
            )
            .fetch_one(&self.db)
            .await
            .map_err(|e| format!("Failed to get stats: {}", e))?;

        // Count active chains from RpcManager
        let active_chains = 0; // TODO: Add method to RpcManager to count active chains

        Ok(ListenerStats {
            total_pending: total_pending as u64,
            oldest_pending,
            active_chains,
        })
    }
}

#[derive(Debug)]
pub struct ListenerStats {
    pub total_pending: u64,
    pub oldest_pending: Option<chrono::DateTime<chrono::Utc>>,
    pub active_chains: usize,
}
