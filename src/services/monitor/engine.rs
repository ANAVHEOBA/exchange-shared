use std::time::Duration;
use sqlx::{MySql, Pool};
use crate::modules::monitor::crud::MonitorCrud;
use crate::services::trocador::TrocadorClient;
use crate::services::wallet::manager::WalletManager;
use crate::services::wallet::rpc::HttpRpcClient;
use crate::services::redis_cache::RedisService;
use crate::modules::monitor::model::PollingState;

use crate::services::monitor::strategy::PollingStrategy;

pub struct MonitorEngine {
    db: Pool<MySql>,
    redis: RedisService,
    master_seed: String,
    strategy: PollingStrategy,
}

impl MonitorEngine {
    pub fn new(db: Pool<MySql>, redis: RedisService, master_seed: String) -> Self {
        // Initialize strategy with default costs:
        // Cp = 1.0 (one poll)
        // Cd = 0.05 (20 seconds of delay equals cost of one poll)
        let strategy = PollingStrategy::new(1.0, 0.05);
        Self { db, redis, master_seed, strategy }
    }

    /// Start the background polling loop
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let monitor_crud = MonitorCrud::new(self.db.clone());

        loop {
            interval.tick().await;
            
            if let Ok(polls) = monitor_crud.get_due_polls().await {
                for poll in polls {
                    let _ = self.process_poll(poll).await;
                }
            }
        }
    }

    /// Process a single swap poll
    pub async fn process_poll(&self, state: PollingState) -> Result<(), String> {
        // 1. Distributed Lock to prevent concurrency
        let lock_key = format!("lock:monitor:{}", state.swap_id);
        if !self.redis.try_lock(&lock_key, 30).await.unwrap_or(false) {
            return Ok(());
        }

        // 2. Fetch Swap Details
        let swap = sqlx::query!(
            "SELECT provider_swap_id, status, created_at FROM swaps WHERE id = ?",
            state.swap_id
        )
        .fetch_optional(&self.db)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Swap not found".to_string())?;

        // 3. Check if blockchain listener already detected funds
        if swap.status == "funds_received" {
            tracing::info!("Swap {} already has funds detected by blockchain listener, executing payout", state.swap_id);
            
            // Blockchain listener detected funds, now execute payout
            let wallet_crud = crate::modules::wallet::crud::WalletCrud::new(self.db.clone());
            let rpc_url = std::env::var("ETH_RPC_URL").unwrap_or_else(|_| "http://localhost:8545".to_string());
            let provider: std::sync::Arc<dyn crate::services::wallet::rpc::BlockchainProvider> = 
                std::sync::Arc::new(HttpRpcClient::new(rpc_url));
            let wallet_manager = WalletManager::new(wallet_crud, self.master_seed.clone(), provider);
            
            match wallet_manager.process_payout(crate::modules::wallet::schema::PayoutRequest {
                swap_id: state.swap_id.clone(),
            }).await {
                Ok(payout) => {
                    tracing::info!(
                        "✅ Payout successful for swap {}: tx_hash={}, amount={}",
                        state.swap_id, payout.tx_hash, payout.amount
                    );
                    
                    sqlx::query!("UPDATE swaps SET status = 'completed', updated_at = NOW() WHERE id = ?", state.swap_id)
                        .execute(&self.db).await.ok();
                    
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud.update_poll_result(&state.swap_id, "completed", 86400).await;
                    
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("❌ Payout failed for swap {}: {}", state.swap_id, e);
                    
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud.update_poll_result(&state.swap_id, "payout_failed", 300).await;
                    
                    return Ok(());
                }
            }
        }

        let provider_swap_id = swap.provider_swap_id
            .ok_or_else(|| "No provider trade ID".to_string())?;

        // 4. Check Trocador Status (fallback if blockchain listener hasn't detected yet)
        let api_key = std::env::var("TROCADOR_API_KEY").unwrap_or_default();
        let client = TrocadorClient::new(api_key);
        
        let trocador_trade = client.get_trade_status(&provider_swap_id).await
            .map_err(|e| e.to_string())?;

        // 5. THE BRIDGE: Check blockchain and trigger payout if funds confirmed
        let final_status: String;
        let next_poll_secs: u64;

        if trocador_trade.status == "finished" {
            tracing::info!("Swap {} finished on Trocador. Verifying blockchain balance (fallback check).", state.swap_id);
            
            // Get our address info for this swap
            let wallet_crud = crate::modules::wallet::crud::WalletCrud::new(self.db.clone());
            let address_info = match wallet_crud.get_address_info(&state.swap_id).await {
                Ok(Some(info)) => info,
                Ok(None) => {
                    tracing::error!("No address info found for swap {}", state.swap_id);
                    final_status = "error".to_string();
                    next_poll_secs = 300;
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud.update_poll_result(&state.swap_id, &final_status, next_poll_secs).await;
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("Failed to get address info: {}", e);
                    final_status = "error".to_string();
                    next_poll_secs = 300;
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud.update_poll_result(&state.swap_id, &final_status, next_poll_secs).await;
                    return Ok(());
                }
            };
            
            // Check blockchain balance (fallback verification)
            let rpc_url = std::env::var("ETH_RPC_URL").unwrap_or_else(|_| "http://localhost:8545".to_string());
            let provider: std::sync::Arc<dyn crate::services::wallet::rpc::BlockchainProvider> = 
                std::sync::Arc::new(HttpRpcClient::new(rpc_url));
            
            match provider.get_balance(&address_info.our_address).await {
                Ok(balance) if balance >= 0.0001 => {
                    // Funds confirmed on blockchain!
                    tracing::info!(
                        "✅ Blockchain balance confirmed for swap {} (monitor fallback): {} at address {}",
                        state.swap_id, balance, address_info.our_address
                    );
                    
                    // Update status to funds_received (in case listener missed it)
                    sqlx::query!("UPDATE swaps SET status = 'funds_received', updated_at = NOW() WHERE id = ?", state.swap_id)
                        .execute(&self.db).await.ok();
                    
                    // Now safe to trigger payout
                    let wallet_manager = WalletManager::new(wallet_crud, self.master_seed.clone(), provider);
                    
                    match wallet_manager.process_payout(crate::modules::wallet::schema::PayoutRequest {
                        swap_id: state.swap_id.clone(),
                    }).await {
                        Ok(payout) => {
                            tracing::info!(
                                "✅ Payout successful for swap {}: tx_hash={}, amount={}",
                                state.swap_id, payout.tx_hash, payout.amount
                            );
                            final_status = "completed".to_string();
                            next_poll_secs = 3600 * 24; // Stop polling (once a day for cleanup)
                            
                            sqlx::query!("UPDATE swaps SET status = 'completed', updated_at = NOW() WHERE id = ?", state.swap_id)
                                .execute(&self.db).await.ok();
                        }
                        Err(e) => {
                            tracing::error!("❌ Payout failed for swap {}: {}", state.swap_id, e);
                            final_status = "payout_failed".to_string();
                            next_poll_secs = 300; // Retry in 5 minutes
                        }
                    }
                }
                Ok(balance) => {
                    // Trocador says finished but funds not on chain yet
                    tracing::warn!(
                        "⏳ Trocador finished but blockchain balance insufficient for swap {}: {} (waiting for confirmations)",
                        state.swap_id, balance
                    );
                    final_status = "awaiting_funds".to_string();
                    next_poll_secs = 60; // Check again in 1 minute
                }
                Err(e) => {
                    tracing::error!("Failed to check blockchain balance for swap {}: {}", state.swap_id, e);
                    final_status = "awaiting_funds".to_string();
                    next_poll_secs = 120; // Retry in 2 minutes
                }
            }
        } else {
            final_status = trocador_trade.status.clone();
            // Update internal swap status if changed (e.g. 'confirming' -> 'sending')
            if trocador_trade.status != swap.status {
                sqlx::query!("UPDATE swaps SET status = ?, updated_at = NOW() WHERE id = ?", trocador_trade.status, state.swap_id)
                    .execute(&self.db).await.ok();
            }
            
            // 6. OPTIMAL POLLING LOGIC
            let elapsed = chrono::Utc::now() - swap.created_at;
            let elapsed_secs = elapsed.num_seconds().max(0) as u64;
            next_poll_secs = self.strategy.calculate_next_interval(elapsed_secs).as_secs();
        }

        // 7. Update Monitoring State
        let monitor_crud = MonitorCrud::new(self.db.clone());
        let _ = monitor_crud.update_poll_result(&state.swap_id, &final_status, next_poll_secs).await;

        Ok(())
    }
}