use crate::modules::monitor::crud::MonitorCrud;
use crate::modules::monitor::model::PollingState;
use crate::modules::swap::status::SwapStatus;
use crate::services::redis_cache::RedisService;
use crate::services::rpc::{build_provider_for_network, RpcManager};
use crate::services::settlement::{SettlementOutcome, SettlementService};
use crate::services::trocador::TrocadorGateway;
use crate::services::wallet::rpc::BlockchainProvider;
use sqlx::{MySql, Pool};
use std::sync::Arc;
use std::time::Duration;

use crate::services::monitor::strategy::PollingStrategy;

pub struct MonitorEngine {
    db: Pool<MySql>,
    redis: RedisService,
    master_seed: String,
    strategy: PollingStrategy,
    rpc_manager: Arc<RpcManager>,
}

impl MonitorEngine {
    pub fn new(
        db: Pool<MySql>,
        redis: RedisService,
        master_seed: String,
        rpc_manager: Arc<RpcManager>,
    ) -> Self {
        // Initialize strategy with default costs:
        // Cp = 1.0 (one poll)
        // Cd = 0.05 (20 seconds of delay equals cost of one poll)
        let strategy = PollingStrategy::new(1.0, 0.05);
        Self {
            db,
            redis,
            master_seed,
            strategy,
            rpc_manager,
        }
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
            "SELECT provider_swap_id, status, created_at, to_network FROM swaps WHERE id = ?",
            state.swap_id
        )
        .fetch_optional(&self.db)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Swap not found".to_string())?;

        // 3. Check if blockchain listener already detected funds
        if swap.status == SwapStatus::FundsReceived.as_str() {
            tracing::info!(
                "Swap {} already has funds detected by blockchain listener, executing payout",
                state.swap_id
            );

            match self
                .settlement_service()
                .settle_swap(
                    &state.swap_id,
                    self.provider_for_network(&swap.to_network).await?,
                    None,
                )
                .await
            {
                Ok(SettlementOutcome::Completed(payout)) => {
                    tracing::info!(
                        "✅ Payout successful for swap {}: tx_hash={}, amount={}",
                        state.swap_id,
                        payout.tx_hash,
                        payout.amount
                    );
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud
                        .update_poll_result(&state.swap_id, SwapStatus::Completed.as_str(), 86400)
                        .await;

                    return Ok(());
                }
                Ok(SettlementOutcome::AlreadyCompleted) => {
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud
                        .update_poll_result(&state.swap_id, SwapStatus::Completed.as_str(), 86400)
                        .await;
                    return Ok(());
                }
                Ok(SettlementOutcome::AwaitingPayout) => {
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud
                        .update_poll_result(&state.swap_id, SwapStatus::FundsReceived.as_str(), 300)
                        .await;
                    return Ok(());
                }
                Ok(SettlementOutcome::PayoutInProgress) => {
                    let monitor_crud = MonitorCrud::new(self.db.clone());
                    let _ = monitor_crud
                        .update_poll_result(&state.swap_id, SwapStatus::FundsReceived.as_str(), 60)
                        .await;
                    return Ok(());
                }
                Ok(SettlementOutcome::PendingRetry { reason }) => {
                    tracing::error!("❌ Payout failed for swap {}: {}", state.swap_id, reason);
                    self.update_poll_result(&state.swap_id, "payout_failed", 300)
                        .await;

                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        "Settlement error for swap {}. Keeping it retryable: {}",
                        state.swap_id,
                        e
                    );
                    self.update_poll_result(
                        &state.swap_id,
                        SwapStatus::FundsReceived.as_str(),
                        300,
                    )
                    .await;
                    return Ok(());
                }
            }
        }

        let provider_swap_id = swap
            .provider_swap_id
            .ok_or_else(|| "No provider trade ID".to_string())?;

        // 4. Check Trocador Status (fallback if blockchain listener hasn't detected yet)
        let trocador_gateway = match TrocadorGateway::from_env() {
            Ok(gateway) => gateway,
            Err(_) => {
                tracing::warn!(
                    "TROCADOR_API_KEY not set while polling swap {}. Retrying later.",
                    state.swap_id
                );
                self.update_poll_result(&state.swap_id, &swap.status, 300)
                    .await;
                return Ok(());
            }
        };

        let trocador_trade = match trocador_gateway.fetch_trade_status(&provider_swap_id).await {
            Ok(trade) => trade,
            Err(e) => {
                tracing::warn!(
                    "Failed to fetch provider status for swap {}. Retrying later: {}",
                    state.swap_id,
                    e
                );
                self.update_poll_result(&state.swap_id, &swap.status, 300)
                    .await;
                return Ok(());
            }
        };

        // 5. THE BRIDGE: Check blockchain and trigger payout if funds confirmed
        let final_status: String;
        let next_poll_secs: u64;

        if trocador_trade.status == "finished" {
            tracing::info!(
                "Swap {} finished on Trocador. Verifying blockchain balance (fallback check).",
                state.swap_id
            );

            // Get our address info for this swap
            let wallet_crud = crate::modules::wallet::crud::WalletCrud::new(self.db.clone());
            let address_info = match wallet_crud.get_address_info(&state.swap_id).await {
                Ok(Some(info)) => info,
                Ok(None) => {
                    tracing::error!("No address info found for swap {}", state.swap_id);
                    final_status = "error".to_string();
                    next_poll_secs = 300;
                    self.update_poll_result(&state.swap_id, &final_status, next_poll_secs)
                        .await;
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("Failed to get address info: {}", e);
                    final_status = "error".to_string();
                    next_poll_secs = 300;
                    self.update_poll_result(&state.swap_id, &final_status, next_poll_secs)
                        .await;
                    return Ok(());
                }
            };

            // Check blockchain balance (fallback verification)
            let provider = self.provider_for_network(&swap.to_network).await?;

            match provider.get_balance(&address_info.our_address).await {
                Ok(balance) if balance >= 0.0001 => {
                    // Funds confirmed on blockchain!
                    tracing::info!(
                        "✅ Blockchain balance confirmed for swap {} (monitor fallback): {} at address {}",
                        state.swap_id, balance, address_info.our_address
                    );

                    match self
                        .settlement_service()
                        .settle_swap(&state.swap_id, provider, Some(balance))
                        .await
                    {
                        Ok(SettlementOutcome::Completed(payout)) => {
                            tracing::info!(
                                "✅ Payout successful for swap {}: tx_hash={}, amount={}",
                                state.swap_id,
                                payout.tx_hash,
                                payout.amount
                            );
                            final_status = SwapStatus::Completed.as_str().to_string();
                            next_poll_secs = 3600 * 24;
                        }
                        Ok(SettlementOutcome::AlreadyCompleted) => {
                            final_status = SwapStatus::Completed.as_str().to_string();
                            next_poll_secs = 3600 * 24;
                        }
                        Ok(SettlementOutcome::AwaitingPayout) => {
                            final_status = SwapStatus::FundsReceived.as_str().to_string();
                            next_poll_secs = 300;
                        }
                        Ok(SettlementOutcome::PayoutInProgress) => {
                            final_status = SwapStatus::FundsReceived.as_str().to_string();
                            next_poll_secs = 60;
                        }
                        Ok(SettlementOutcome::PendingRetry { reason }) => {
                            tracing::error!(
                                "❌ Payout failed for swap {}: {}",
                                state.swap_id,
                                reason
                            );
                            final_status = "payout_failed".to_string();
                            next_poll_secs = 300;
                        }
                        Err(e) => {
                            tracing::error!("Settlement failed for swap {}: {}", state.swap_id, e);
                            final_status = "error".to_string();
                            next_poll_secs = 300;
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
                    tracing::error!(
                        "Failed to check blockchain balance for swap {}: {}",
                        state.swap_id,
                        e
                    );
                    final_status = "awaiting_funds".to_string();
                    next_poll_secs = 120; // Retry in 2 minutes
                }
            }
        } else {
            let mapped_status = SwapStatus::from_trocador_status(&trocador_trade.status);
            final_status = mapped_status.as_str().to_string();
            // Update internal swap status if changed (e.g. 'confirming' -> 'sending')
            if mapped_status.as_str() != swap.status {
                sqlx::query!(
                    "UPDATE swaps SET status = ?, updated_at = NOW() WHERE id = ?",
                    mapped_status.as_str(),
                    state.swap_id
                )
                .execute(&self.db)
                .await
                .ok();
            }

            // 6. OPTIMAL POLLING LOGIC
            let elapsed = chrono::Utc::now() - swap.created_at;
            let elapsed_secs = elapsed.num_seconds().max(0) as u64;
            next_poll_secs = self
                .strategy
                .calculate_next_interval(elapsed_secs)
                .as_secs();
        }

        // 7. Update Monitoring State
        self.update_poll_result(&state.swap_id, &final_status, next_poll_secs)
            .await;

        Ok(())
    }

    async fn provider_for_network(
        &self,
        network: &str,
    ) -> Result<Arc<dyn BlockchainProvider>, String> {
        build_provider_for_network(self.rpc_manager.clone(), network).await
    }

    fn settlement_service(&self) -> SettlementService {
        SettlementService::new(self.db.clone(), Some(self.master_seed.clone()))
    }

    async fn update_poll_result(&self, swap_id: &str, status: &str, next_poll_secs: u64) {
        let monitor_crud = MonitorCrud::new(self.db.clone());
        if let Err(e) = monitor_crud
            .update_poll_result(swap_id, status, next_poll_secs)
            .await
        {
            tracing::warn!(
                "Failed to update polling state for swap {} to {}: {}",
                swap_id,
                status,
                e
            );
        }
    }
}
