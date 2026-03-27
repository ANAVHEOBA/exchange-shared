use crate::modules::swap::status::SwapStatus;
use crate::modules::wallet::crud::WalletCrud;
use crate::modules::wallet::schema::{PayoutRequest, PayoutResponse};
use crate::services::wallet::manager::WalletManager;
use crate::services::wallet::rpc::BlockchainProvider;
use sqlx::{MySql, Pool};
use std::sync::Arc;

const DEFAULT_PAYOUT_RETRIES: usize = 3;

pub struct SettlementService {
    db: Pool<MySql>,
    wallet_mnemonic: Option<String>,
    payout_retries: usize,
}

#[derive(Debug, Clone)]
pub enum SettlementOutcome {
    Completed(PayoutResponse),
    AlreadyCompleted,
    AwaitingPayout,
    PayoutInProgress,
    PendingRetry { reason: String },
}

impl SettlementService {
    pub fn new(db: Pool<MySql>, wallet_mnemonic: Option<String>) -> Self {
        Self {
            db,
            wallet_mnemonic,
            payout_retries: DEFAULT_PAYOUT_RETRIES,
        }
    }

    pub async fn settle_swap(
        &self,
        swap_id: &str,
        provider: Arc<dyn BlockchainProvider>,
        observed_balance: Option<f64>,
    ) -> Result<SettlementOutcome, String> {
        let current_status = self.fetch_swap_status(swap_id).await?;

        match current_status {
            SwapStatus::Completed => return Ok(SettlementOutcome::AlreadyCompleted),
            SwapStatus::Refunded | SwapStatus::Expired => {
                return Err(format!(
                    "Swap {} is in terminal settlement status {}",
                    swap_id, current_status
                ));
            }
            SwapStatus::FundsReceived => {}
            // `failed` may exist from pre-refactor payout attempts. Re-arm it so the
            // monitor and listener can converge on the same retryable settlement flow.
            SwapStatus::Failed
            | SwapStatus::Waiting
            | SwapStatus::Confirming
            | SwapStatus::Exchanging
            | SwapStatus::Sending => {
                self.mark_funds_received(swap_id).await?;
            }
        }

        if let Some(balance) = observed_balance {
            tracing::info!(
                "Settlement entrypoint reached for swap {} with observed on-chain balance {}",
                swap_id,
                balance
            );
        }

        let Some(wallet_mnemonic) = self.wallet_mnemonic.clone() else {
            tracing::warn!(
                "Wallet mnemonic not configured - swap {} will remain in {}",
                swap_id,
                SwapStatus::FundsReceived
            );
            return Ok(SettlementOutcome::AwaitingPayout);
        };

        let wallet_manager =
            WalletManager::new(WalletCrud::new(self.db.clone()), wallet_mnemonic, provider);

        match wallet_manager
            .process_payout_with_retry(
                PayoutRequest {
                    swap_id: swap_id.to_string(),
                },
                self.payout_retries,
            )
            .await
        {
            Ok(response) => {
                self.mark_completed(swap_id).await?;
                Ok(SettlementOutcome::Completed(response))
            }
            Err(reason) if is_payout_in_progress_error(&reason) => {
                Ok(SettlementOutcome::PayoutInProgress)
            }
            Err(reason) => {
                // Keep the swap retryable. The monitor can re-enter the same settlement
                // path later instead of recovering from a terminal failed status.
                self.mark_funds_received(swap_id).await?;
                Ok(SettlementOutcome::PendingRetry { reason })
            }
        }
    }

    async fn fetch_swap_status(&self, swap_id: &str) -> Result<SwapStatus, String> {
        let status = sqlx::query_scalar::<_, String>("SELECT status FROM swaps WHERE id = ?")
            .bind(swap_id)
            .fetch_optional(&self.db)
            .await
            .map_err(|e| format!("Failed to load swap status: {}", e))?
            .ok_or_else(|| format!("Swap not found: {}", swap_id))?;

        SwapStatus::from_persisted(&status)
            .ok_or_else(|| format!("Unknown persisted swap status '{}' for {}", status, swap_id))
    }

    async fn mark_funds_received(&self, swap_id: &str) -> Result<(), String> {
        sqlx::query(
            r#"
            UPDATE swaps
            SET status = ?, updated_at = NOW()
            WHERE id = ? AND status != ?
            "#,
        )
        .bind(SwapStatus::FundsReceived.as_str())
        .bind(swap_id)
        .bind(SwapStatus::Completed.as_str())
        .execute(&self.db)
        .await
        .map_err(|e| format!("Failed to mark swap as funds received: {}", e))?;

        Ok(())
    }

    async fn mark_completed(&self, swap_id: &str) -> Result<(), String> {
        sqlx::query(
            r#"
            UPDATE swaps
            SET status = ?, completed_at = NOW(), updated_at = NOW()
            WHERE id = ?
            "#,
        )
        .bind(SwapStatus::Completed.as_str())
        .bind(swap_id)
        .execute(&self.db)
        .await
        .map_err(|e| format!("Failed to mark swap as completed: {}", e))?;

        Ok(())
    }
}

fn is_payout_in_progress_error(reason: &str) -> bool {
    reason.contains("Payout already in progress")
}
