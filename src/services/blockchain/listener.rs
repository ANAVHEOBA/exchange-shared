use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use sqlx::{MySql, Pool};
use crate::services::wallet::rpc::{BlockchainProvider, HttpRpcClient};

/// Blockchain event listener that monitors addresses for incoming funds
/// This is the optimal approach - detects funds immediately without polling Trocador
pub struct BlockchainListener {
    db: Pool<MySql>,
    providers: HashMap<String, Arc<dyn BlockchainProvider>>,
    check_interval: Duration,
}

impl BlockchainListener {
    /// Create a new blockchain listener with RPC providers for each chain
    pub fn new(db: Pool<MySql>) -> Self {
        let mut providers: HashMap<String, Arc<dyn BlockchainProvider>> = HashMap::new();
        
        // Initialize RPC clients for each supported EVM chain
        // Ethereum
        if let Ok(rpc) = std::env::var("ETH_RPC_URL") {
            providers.insert("ethereum".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Polygon
        if let Ok(rpc) = std::env::var("POLYGON_RPC_URL") {
            providers.insert("polygon".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Binance Smart Chain
        if let Ok(rpc) = std::env::var("BSC_RPC_URL") {
            providers.insert("bsc".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Arbitrum
        if let Ok(rpc) = std::env::var("ARBITRUM_RPC_URL") {
            providers.insert("arbitrum".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Optimism
        if let Ok(rpc) = std::env::var("OPTIMISM_RPC_URL") {
            providers.insert("optimism".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Avalanche
        if let Ok(rpc) = std::env::var("AVALANCHE_RPC_URL") {
            providers.insert("avalanche".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Base
        if let Ok(rpc) = std::env::var("BASE_RPC_URL") {
            providers.insert("base".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Fantom
        if let Ok(rpc) = std::env::var("FANTOM_RPC_URL") {
            providers.insert("fantom".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Gnosis
        if let Ok(rpc) = std::env::var("GNOSIS_RPC_URL") {
            providers.insert("gnosis".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Cronos
        if let Ok(rpc) = std::env::var("CRONOS_RPC_URL") {
            providers.insert("cronos".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Moonbeam
        if let Ok(rpc) = std::env::var("MOONBEAM_RPC_URL") {
            providers.insert("moonbeam".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Moonriver
        if let Ok(rpc) = std::env::var("MOONRIVER_RPC_URL") {
            providers.insert("moonriver".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Celo
        if let Ok(rpc) = std::env::var("CELO_RPC_URL") {
            providers.insert("celo".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Aurora
        if let Ok(rpc) = std::env::var("AURORA_RPC_URL") {
            providers.insert("aurora".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Harmony
        if let Ok(rpc) = std::env::var("HARMONY_RPC_URL") {
            providers.insert("harmony".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Metis
        if let Ok(rpc) = std::env::var("METIS_RPC_URL") {
            providers.insert("metis".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // zkSync Era
        if let Ok(rpc) = std::env::var("ZKSYNC_RPC_URL") {
            providers.insert("zksync".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Linea
        if let Ok(rpc) = std::env::var("LINEA_RPC_URL") {
            providers.insert("linea".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Scroll
        if let Ok(rpc) = std::env::var("SCROLL_RPC_URL") {
            providers.insert("scroll".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Mantle
        if let Ok(rpc) = std::env::var("MANTLE_RPC_URL") {
            providers.insert("mantle".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Blast
        if let Ok(rpc) = std::env::var("BLAST_RPC_URL") {
            providers.insert("blast".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Mode
        if let Ok(rpc) = std::env::var("MODE_RPC_URL") {
            providers.insert("mode".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        // Manta Pacific
        if let Ok(rpc) = std::env::var("MANTA_RPC_URL") {
            providers.insert("manta".to_string(), Arc::new(HttpRpcClient::new(rpc)));
        }
        
        if providers.is_empty() {
            tracing::warn!("⚠️  No RPC providers configured! Blockchain listener will not work.");
            tracing::warn!("    Add RPC URLs to .env file (e.g., ETH_RPC_URL, POLYGON_RPC_URL)");
        } else {
            tracing::info!("🚀 Blockchain listener initialized with {} chains: {:?}", 
                providers.len(), 
                providers.keys().collect::<Vec<_>>()
            );
        }
        
        Self {
            db,
            providers,
            check_interval: Duration::from_secs(30), // Check every 30 seconds
        }
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
    async fn check_pending_swaps(&self) -> Result<(), String> {
        // Get swaps that are in progress and waiting for funds
        let pending: Vec<(String, String, String, f64, f64)> = sqlx::query_as(
            r#"
            SELECT 
                s.id,
                sa.our_address,
                s.to_network,
                s.estimated_receive,
                s.platform_fee
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
        let normalized = network.to_lowercase();
        
        // Try exact match first
        if let Some(provider) = self.providers.get(&normalized) {
            return Some(provider.clone());
        }
        
        // Try common aliases and variations
        let provider_key = match normalized.as_str() {
            // Ethereum aliases
            "erc20" | "eth" | "mainnet" => "ethereum",
            
            // Polygon aliases
            "matic" | "pos" => "polygon",
            
            // BSC aliases
            "bnb" | "bep20" | "binance" | "smartchain" => "bsc",
            
            // Arbitrum aliases
            "arb" | "arbitrum one" | "arbitrumone" => "arbitrum",
            
            // Optimism aliases
            "op" | "optimistic" => "optimism",
            
            // Avalanche aliases
            "avax" | "avalanche c-chain" | "cchain" => "avalanche",
            
            // Base aliases
            "base mainnet" | "coinbase" => "base",
            
            // Fantom aliases
            "ftm" | "opera" => "fantom",
            
            // Gnosis aliases
            "xdai" | "gno" => "gnosis",
            
            // Cronos aliases
            "cro" => "cronos",
            
            // Moonbeam aliases
            "glmr" => "moonbeam",
            
            // Moonriver aliases
            "movr" => "moonriver",
            
            // Celo aliases
            "celo mainnet" => "celo",
            
            // Aurora aliases
            "aurora mainnet" | "near" => "aurora",
            
            // Harmony aliases
            "one" | "harmony one" => "harmony",
            
            // Metis aliases
            "metis andromeda" => "metis",
            
            // zkSync aliases
            "zksync era" | "zks" => "zksync",
            
            // Linea aliases
            "linea mainnet" => "linea",
            
            // Scroll aliases
            "scroll mainnet" => "scroll",
            
            // Mantle aliases
            "mnt" => "mantle",
            
            // Blast aliases
            "blast mainnet" => "blast",
            
            // Mode aliases
            "mode mainnet" => "mode",
            
            // Manta aliases
            "manta pacific" | "manta mainnet" => "manta",
            
            _ => {
                tracing::debug!("No RPC provider found for network: {}", network);
                return None;
            }
        };
        
        self.providers.get(provider_key).cloned()
    }
    
    /// Trigger payout by updating swap status
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
        
        // Update swap_address_info with actual received amount
        sqlx::query(
            r#"
            UPDATE swap_address_info 
            SET actual_received = ?, last_balance_check = NOW()
            WHERE swap_id = ?
            "#
        )
        .bind(actual_balance)
        .bind(swap_id)
        .execute(&self.db)
        .await
        .map_err(|e| format!("Failed to update address info: {}", e))?;
        
        tracing::info!(
            "🎯 Payout triggered for swap {}: {} received on blockchain",
            swap_id, actual_balance
        );
        
        Ok(())
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
            active_chains: self.providers.len(),
        })
    }
}

#[derive(Debug)]
pub struct ListenerStats {
    pub total_pending: u64,
    pub oldest_pending: Option<chrono::DateTime<chrono::Utc>>,
    pub active_chains: usize,
}
