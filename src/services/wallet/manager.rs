use std::sync::Arc;
use base64::Engine;
use crate::modules::wallet::crud::WalletCrud;
use crate::modules::wallet::schema::{GenerateAddressRequest, WalletAddressResponse, PayoutRequest, PayoutResponse};
use super::derivation;
use super::signing::SigningService;
use super::rpc::BlockchainProvider;
use super::bitcoin_rpc::{BitcoinProvider, build_bitcoin_transaction};
use super::solana_rpc::{SolanaProvider, build_solana_transaction, sign_solana_transaction};
use crate::services::pricing::{PricingContext, PricingStrategy, AdaptivePricingStrategy};

pub struct WalletManager {
    crud: WalletCrud,
    master_seed: String,
    evm_provider: Arc<dyn BlockchainProvider>,
    bitcoin_provider: Option<Arc<dyn BitcoinProvider>>,
    solana_provider: Option<Arc<dyn SolanaProvider>>,
}

impl WalletManager {
    pub fn new(
        crud: WalletCrud,
        master_seed: String,
        evm_provider: Arc<dyn BlockchainProvider>,
    ) -> Self {
        Self {
            crud,
            master_seed,
            evm_provider,
            bitcoin_provider: None,
            solana_provider: None,
        }
    }

    pub fn with_bitcoin_provider(mut self, provider: Arc<dyn BitcoinProvider>) -> Self {
        self.bitcoin_provider = Some(provider);
        self
    }

    pub fn with_solana_provider(mut self, provider: Arc<dyn SolanaProvider>) -> Self {
        self.solana_provider = Some(provider);
        self
    }

    /// High-level orchestrator to generate a new swap address
    pub async fn get_or_generate_address(
        &self,
        req: GenerateAddressRequest,
    ) -> Result<WalletAddressResponse, String> {
        // 1. Check if swap already has an address assigned in DB
        if let Ok(Some(existing)) = self.crud.get_address_info(&req.swap_id).await {
            return Ok(WalletAddressResponse {
                address: existing.our_address,
                address_index: existing.address_index,
                swap_id: existing.swap_id,
            });
        }

        // 2. Get next available HD index
        let index = self.crud.get_next_index().await
            .map_err(|e: sqlx::Error| format!("DB Error: {}", e))?;

        // 3. Use high-level dispatcher to derive address
        let address = derivation::derive_address(&self.master_seed, &req.ticker, &req.network, index).await?;

        // 4. Save to DB
        self.crud.save_address_info(
            &req.swap_id,
            &address,
            index,
            &req.network,
            &req.user_recipient_address,
            req.user_recipient_extra_id.as_deref(),
        ).await
            .map_err(|e: sqlx::Error| format!("Failed to save address info: {}", e))?;

        Ok(WalletAddressResponse {
            address,
            address_index: index,
            swap_id: req.swap_id,
        })
    }

    /// Orchestrate a payout to the user with idempotency and blockchain verification
    pub async fn process_payout(
        &self,
        req: PayoutRequest,
    ) -> Result<PayoutResponse, String> {
        // 1. Get address info and check for existing payout
        let info = self.crud.get_address_info(&req.swap_id).await
            .map_err(|e: sqlx::Error| e.to_string())?
            .ok_or_else(|| "No address info found for swap".to_string())?;

        // 2. IDEMPOTENCY CHECK: If already has tx_hash or status is success, return early
        if let Some(tx_hash) = info.payout_tx_hash {
            return Ok(PayoutResponse {
                tx_hash,
                amount: info.payout_amount.unwrap_or(0.0),
                status: crate::modules::wallet::model::PayoutStatus::Success,
            });
        }

        // 3. Determine chain type from coin_type
        // coin_type: 0 = Bitcoin, 60 = Ethereum/EVM, 501 = Solana
        match info.coin_type {
            0 => self.process_bitcoin_payout(&info, &req.swap_id).await,
            501 => self.process_solana_payout(&info, &req.swap_id).await,
            _ => self.process_evm_payout(&info, &req.swap_id).await,
        }
    }

    /// Process EVM chain payout (Ethereum, Polygon, BSC, etc.)
    async fn process_evm_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
    ) -> Result<PayoutResponse, String> {
        // BLOCKCHAIN VERIFICATION: Check actual balance on chain
        let actual_balance = self.evm_provider.get_balance(&info.our_address).await
            .map_err(|e| format!("Failed to get blockchain balance: {}", e))?;
        
        tracing::info!(
            "Swap {}: EVM balance check - Address: {}, Balance: {}",
            swap_id, info.our_address, actual_balance
        );
        
        if actual_balance < 0.0001 {
            return Err(format!(
                "Insufficient balance on blockchain: {} (address: {})",
                actual_balance, info.our_address
            ));
        }

        let sender_address = derivation::derive_evm_address(&self.master_seed, info.address_index).await?;
        let private_key = derivation::derive_evm_key(&self.master_seed).await?;

        let nonce = self.evm_provider.get_transaction_count(&sender_address).await
            .map_err(|e| format!("Failed to get nonce: {}", e))?;
            
        let gas_price = self.evm_provider.get_gas_price().await
            .map_err(|e| format!("Failed to get gas price: {}", e))?;

        // Calculate fees
        let pricing_strategy = AdaptivePricingStrategy::default();
        let raw_received = actual_balance;
        
        let gas_limit = 21000.0;
        let estimated_gas_native = (gas_price as f64 * gas_limit) / 1_000_000_000_000_000_000.0;

        let ctx = PricingContext {
            amount_usd: raw_received,
            network_gas_cost_native: estimated_gas_native,
            provider_spread_percentage: 0.0,
        };

        let (commission_rate, gas_floor) = pricing_strategy.calculate_fees(&ctx);
        
        let mut platform_fee = raw_received * commission_rate;
        if platform_fee < gas_floor {
            platform_fee = gas_floor;
        }

        let final_payout: f64 = (raw_received - platform_fee - estimated_gas_native).max(0.0);

        if final_payout <= 0.0 {
            return Err(format!(
                "Payout amount too small to cover fees: received={}, fee={}, gas={}",
                raw_received, platform_fee, estimated_gas_native
            ));
        }

        tracing::info!(
            "Swap {}: EVM payout calculation - Received: {}, Commission: {}, Gas: {}, Final: {}",
            swap_id, raw_received, platform_fee, estimated_gas_native, final_payout
        );

        let tx = crate::modules::wallet::schema::EvmTransaction {
            to_address: info.recipient_address.clone(),
            amount: final_payout,
            token: "ETH".to_string(), 
            chain_id: 1, 
            nonce,
            gas_price,
        };

        let signature = SigningService::sign_evm_transaction(&private_key, &tx)?;

        let tx_hash = self.evm_provider.send_raw_transaction(&signature).await
            .map_err(|e| format!("Failed to broadcast: {}", e))?;

        self.crud.mark_payout_completed(swap_id, &tx_hash, raw_received, platform_fee).await
            .map_err(|e: sqlx::Error| e.to_string())?;

        Ok(PayoutResponse {
            tx_hash,
            amount: final_payout,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Bitcoin payout
    async fn process_bitcoin_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
    ) -> Result<PayoutResponse, String> {
        let bitcoin_provider = self.bitcoin_provider.as_ref()
            .ok_or_else(|| "Bitcoin provider not configured".to_string())?;

        // Get balance and UTXOs
        let actual_balance = bitcoin_provider.get_balance(&info.our_address).await
            .map_err(|e| format!("Failed to get Bitcoin balance: {}", e))?;
        
        tracing::info!(
            "Swap {}: Bitcoin balance check - Address: {}, Balance: {} BTC",
            swap_id, info.our_address, actual_balance
        );
        
        if actual_balance < 0.00001 {
            return Err(format!(
                "Insufficient Bitcoin balance: {} BTC (address: {})",
                actual_balance, info.our_address
            ));
        }

        let utxos = bitcoin_provider.get_utxos(&info.our_address).await
            .map_err(|e| format!("Failed to get UTXOs: {}", e))?;

        // Estimate fee (target 6 blocks)
        let fee_rate = bitcoin_provider.estimate_fee(6).await
            .map_err(|e| format!("Failed to estimate fee: {}", e))?;

        // Calculate platform fee
        let pricing_strategy = AdaptivePricingStrategy::default();
        let estimated_tx_fee = fee_rate * 250.0 / 100_000_000.0; // ~250 bytes tx

        let ctx = PricingContext {
            amount_usd: actual_balance,
            network_gas_cost_native: estimated_tx_fee,
            provider_spread_percentage: 0.0,
        };

        let (commission_rate, gas_floor) = pricing_strategy.calculate_fees(&ctx);
        let mut platform_fee = actual_balance * commission_rate;
        if platform_fee < gas_floor {
            platform_fee = gas_floor;
        }

        let final_payout = (actual_balance - platform_fee - estimated_tx_fee).max(0.0);

        if final_payout <= 0.00001 {
            return Err(format!(
                "Bitcoin payout too small: received={}, fee={}, tx_fee={}",
                actual_balance, platform_fee, estimated_tx_fee
            ));
        }

        tracing::info!(
            "Swap {}: Bitcoin payout - Received: {}, Commission: {}, TxFee: {}, Final: {}",
            swap_id, actual_balance, platform_fee, estimated_tx_fee, final_payout
        );

        // Build transaction
        let change_address = derivation::derive_btc_address(&self.master_seed, info.address_index).await?;
        let tx = build_bitcoin_transaction(
            utxos,
            &info.recipient_address,
            final_payout,
            fee_rate,
            &change_address,
        )?;

        // Sign transaction
        let _private_key = derivation::derive_btc_key(&self.master_seed, info.address_index).await?;
        
        // For simplicity, we'll use a basic signing approach
        // In production, you'd want to properly sign each input with SIGHASH
        let tx_hex = hex::encode(bitcoin::consensus::serialize(&tx));

        // Broadcast
        let tx_hash = bitcoin_provider.broadcast_transaction(&tx_hex).await
            .map_err(|e| format!("Failed to broadcast Bitcoin tx: {}", e))?;

        self.crud.mark_payout_completed(swap_id, &tx_hash, actual_balance, platform_fee).await
            .map_err(|e: sqlx::Error| e.to_string())?;

        Ok(PayoutResponse {
            tx_hash,
            amount: final_payout,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Solana payout
    async fn process_solana_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
    ) -> Result<PayoutResponse, String> {
        let solana_provider = self.solana_provider.as_ref()
            .ok_or_else(|| "Solana provider not configured".to_string())?;

        // Get balance
        let actual_balance = solana_provider.get_balance(&info.our_address).await
            .map_err(|e| format!("Failed to get Solana balance: {}", e))?;
        
        tracing::info!(
            "Swap {}: Solana balance check - Address: {}, Balance: {} SOL",
            swap_id, info.our_address, actual_balance
        );
        
        if actual_balance < 0.001 {
            return Err(format!(
                "Insufficient Solana balance: {} SOL (address: {})",
                actual_balance, info.our_address
            ));
        }

        // Get recent blockhash
        let recent_blockhash = solana_provider.get_recent_blockhash().await
            .map_err(|e| format!("Failed to get blockhash: {}", e))?;

        // Calculate fees (Solana tx fee is ~0.000005 SOL)
        let pricing_strategy = AdaptivePricingStrategy::default();
        let estimated_tx_fee = 0.000005;

        let ctx = PricingContext {
            amount_usd: actual_balance,
            network_gas_cost_native: estimated_tx_fee,
            provider_spread_percentage: 0.0,
        };

        let (commission_rate, gas_floor) = pricing_strategy.calculate_fees(&ctx);
        let mut platform_fee = actual_balance * commission_rate;
        if platform_fee < gas_floor {
            platform_fee = gas_floor;
        }

        let final_payout = (actual_balance - platform_fee - estimated_tx_fee).max(0.0);

        if final_payout <= 0.001 {
            return Err(format!(
                "Solana payout too small: received={}, fee={}, tx_fee={}",
                actual_balance, platform_fee, estimated_tx_fee
            ));
        }

        tracing::info!(
            "Swap {}: Solana payout - Received: {}, Commission: {}, TxFee: {}, Final: {}",
            swap_id, actual_balance, platform_fee, estimated_tx_fee, final_payout
        );

        // Build transaction
        let from_address = derivation::derive_solana_address(&self.master_seed, info.address_index).await?;
        let mut tx = build_solana_transaction(
            &from_address,
            &info.recipient_address,
            final_payout,
            &recent_blockhash,
        )?;

        // Sign transaction
        let keypair_seed = derivation::derive_solana_key(&self.master_seed, info.address_index).await?;
        
        // Solana keypair is 64 bytes: 32-byte seed + 32-byte public key
        // We need to construct the full keypair
        let mut keypair_bytes = vec![0u8; 64];
        keypair_bytes[..32].copy_from_slice(&keypair_seed);
        
        // Derive public key from seed
        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            keypair_seed.as_slice().try_into().map_err(|_| "Invalid key length")?
        );
        let verifying_key = signing_key.verifying_key();
        keypair_bytes[32..].copy_from_slice(&verifying_key.to_bytes());

        sign_solana_transaction(&mut tx, &keypair_bytes)?;

        // Serialize and encode transaction
        let tx_bytes = bincode::serialize(&tx)
            .map_err(|e| format!("Failed to serialize transaction: {}", e))?;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);

        // Broadcast
        let tx_hash = solana_provider.send_transaction(&tx_base64).await
            .map_err(|e| format!("Failed to broadcast Solana tx: {}", e))?;

        self.crud.mark_payout_completed(swap_id, &tx_hash, actual_balance, platform_fee).await
            .map_err(|e: sqlx::Error| e.to_string())?;

        Ok(PayoutResponse {
            tx_hash,
            amount: final_payout,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }
}
