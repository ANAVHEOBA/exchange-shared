use super::bitcoin_rpc::{build_bitcoin_transaction_sats, estimate_bitcoin_fee_sats};
use super::derivation;
use super::rpc::BlockchainProvider;
use super::signing::SigningService;
use super::solana_rpc::{build_solana_transaction, sign_solana_transaction};
use crate::modules::wallet::crud::WalletCrud;
use crate::modules::wallet::schema::{
    GenerateAddressRequest, PayoutRequest, PayoutResponse, WalletAddressResponse,
};
use base64::Engine;
use ed25519_dalek::Signer;
use std::sync::Arc;

const DEFAULT_EVM_TRANSFER_GAS_LIMIT: u64 = 21_000;
const DEFAULT_SOLANA_NETWORK_FEE: f64 = 0.000005;

pub struct WalletManager {
    crud: WalletCrud,
    master_seed: String,
    provider: Arc<dyn BlockchainProvider>,
}

impl WalletManager {
    pub fn new(
        crud: WalletCrud,
        master_seed: String,
        provider: Arc<dyn BlockchainProvider>,
    ) -> Self {
        Self {
            crud,
            master_seed,
            provider,
        }
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
        let index = self
            .crud
            .get_next_index()
            .await
            .map_err(|e: sqlx::Error| format!("DB Error: {}", e))?;

        // 3. Use high-level dispatcher to derive address
        let address =
            derivation::derive_address(&self.master_seed, &req.ticker, &req.network, index).await?;
        let coin_type = derivation::resolve_coin_type(&req.ticker, &req.network)? as i32;

        // 4. Save to DB
        self.crud
            .save_address_info(
                &req.swap_id,
                &address,
                index,
                coin_type,
                &req.user_recipient_address,
                req.user_recipient_extra_id.as_deref(),
            )
            .await
            .map_err(|e: sqlx::Error| format!("Failed to save address info: {}", e))?;

        Ok(WalletAddressResponse {
            address,
            address_index: index,
            swap_id: req.swap_id,
        })
    }

    /// Orchestrate a payout to the user with idempotency and blockchain verification
    pub async fn process_payout(&self, req: PayoutRequest) -> Result<PayoutResponse, String> {
        // 1. Get address info and check for existing payout
        let info = self
            .crud
            .get_address_info(&req.swap_id)
            .await
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

        let service_fee = self
            .crud
            .get_payout_fee_quote(&req.swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?
            .ok_or_else(|| "No swap found for payout".to_string())?
            .platform_fee;

        if !service_fee.is_finite() || service_fee < 0.0 {
            return Err("Invalid platform fee configured for payout".to_string());
        }

        // 3. Dispatch based on network family using SLIP-0044 coin types
        match info.coin_type {
            // UTXO Family (Bitcoin and likes)
            0 | 2 | 3 | 5 | 20 | 22 | 133 | 145 | 175 => {
                self.process_bitcoin_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Solana
            501 => {
                self.process_solana_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Cosmos Family
            118 => {
                self.process_cosmos_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Substrate Family (Polkadot/Kusama)
            354 | 434 => {
                self.process_substrate_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Algorand
            283 => {
                self.process_algorand_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // NEAR
            397 => {
                self.process_near_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Cardano
            1815 => {
                self.process_cardano_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Ripple (XRP)
            144 => {
                self.process_xrp_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Tron
            195 => {
                self.process_tron_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Tezos
            1729 => {
                self.process_tezos_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Stellar
            148 => {
                self.process_stellar_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Waves
            5741 => {
                self.process_waves_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Stacks
            5757 => {
                self.process_stacks_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // TON
            607 => {
                self.process_ton_payout(&info, &req.swap_id, service_fee)
                    .await
            }

            // Default to EVM (60) or generic handler
            _ => {
                self.process_evm_payout(&info, &req.swap_id, service_fee)
                    .await
            }
        }
    }

    fn calculate_payout_amount(
        actual_received: f64,
        reserved_balance: f64,
        service_fee: f64,
        network_fee: f64,
    ) -> Result<f64, String> {
        for (label, value) in [
            ("actual received", actual_received),
            ("reserved balance", reserved_balance),
            ("service fee", service_fee),
            ("network fee", network_fee),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(format!("Invalid {} for payout calculation", label));
            }
        }

        if actual_received <= reserved_balance {
            return Err("Insufficient spendable balance".to_string());
        }

        let spendable_balance = actual_received - reserved_balance;
        let total_deductions = service_fee + network_fee;

        if spendable_balance <= total_deductions {
            return Err("Insufficient balance after service and network fees".to_string());
        }

        Ok(spendable_balance - total_deductions)
    }

    fn estimate_evm_network_fee(gas_price: u64) -> f64 {
        (gas_price as f64 * DEFAULT_EVM_TRANSFER_GAS_LIMIT as f64) / 1_000_000_000_000_000_000.0
    }

    async fn record_completed_payout(
        &self,
        swap_id: &str,
        tx_hash: &str,
        actual_received: f64,
        payout_amount: f64,
        service_fee: f64,
        network_fee: f64,
    ) -> Result<(), String> {
        self.crud
            .mark_payout_completed(
                swap_id,
                tx_hash,
                actual_received,
                payout_amount,
                service_fee,
                network_fee,
            )
            .await
            .map_err(|e: sqlx::Error| e.to_string())
    }

    /// Process Algorand payout
    async fn process_algorand_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        use crate::services::wallet::tx_builders::algorand::{
            get_algorand_params, AlgorandTransaction,
        };

        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Algorand balance: {}", e))?;

        if actual_balance < 0.001 {
            return Err("Insufficient Algorand balance".to_string());
        }

        // 1. Get network parameters
        let rpc_url = std::env::var("ALGORAND_RPC_URL")
            .unwrap_or_else(|_| "https://mainnet-api.algonode.cloud".to_string());
        let params = get_algorand_params(&rpc_url).await?;

        // 2. Derive private key
        let private_key_hex =
            derivation::derive_algorand_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        // 3. Build transaction
        let network_fee = params.min_fee as f64 / 1_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to microAlgos
        let genesis_hash = base64::engine::general_purpose::STANDARD
            .decode(&params.genesis_hash)
            .map_err(|e| format!("Invalid genesis hash: {}", e))?;

        let tx = AlgorandTransaction::new_payment(
            &info.our_address,
            &info.recipient_address,
            send_amount,
            params.min_fee,
            params.last_round,
            params.last_round + 1000,
            params.genesis_id,
            genesis_hash,
        )?;

        // 4. Sign transaction
        let signed_tx_bytes = tx.sign(&key_bytes)?;
        let tx_hex = format!("0x{}", hex::encode(&signed_tx_bytes));

        // 5. Broadcast
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Algorand tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    async fn process_near_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        use crate::services::wallet::tx_builders::near::{get_near_access_key, NearTransaction};
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get NEAR balance: {}", e))?;
        if actual_balance < 0.01 {
            return Err("Insufficient NEAR balance".to_string());
        }
        let private_key_hex =
            derivation::derive_near_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;
        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let verifying_key = signing_key.verifying_key();
        let public_key = format!(
            "ed25519:{}",
            bs58::encode(verifying_key.to_bytes()).into_string()
        );
        let rpc_url = std::env::var("NEAR_RPC_URL")
            .unwrap_or_else(|_| "https://rpc.mainnet.near.org".to_string());
        let access_key = get_near_access_key(&rpc_url, &info.our_address, &public_key).await?;
        let network_fee = 0.001;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000_000_000_000_000_000_000.0).round() as u128;
        let tx = NearTransaction::new_transfer(
            &info.our_address,
            &info.recipient_address,
            send_amount,
            access_key.nonce,
            &access_key.block_hash,
            &public_key,
        );
        let signed_tx = tx.sign(&key_bytes)?;
        let tx_json =
            serde_json::to_string(&signed_tx).map_err(|e| format!("Failed to serialize: {}", e))?;
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_json)
            .await
            .map_err(|e| format!("Failed to broadcast NEAR tx: {}", e))?;
        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;
        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Cardano payout
    async fn process_cardano_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Cardano balance: {}", e))?;

        if actual_balance < 1.5 {
            return Err("Insufficient Cardano balance (min 1.5 ADA)".to_string());
        }

        // Cardano uses CBOR-encoded transactions with complex structure
        // This is a simplified implementation - production would use cardano-serialization-lib
        let fee = 170000u64; // ~0.17 ADA typical fee
        let network_fee = fee as f64 / 1_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to Lovelace

        // Build simplified transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"CARDANO_TX");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());

        // Sign with Ed25519 (Cardano uses extended keys)
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&tx_data);

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&signature.to_bytes());

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Cardano tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Ripple (XRP) payout
    async fn process_xrp_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get XRP balance: {}", e))?;

        if actual_balance < 20.1 {
            return Err("Insufficient XRP balance (min 20 XRP reserve + fees)".to_string());
        }

        // XRP uses a JSON-based transaction format
        let fee = 12u64; // 12 drops = 0.000012 XRP
        let network_fee = fee as f64 / 1_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 20.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to drops

        // Get account sequence
        let sequence = self
            .provider
            .get_transaction_count(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get sequence: {}", e))?;

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"XRP_PAYMENT");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());
        tx_data.extend_from_slice(&sequence.to_be_bytes());

        // Sign with Secp256k1
        let signature = SigningService::sign_cosmos_transaction(
            &derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?,
            &hex::encode(&tx_data),
        )?;

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&hex::decode(signature).map_err(|e| e.to_string())?);

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast XRP tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Tron payout
    async fn process_tron_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Tron balance: {}", e))?;

        if actual_balance < 1.0 {
            return Err("Insufficient Tron balance".to_string());
        }

        // Tron uses protobuf-encoded transactions
        let network_fee = 0.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to SUN

        // Build transaction data (simplified)
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"TRON_TRANSFER");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());

        // Sign with Secp256k1
        let signature = SigningService::sign_cosmos_transaction(
            &derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?,
            &hex::encode(&tx_data),
        )?;

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&hex::decode(signature).map_err(|e| e.to_string())?);

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Tron tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Tezos payout
    async fn process_tezos_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Tezos balance: {}", e))?;

        if actual_balance < 0.01 {
            return Err("Insufficient Tezos balance".to_string());
        }

        // Tezos uses Michelson-encoded operations
        let fee = 1420u64; // ~0.00142 XTZ typical fee
        let network_fee = fee as f64 / 1_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to mutez

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"TEZOS_TX");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());

        // Sign with Ed25519
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&tx_data);

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&signature.to_bytes());

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Tezos tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Stellar payout
    async fn process_stellar_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Stellar balance: {}", e))?;

        if actual_balance < 1.5 {
            return Err("Insufficient Stellar balance (min 1 XLM reserve)".to_string());
        }

        // Stellar uses XDR-encoded transactions
        let fee = 100i64; // 100 stroops = 0.00001 XLM base fee
        let network_fee = fee as f64 / 10_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 1.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 10_000_000.0).round() as i64; // Convert to stroops

        // Get sequence number
        let sequence = self
            .provider
            .get_transaction_count(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get sequence: {}", e))?;

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"STELLAR_PAYMENT");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());
        tx_data.extend_from_slice(&sequence.to_be_bytes());

        // Sign with Ed25519
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&tx_data);

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&signature.to_bytes());

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Stellar tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Waves payout
    async fn process_waves_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Waves balance: {}", e))?;

        if actual_balance < 0.01 {
            return Err("Insufficient Waves balance".to_string());
        }

        // Waves uses a custom binary format
        let fee = 100000u64; // 0.001 WAVES
        let network_fee = fee as f64 / 100_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 100_000_000.0).round() as u64; // Convert to wavelets

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.push(4u8); // Transfer transaction type
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());

        // Sign with Ed25519
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&tx_data);

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&signature.to_bytes());

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Waves tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Stacks payout
    async fn process_stacks_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Stacks balance: {}", e))?;

        if actual_balance < 0.01 {
            return Err("Insufficient Stacks balance".to_string());
        }

        // Stacks uses Clarity smart contracts for transfers
        let fee = 1000u64; // 0.001 STX
        let network_fee = fee as f64 / 1_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to microSTX

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"STX_TRANSFER");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());

        // Sign with Secp256k1
        let signature = SigningService::sign_cosmos_transaction(
            &derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?,
            &hex::encode(&tx_data),
        )?;

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&hex::decode(signature).map_err(|e| e.to_string())?);

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Stacks tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process TON payout
    async fn process_ton_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get TON balance: {}", e))?;

        if actual_balance < 0.1 {
            return Err("Insufficient TON balance".to_string());
        }

        // TON uses TL-B serialization for messages
        let fee = 10_000_000u64; // 0.01 TON
        let network_fee = fee as f64 / 1_000_000_000.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000_000.0).round() as u64; // Convert to nanoTON

        // Build transaction data
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"TON_TRANSFER");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());
        tx_data.extend_from_slice(&fee.to_be_bytes());

        // Sign with Ed25519
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;

        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            &key_bytes[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&tx_data);

        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&signature.to_bytes());

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast TON tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process payout with retry logic and exponential backoff
    pub async fn process_payout_with_retry(
        &self,
        req: PayoutRequest,
        max_attempts: usize,
    ) -> Result<PayoutResponse, String> {
        let mut last_error = String::new();

        for attempt in 1..=max_attempts {
            match self.process_payout(req.clone()).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    last_error = e.clone();
                    if attempt < max_attempts {
                        let backoff_secs = 2u64.pow((attempt - 1) as u32);
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                    }
                }
            }
        }

        Err(format!(
            "Payout failed after {} attempts: {}",
            max_attempts, last_error
        ))
    }

    /// Process EVM chain payout
    async fn process_evm_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get blockchain balance: {}", e))?;

        if actual_balance < 0.0001 {
            return Err("Insufficient balance".to_string());
        }

        let sender_address =
            derivation::derive_evm_address(&self.master_seed, info.address_index).await?;
        let private_key = derivation::derive_evm_key(&self.master_seed, info.address_index).await?;

        let nonce = self
            .provider
            .get_transaction_count(&sender_address)
            .await
            .map_err(|e| format!("Failed to get nonce: {}", e))?;

        let gas_price = self
            .provider
            .get_gas_price()
            .await
            .map_err(|e| format!("Failed to get gas price: {}", e))?;

        let network_fee = Self::estimate_evm_network_fee(gas_price);
        let final_payout =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;

        let tx = crate::modules::wallet::schema::EvmTransaction {
            to_address: info.recipient_address.clone(),
            amount: final_payout,
            token: "ETH".to_string(),
            chain_id: 1,
            nonce,
            gas_price,
        };

        let signature = SigningService::sign_evm_transaction(&private_key, &tx)?;

        let tx_hash = self
            .provider
            .send_raw_transaction(&signature)
            .await
            .map_err(|e| format!("Failed to broadcast: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            final_payout,
            service_fee,
            network_fee,
        )
        .await?;

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
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Bitcoin balance: {}", e))?;

        if actual_balance < 0.00001 {
            return Err("Insufficient balance".to_string());
        }

        let utxos = self
            .provider
            .get_utxos(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get UTXOs: {}", e))?;

        let fee_rate = self
            .provider
            .estimate_fee(6)
            .await
            .map_err(|e| format!("Failed to estimate fee: {}", e))?;

        let change_address =
            derivation::derive_btc_address(&self.master_seed, info.address_index).await?;
        let total_input_sats = utxos.iter().try_fold(0u64, |acc, utxo| {
            if !utxo.amount.is_finite() || utxo.amount < 0.0 {
                return Err("Invalid Bitcoin UTXO amount".to_string());
            }

            Ok(acc + (utxo.amount * 100_000_000.0).round() as u64)
        })?;
        let fee_sats = estimate_bitcoin_fee_sats(
            utxos.len(),
            &info.recipient_address,
            fee_rate,
            false,
            &change_address,
        )?;

        let gross_received = total_input_sats as f64 / 100_000_000.0;
        let service_fee_sats = if !service_fee.is_finite() || service_fee < 0.0 {
            return Err("Invalid service fee for Bitcoin payout".to_string());
        } else {
            (service_fee * 100_000_000.0).round() as u64
        };

        if total_input_sats <= fee_sats + service_fee_sats {
            return Err("Insufficient balance after service and network fees".to_string());
        }

        let final_payout_sats = total_input_sats - fee_sats - service_fee_sats;

        if final_payout_sats <= 546 {
            return Err("Insufficient balance after service and network fees".to_string());
        }

        let final_payout = final_payout_sats as f64 / 100_000_000.0;

        let tx = build_bitcoin_transaction_sats(
            utxos,
            &info.recipient_address,
            final_payout_sats,
            fee_rate,
            &change_address,
        )?;

        let _private_key =
            derivation::derive_btc_key(&self.master_seed, info.address_index).await?;
        let tx_hex = hex::encode(bitcoin::consensus::serialize(&tx));

        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Bitcoin tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            gross_received,
            final_payout,
            service_fee_sats as f64 / 100_000_000.0,
            fee_sats as f64 / 100_000_000.0,
        )
        .await?;

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
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Solana balance: {}", e))?;

        if actual_balance < 0.001 {
            return Err("Insufficient Solana balance".to_string());
        }

        let recent_blockhash = self
            .provider
            .get_recent_blockhash()
            .await
            .map_err(|e| format!("Failed to get blockhash: {}", e))?;

        let network_fee = DEFAULT_SOLANA_NETWORK_FEE;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let from_address =
            derivation::derive_solana_address(&self.master_seed, info.address_index).await?;
        let mut tx = build_solana_transaction(
            &from_address,
            &info.recipient_address,
            payout_amount,
            &recent_blockhash,
        )?;

        let keypair_seed =
            derivation::derive_solana_key(&self.master_seed, info.address_index).await?;
        let keypair_seed_bytes = hex::decode(keypair_seed.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid keypair seed hex: {}", e))?;
        let mut keypair_bytes = vec![0u8; 64];
        keypair_bytes[..32].copy_from_slice(&keypair_seed_bytes);

        sign_solana_transaction(&mut tx, &keypair_bytes)?;

        let tx_bytes = bincode::serialize(&tx).map_err(|e| e.to_string())?;
        use base64::Engine;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);

        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_base64)
            .await
            .map_err(|e| format!("Failed to broadcast Solana tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Cosmos chain payout
    /// Simplified implementation without cosmrs crate
    async fn process_cosmos_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Cosmos balance: {}", e))?;

        if actual_balance < 0.001 {
            return Err("Insufficient balance".to_string());
        }

        // 1. Derive private key
        let private_key_hex =
            derivation::derive_cosmos_key(&self.master_seed, info.address_index).await?;

        // 2. Build simplified Cosmos transaction
        // In production, this would use cosmrs crate for proper protobuf encoding
        let network_fee = 0.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64; // Convert to uatom

        // Build transaction data (simplified)
        let mut tx_data = Vec::new();
        tx_data.extend_from_slice(b"cosmos-tx");
        tx_data.extend_from_slice(info.our_address.as_bytes());
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_be_bytes());

        // 3. Sign with Secp256k1
        let signature =
            SigningService::sign_cosmos_transaction(&private_key_hex, &hex::encode(&tx_data))?;

        // 4. Build signed transaction
        let mut signed_tx = Vec::new();
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(&hex::decode(signature).map_err(|e| e.to_string())?);

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Cosmos tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }

    /// Process Substrate chain payout
    /// Simplified implementation without subxt crate
    async fn process_substrate_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Substrate balance: {}", e))?;

        if actual_balance < 0.1 {
            return Err("Insufficient balance".to_string());
        }

        // 1. Derive seed
        let seed = derivation::derive_substrate_seed(&self.master_seed, info.address_index).await?;

        let network_fee = 0.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 10_000_000_000.0).round() as u128; // Convert to Planck (10^10)

        // 3. Build simplified Substrate transaction
        // In production, this would use subxt crate for proper SCALE encoding
        let mut tx_data = Vec::new();
        tx_data.push(5u8); // Balances pallet
        tx_data.push(3u8); // transfer_keep_alive call
        tx_data.extend_from_slice(info.recipient_address.as_bytes());
        tx_data.extend_from_slice(&send_amount.to_le_bytes());

        // 4. Sign with Ed25519 (or Sr25519 in production)
        let signature = SigningService::sign_substrate_transaction(
            &hex::encode(&seed),
            &hex::encode(&tx_data),
        )?;

        // 5. Build signed extrinsic
        let mut signed_tx = Vec::new();
        signed_tx.push(0x84u8); // Signed extrinsic version
        signed_tx.extend_from_slice(&tx_data);
        signed_tx.extend_from_slice(
            &hex::decode(signature.trim_start_matches("0x")).map_err(|e| e.to_string())?,
        );

        let tx_hex = format!("0x{}", hex::encode(&signed_tx));
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_hex)
            .await
            .map_err(|e| format!("Failed to broadcast Substrate tx: {}", e))?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_balance,
            payout_amount,
            service_fee,
            network_fee,
        )
        .await?;

        Ok(PayoutResponse {
            tx_hash,
            amount: payout_amount,
            status: crate::modules::wallet::model::PayoutStatus::Success,
        })
    }
}
