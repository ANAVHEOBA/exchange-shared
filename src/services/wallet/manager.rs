use super::bitcoin_rpc::{build_bitcoin_transaction_sats, estimate_bitcoin_fee_sats};
use super::derivation;
use super::rpc::BlockchainProvider;
use super::signing::SigningService;
use super::solana_rpc::{build_solana_transaction, sign_solana_transaction};
use crate::modules::wallet::crud::{PayoutLockResult, WalletCrud};
use crate::modules::wallet::model::PayoutAssetMetadata;
use crate::modules::wallet::schema::{
    GenerateAddressRequest, PayoutRequest, PayoutResponse, WalletAddressResponse,
};
use crate::services::token::{from_base_units, to_base_units};
use crate::services::wallet::blockchains::encoding::tron_address_to_hex;
use crate::services::wallet::catalog::{mainnet_family, MainnetFamily};
use crate::services::wallet::validation::normalize_supported_recipient_extra_id;
use alloy::primitives::U256;
use base64::Engine;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;

const DEFAULT_EVM_TRANSFER_GAS_LIMIT: u64 = 21_000;
const DEFAULT_SOLANA_NETWORK_FEE: f64 = 0.000005;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PayoutRoute {
    Bitcoin,
    Solana,
    Cosmos,
    Substrate,
    Algorand,
    Near,
    Cardano,
    Xrp,
    Tron,
    Tezos,
    Stellar,
    Waves,
    Stacks,
    Ton,
    EvmNative { chain_id: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenPayoutRoute {
    Evm { chain_id: u32 },
    Trc20,
    Spl,
}

#[derive(Debug, Clone)]
struct ResolvedTokenPayout {
    contract_address: String,
    decimals: u8,
    gas_multiplier: f64,
    route: TokenPayoutRoute,
}

#[derive(Debug, Clone)]
struct PayoutContext {
    info: crate::modules::wallet::model::SwapAddressInfo,
    service_fee: f64,
}

fn resolve_payout_route(ticker: &str, network: &str) -> Result<PayoutRoute, String> {
    let ticker_lower = ticker.to_ascii_lowercase();
    let network_lower = network.to_ascii_lowercase();

    if network_lower == "mainnet" {
        return resolve_mainnet_payout_route(&ticker_lower, network);
    }

    match network_lower.as_str() {
        "bitcoin" | "btc" => Ok(PayoutRoute::Bitcoin),
        "solana" | "sol" => Ok(PayoutRoute::Solana),
        "cosmos" | "cosmos_hub" | "osmosis" | "juno" | "akash" | "injective" | "regen"
        | "stargaze" | "secret" | "band" | "ion" | "gravity" | "terra" | "terra_classic" => {
            Ok(PayoutRoute::Cosmos)
        }
        "polkadot" | "dot" | "kusama" | "ksm" | "acala" | "astar" | "shiden" | "parallel" => {
            Ok(PayoutRoute::Substrate)
        }
        "algorand" | "algo" => Ok(PayoutRoute::Algorand),
        "near" => Ok(PayoutRoute::Near),
        "cardano" | "ada" => Ok(PayoutRoute::Cardano),
        "ripple" | "xrp" => Ok(PayoutRoute::Xrp),
        "tron" | "trx" => Ok(PayoutRoute::Tron),
        "tezos" | "xtz" => Ok(PayoutRoute::Tezos),
        "stellar" | "xlm" => Ok(PayoutRoute::Stellar),
        "waves" => Ok(PayoutRoute::Waves),
        "stacks" | "stx" => Ok(PayoutRoute::Stacks),
        "ton" => Ok(PayoutRoute::Ton),
        _ => resolve_evm_payout_route(&ticker_lower, &network_lower)
            .ok_or_else(|| unsupported_payout_route_message(ticker, network)),
    }
}

fn resolve_mainnet_payout_route(ticker_lower: &str, network: &str) -> Result<PayoutRoute, String> {
    match mainnet_family(ticker_lower) {
        MainnetFamily::Bitcoin => Ok(PayoutRoute::Bitcoin),
        MainnetFamily::Solana => Ok(PayoutRoute::Solana),
        MainnetFamily::Algorand => Ok(PayoutRoute::Algorand),
        MainnetFamily::Near => Ok(PayoutRoute::Near),
        MainnetFamily::Cardano => Ok(PayoutRoute::Cardano),
        MainnetFamily::Ripple => Ok(PayoutRoute::Xrp),
        MainnetFamily::Tron => Ok(PayoutRoute::Tron),
        MainnetFamily::Stellar => Ok(PayoutRoute::Stellar),
        MainnetFamily::Tezos => Ok(PayoutRoute::Tezos),
        MainnetFamily::Waves => Ok(PayoutRoute::Waves),
        MainnetFamily::Stacks => Ok(PayoutRoute::Stacks),
        MainnetFamily::Ton => Ok(PayoutRoute::Ton),
        MainnetFamily::CosmosHub
        | MainnetFamily::Osmosis
        | MainnetFamily::Juno
        | MainnetFamily::Akash
        | MainnetFamily::Injective
        | MainnetFamily::Regen
        | MainnetFamily::Stargaze
        | MainnetFamily::Secret
        | MainnetFamily::Band
        | MainnetFamily::Ion
        | MainnetFamily::GravityBridge
        | MainnetFamily::Terra => Ok(PayoutRoute::Cosmos),
        MainnetFamily::Polkadot
        | MainnetFamily::Kusama
        | MainnetFamily::Acala
        | MainnetFamily::Astar
        | MainnetFamily::Shiden => Ok(PayoutRoute::Substrate),
        MainnetFamily::Evm if ticker_lower == "eth" => Ok(PayoutRoute::EvmNative { chain_id: 1 }),
        _ => Err(unsupported_payout_route_message(ticker_lower, network)),
    }
}

fn resolve_evm_payout_route(ticker_lower: &str, network_lower: &str) -> Option<PayoutRoute> {
    let chain_id = resolve_evm_chain_id(network_lower)?;
    let canonical_network = canonical_payout_asset_network(network_lower);

    let is_native_asset = match canonical_network.as_str() {
        "ethereum" => ticker_lower == "eth",
        "polygon" => matches!(ticker_lower, "matic" | "pol"),
        "bsc" => ticker_lower == "bnb",
        "arbitrum" | "optimism" | "base" | "aurora" => ticker_lower == "eth",
        "avalanche" => ticker_lower == "avax",
        "fantom" => ticker_lower == "ftm",
        "celo" => ticker_lower == "celo",
        "moonbeam" => ticker_lower == "glmr",
        "moonriver" => ticker_lower == "movr",
        "cronos" => ticker_lower == "cro",
        "evmos" => ticker_lower == "evmos",
        "kava" => ticker_lower == "kava",
        "harmony" => ticker_lower == "one",
        "ronin" => ticker_lower == "ron",
        "flare" => ticker_lower == "flr",
        "rootstock" => ticker_lower == "rbtc",
        "opbnb" => ticker_lower == "bnb",
        "gnosis" => ticker_lower == "xdai",
        _ => false,
    };

    is_native_asset.then_some(PayoutRoute::EvmNative { chain_id })
}

fn canonical_payout_asset_network(network: &str) -> String {
    match network.to_ascii_lowercase().as_str() {
        "ethereum" | "eth" | "erc20" => "ethereum".to_string(),
        "polygon" | "matic" => "polygon".to_string(),
        "bsc" | "smartchain" | "bep20" => "bsc".to_string(),
        "arbitrum" => "arbitrum".to_string(),
        "optimism" => "optimism".to_string(),
        "base" => "base".to_string(),
        "avalanche" | "avaxc" => "avalanche".to_string(),
        "fantom" | "ftm" => "fantom".to_string(),
        "celo" => "celo".to_string(),
        "moonbeam" => "moonbeam".to_string(),
        "moonriver" => "moonriver".to_string(),
        "cronos" => "cronos".to_string(),
        "aurora" => "aurora".to_string(),
        "evmos" => "evmos".to_string(),
        "kava" => "kava".to_string(),
        "harmony" => "harmony".to_string(),
        "ronin" => "ronin".to_string(),
        "flare" | "flr" => "flare".to_string(),
        "rootstock" | "rsk" => "rootstock".to_string(),
        "opbnb" => "opbnb".to_string(),
        "gnosis" => "gnosis".to_string(),
        "tron" | "trx" | "trc20" => "tron".to_string(),
        "sol" | "solana" | "spl" => "solana".to_string(),
        other => other.to_string(),
    }
}

fn resolve_evm_chain_id(network_lower: &str) -> Option<u32> {
    match canonical_payout_asset_network(network_lower).as_str() {
        "ethereum" => Some(1),
        "polygon" => Some(137),
        "bsc" => Some(56),
        "arbitrum" => Some(42_161),
        "optimism" => Some(10),
        "base" => Some(8_453),
        "avalanche" => Some(43_114),
        "fantom" => Some(250),
        "celo" => Some(42_220),
        "moonbeam" => Some(1_284),
        "moonriver" => Some(1_285),
        "cronos" => Some(25),
        "aurora" => Some(1_313_161_554),
        "evmos" => Some(9_001),
        "kava" => Some(2_222),
        "harmony" => Some(1_666_600_000),
        "ronin" => Some(2_020),
        "flare" => Some(14),
        "rootstock" => Some(30),
        "opbnb" => Some(204),
        "gnosis" => Some(100),
        _ => None,
    }
}

fn unsupported_payout_route_message(ticker: &str, network: &str) -> String {
    format!(
        "No exact payout handler is implemented for {}/{}. This route still needs a network-specific sender.",
        ticker, network
    )
}

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
        let context = self.load_payout_context(&req.swap_id).await?;

        match self
            .crud
            .acquire_payout_lock(&req.swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?
        {
            PayoutLockResult::Acquired => {}
            PayoutLockResult::AlreadyCompleted {
                tx_hash,
                payout_amount,
            } => {
                return Ok(PayoutResponse {
                    tx_hash,
                    amount: payout_amount,
                    status: crate::modules::wallet::model::PayoutStatus::Success,
                });
            }
            PayoutLockResult::InProgress => {
                return Err(Self::payout_in_progress_message(&req.swap_id));
            }
        }

        let result = self
            .process_payout_locked(&context.info, &req.swap_id, context.service_fee)
            .await;

        if result.is_err() {
            self.crud
                .mark_payout_failed(&req.swap_id)
                .await
                .map_err(|e: sqlx::Error| e.to_string())?;
        }

        result
    }

    async fn process_payout_locked(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        normalize_supported_recipient_extra_id(
            &info.payout_ticker,
            &info.payout_network,
            info.recipient_extra_id.as_deref(),
        )?;

        let lookup_network = canonical_payout_asset_network(&info.payout_network);
        let token_metadata = self
            .crud
            .get_payout_asset_metadata(&info.payout_ticker, &info.payout_network, &lookup_network)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?;

        if let Some(token_asset) = token_metadata
            .as_ref()
            .map(|metadata| resolve_token_payout(metadata, &info.payout_network))
            .transpose()?
            .flatten()
        {
            return match token_asset.route {
                TokenPayoutRoute::Evm { chain_id } => {
                    self.process_evm_token_payout(
                        info,
                        swap_id,
                        service_fee,
                        &token_asset,
                        chain_id,
                    )
                    .await
                }
                TokenPayoutRoute::Trc20 => {
                    self.process_trc20_payout(info, swap_id, service_fee, &token_asset)
                        .await
                }
                TokenPayoutRoute::Spl => Err(format!(
                    "SPL token payout broadcasting is not implemented yet for {}/{}.",
                    info.payout_ticker, info.payout_network
                )),
            };
        }

        // Dispatch using the exact saved payout route, not only SLIP-0044 family collapse.
        match resolve_payout_route(&info.payout_ticker, &info.payout_network)? {
            PayoutRoute::Bitcoin => {
                self.process_bitcoin_payout(info, swap_id, service_fee)
                    .await
            }
            PayoutRoute::Solana => self.process_solana_payout(info, swap_id, service_fee).await,
            PayoutRoute::Cosmos => self.process_cosmos_payout(info, swap_id, service_fee).await,
            PayoutRoute::Substrate => {
                self.process_substrate_payout(info, swap_id, service_fee)
                    .await
            }
            PayoutRoute::Algorand => {
                self.process_algorand_payout(info, swap_id, service_fee)
                    .await
            }
            PayoutRoute::Near => self.process_near_payout(info, swap_id, service_fee).await,
            PayoutRoute::Cardano => {
                self.process_cardano_payout(info, swap_id, service_fee)
                    .await
            }
            PayoutRoute::Xrp => self.process_xrp_payout(info, swap_id, service_fee).await,
            PayoutRoute::Tron => self.process_tron_payout(info, swap_id, service_fee).await,
            PayoutRoute::Tezos => self.process_tezos_payout(info, swap_id, service_fee).await,
            PayoutRoute::Stellar => {
                self.process_stellar_payout(info, swap_id, service_fee)
                    .await
            }
            PayoutRoute::Waves => self.process_waves_payout(info, swap_id, service_fee).await,
            PayoutRoute::Stacks => self.process_stacks_payout(info, swap_id, service_fee).await,
            PayoutRoute::Ton => self.process_ton_payout(info, swap_id, service_fee).await,
            PayoutRoute::EvmNative { chain_id } => {
                self.process_evm_payout(info, swap_id, service_fee, chain_id)
                    .await
            }
        }
    }

    async fn load_payout_context(&self, swap_id: &str) -> Result<PayoutContext, String> {
        let info = self
            .crud
            .get_address_info(swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?
            .ok_or_else(|| "No address info found for swap".to_string())?;

        if let Some(tx_hash) = info.payout_tx_hash.clone() {
            return Ok(PayoutContext {
                info: crate::modules::wallet::model::SwapAddressInfo {
                    payout_tx_hash: Some(tx_hash),
                    ..info
                },
                service_fee: 0.0,
            });
        }

        let service_fee = self
            .crud
            .get_payout_fee_quote(swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?
            .ok_or_else(|| "No swap found for payout".to_string())?
            .platform_fee;

        if !service_fee.is_finite() || service_fee < 0.0 {
            return Err("Invalid platform fee configured for payout".to_string());
        }

        Ok(PayoutContext { info, service_fee })
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
        Self::estimate_evm_network_fee_for_gas(gas_price, DEFAULT_EVM_TRANSFER_GAS_LIMIT)
    }

    fn estimate_evm_network_fee_for_gas(gas_price: u64, gas_limit: u64) -> f64 {
        (gas_price as f64 * gas_limit as f64) / 1_000_000_000_000_000_000.0
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
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("Cardano", info))
    }

    /// Process Ripple (XRP) payout
    async fn process_xrp_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        use crate::services::wallet::tx_builders::xrp::XrpTransaction;

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

        let destination_tag = normalize_supported_recipient_extra_id(
            &info.payout_ticker,
            &info.payout_network,
            info.recipient_extra_id.as_deref(),
        )?
        .map(|value| {
            value.parse::<u32>().map_err(|_| {
                format!(
                    "Invalid XRP destination tag for swap {}: {}",
                    swap_id, value
                )
            })
        })
        .transpose()?;

        let private_key_hex =
            derivation::derive_ripple_key(&self.master_seed, info.address_index).await?;
        let mut tx = XrpTransaction::new_payment(
            &info.our_address,
            &info.recipient_address,
            send_amount,
            fee,
            sequence,
            destination_tag,
        );
        let signature = tx.sign(&private_key_hex)?;
        let tx_json = tx.to_blob(&signature)?;

        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_json)
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

        let network_fee = 0.0;
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;
        let send_amount = (payout_amount * 1_000_000.0).round() as u64;
        let sender_address =
            derivation::derive_tron_address(&self.master_seed, info.address_index).await?;
        let private_key =
            derivation::derive_tron_key(&self.master_seed, info.address_index).await?;
        Self::ensure_exact_sender_matches("Tron", info, &sender_address)?;

        let owner_address_hex = tron_address_to_hex(&sender_address)?;
        let recipient_address_hex = tron_address_to_hex(&info.recipient_address)?;
        let mut transaction = self
            .provider
            .tron_create_transaction(&owner_address_hex, &recipient_address_hex, send_amount)
            .await
            .map_err(|e| format!("Failed to create Tron transaction: {}", e))?;

        let signature = SigningService::sign_tron_transaction_id(&private_key, &transaction.tx_id)?;
        transaction.signature.push(signature);
        let tx_hash = self
            .provider
            .tron_broadcast_transaction(&transaction)
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

    async fn process_trc20_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
        token_asset: &ResolvedTokenPayout,
    ) -> Result<PayoutResponse, String> {
        let gas_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get TRX gas balance: {}", e))?;
        let network_fee = Self::estimate_tron_contract_fee(token_asset.gas_multiplier)?;

        if gas_balance <= network_fee {
            return Err("Insufficient TRX balance to pay TRC20 network fee".to_string());
        }

        let sender_address =
            derivation::derive_tron_address(&self.master_seed, info.address_index).await?;
        let private_key =
            derivation::derive_tron_key(&self.master_seed, info.address_index).await?;
        Self::ensure_exact_sender_matches("Tron", info, &sender_address)?;

        let owner_address_hex = tron_address_to_hex(&sender_address)?;
        let contract_address_hex = tron_address_to_hex(&token_asset.contract_address)?;
        let balance_parameter = SigningService::encode_trc20_balance_of_parameter(&sender_address)?;
        let balance_response = self
            .provider
            .tron_trigger_constant_contract(
                &owner_address_hex,
                &contract_address_hex,
                "balanceOf(address)",
                &balance_parameter,
            )
            .await
            .map_err(|e| format!("Failed to call TRC20 balanceOf: {}", e))?;

        let raw_balance = balance_response
            .constant_result
            .first()
            .ok_or_else(|| "TRC20 balanceOf returned no constant_result".to_string())?;
        let token_balance_base = Self::parse_evm_quantity_u256(raw_balance)?;

        if token_balance_base.is_zero() {
            return Err("Insufficient TRC20 token balance".to_string());
        }

        let actual_received_decimal =
            from_base_units(token_balance_base, token_asset.decimals).map_err(|e| e.to_string())?;
        let service_fee_decimal = Self::decimal_from_f64(service_fee, "service fee")?;

        if actual_received_decimal <= service_fee_decimal {
            return Err("Insufficient TRC20 token balance after service fee".to_string());
        }

        let payout_decimal = actual_received_decimal - service_fee_decimal;
        let payout_base_units =
            to_base_units(payout_decimal, token_asset.decimals).map_err(|e| e.to_string())?;
        let transfer_parameter = SigningService::encode_trc20_transfer_parameter(
            &info.recipient_address,
            payout_base_units,
        )?;
        let fee_limit_sun = (network_fee * 1_000_000.0).ceil() as u64;
        let trigger_response = self
            .provider
            .tron_trigger_smart_contract(
                &owner_address_hex,
                &contract_address_hex,
                "transfer(address,uint256)",
                &transfer_parameter,
                fee_limit_sun,
            )
            .await
            .map_err(|e| format!("Failed to build TRC20 transfer transaction: {}", e))?;

        if let Some(result) = trigger_response.result.as_ref() {
            if !result.result {
                return Err(format!(
                    "TRC20 contract trigger failed: {}",
                    result
                        .message
                        .clone()
                        .or(result.code.clone())
                        .unwrap_or_else(|| "unknown trigger error".to_string())
                ));
            }
        }

        let mut transaction = trigger_response
            .transaction
            .ok_or_else(|| "TRC20 trigger did not return a transaction".to_string())?;
        let signature = SigningService::sign_tron_transaction_id(&private_key, &transaction.tx_id)?;
        transaction.signature.push(signature);

        let tx_hash = self
            .provider
            .tron_broadcast_transaction(&transaction)
            .await
            .map_err(|e| format!("Failed to broadcast TRC20 transfer: {}", e))?;

        let actual_received = actual_received_decimal
            .to_f64()
            .ok_or_else(|| "Failed to convert TRC20 balance to payout units".to_string())?;
        let payout_amount = payout_decimal
            .to_f64()
            .ok_or_else(|| "Failed to convert TRC20 payout amount".to_string())?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_received,
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
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("Tezos", info))
    }

    /// Process Stellar payout
    async fn process_stellar_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        use crate::services::wallet::tx_builders::stellar::{
            StellarTransaction, STELLAR_MAINNET_PASSPHRASE,
        };

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

        let memo = normalize_supported_recipient_extra_id(
            &info.payout_ticker,
            &info.payout_network,
            info.recipient_extra_id.as_deref(),
        )?;

        let tx = StellarTransaction::new_payment(
            &info.our_address,
            &info.recipient_address,
            send_amount as u64,
            sequence,
            fee as u32,
            memo,
        );
        let private_key_hex =
            derivation::derive_stellar_key(&self.master_seed, info.address_index).await?;
        let key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid key hex: {}", e))?;
        let signature = tx.sign(&key_bytes, STELLAR_MAINNET_PASSPHRASE)?;
        let tx_payload = serde_json::to_string(&serde_json::json!({
            "transaction": tx,
            "signature": signature,
        }))
        .map_err(|e| format!("Failed to serialize Stellar tx: {}", e))?;
        let tx_hash = self
            .provider
            .send_raw_transaction(&tx_payload)
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
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("Waves", info))
    }

    /// Process Stacks payout
    async fn process_stacks_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("Stacks", info))
    }

    /// Process TON payout
    async fn process_ton_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("TON", info))
    }

    /// Process payout with retry logic and exponential backoff
    pub async fn process_payout_with_retry(
        &self,
        req: PayoutRequest,
        max_attempts: usize,
    ) -> Result<PayoutResponse, String> {
        if max_attempts == 0 {
            return Err("max_attempts must be at least 1".to_string());
        }

        let context = self.load_payout_context(&req.swap_id).await?;

        if let Some(tx_hash) = context.info.payout_tx_hash.clone() {
            return Ok(PayoutResponse {
                tx_hash,
                amount: context.info.payout_amount.unwrap_or(0.0),
                status: crate::modules::wallet::model::PayoutStatus::Success,
            });
        }

        match self
            .crud
            .acquire_payout_lock(&req.swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?
        {
            PayoutLockResult::Acquired => {}
            PayoutLockResult::AlreadyCompleted {
                tx_hash,
                payout_amount,
            } => {
                return Ok(PayoutResponse {
                    tx_hash,
                    amount: payout_amount,
                    status: crate::modules::wallet::model::PayoutStatus::Success,
                });
            }
            PayoutLockResult::InProgress => {
                return Err(Self::payout_in_progress_message(&req.swap_id));
            }
        }

        let mut last_error = String::new();

        for attempt in 1..=max_attempts {
            match self
                .process_payout_locked(&context.info, &req.swap_id, context.service_fee)
                .await
            {
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

        self.crud
            .mark_payout_failed(&req.swap_id)
            .await
            .map_err(|e: sqlx::Error| e.to_string())?;

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
        chain_id: u32,
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
        Self::ensure_evm_sender_matches(info, &sender_address)?;

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
        let raw_tx = SigningService::sign_evm_raw_transaction(
            &private_key,
            chain_id,
            nonce,
            gas_price,
            DEFAULT_EVM_TRANSFER_GAS_LIMIT,
            &info.recipient_address,
            SigningService::evm_amount_to_wei(final_payout)?,
            &[],
        )?;

        let tx_hash = self
            .provider
            .send_raw_transaction(&raw_tx)
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

    async fn process_evm_token_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        swap_id: &str,
        service_fee: f64,
        token_asset: &ResolvedTokenPayout,
        chain_id: u32,
    ) -> Result<PayoutResponse, String> {
        let gas_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get native gas balance: {}", e))?;

        let sender_address =
            derivation::derive_evm_address(&self.master_seed, info.address_index).await?;
        let private_key = derivation::derive_evm_key(&self.master_seed, info.address_index).await?;
        Self::ensure_evm_sender_matches(info, &sender_address)?;

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

        let gas_limit = Self::estimate_evm_token_gas_limit(token_asset.gas_multiplier)?;
        let network_fee = Self::estimate_evm_network_fee_for_gas(gas_price, gas_limit);

        if gas_balance <= network_fee {
            return Err("Insufficient native balance to pay EVM token gas".to_string());
        }

        let balance_call = SigningService::encode_erc20_balance_of_call(&sender_address)?;
        let raw_token_balance = self
            .provider
            .evm_call(&token_asset.contract_address, &balance_call)
            .await
            .map_err(|e| format!("Failed to call token balanceOf: {}", e))?;
        let token_balance_base = Self::parse_evm_quantity_u256(&raw_token_balance)?;

        if token_balance_base.is_zero() {
            return Err("Insufficient token balance".to_string());
        }

        let actual_received_decimal =
            from_base_units(token_balance_base, token_asset.decimals).map_err(|e| e.to_string())?;
        let service_fee_decimal = Self::decimal_from_f64(service_fee, "service fee")?;

        if actual_received_decimal <= service_fee_decimal {
            return Err("Insufficient token balance after service fee".to_string());
        }

        let payout_decimal = actual_received_decimal - service_fee_decimal;
        let payout_base_units =
            to_base_units(payout_decimal, token_asset.decimals).map_err(|e| e.to_string())?;
        let transfer_call =
            SigningService::encode_erc20_transfer_call(&info.recipient_address, payout_base_units)?;
        let transfer_call_bytes = hex::decode(transfer_call.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid token transfer calldata: {}", e))?;

        let raw_tx = SigningService::sign_evm_raw_transaction(
            &private_key,
            chain_id,
            nonce,
            gas_price,
            gas_limit,
            &token_asset.contract_address,
            U256::ZERO,
            &transfer_call_bytes,
        )?;

        let tx_hash = self
            .provider
            .send_raw_transaction(&raw_tx)
            .await
            .map_err(|e| format!("Failed to broadcast token transfer: {}", e))?;

        let actual_received = actual_received_decimal
            .to_f64()
            .ok_or_else(|| "Failed to convert token balance to payout units".to_string())?;
        let payout_amount = payout_decimal
            .to_f64()
            .ok_or_else(|| "Failed to convert token payout amount".to_string())?;

        self.record_completed_payout(
            swap_id,
            &tx_hash,
            actual_received,
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
    async fn process_cosmos_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message("Cosmos", info))
    }

    /// Process Substrate chain payout
    async fn process_substrate_payout(
        &self,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        _swap_id: &str,
        _service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        Err(Self::chain_native_builder_required_message(
            "Substrate",
            info,
        ))
    }

    fn decimal_from_f64(value: f64, label: &str) -> Result<Decimal, String> {
        if !value.is_finite() || value < 0.0 {
            return Err(format!("Invalid {} amount", label));
        }

        Decimal::from_str_exact(&value.to_string())
            .map_err(|e| format!("Invalid {} amount: {}", label, e))
    }

    fn parse_evm_quantity_u256(hex_value: &str) -> Result<U256, String> {
        let clean = hex_value.trim().trim_start_matches("0x");
        if clean.is_empty() {
            return Ok(U256::ZERO);
        }

        U256::from_str_radix(clean, 16)
            .map_err(|e| format!("Invalid EVM quantity returned by RPC: {}", e))
    }

    fn estimate_evm_token_gas_limit(gas_multiplier: f64) -> Result<u64, String> {
        if !gas_multiplier.is_finite() || gas_multiplier <= 0.0 {
            return Err("Invalid token gas multiplier".to_string());
        }

        Ok((65_000.0 * gas_multiplier.max(1.0) * 1.2).ceil() as u64)
    }

    fn estimate_tron_contract_fee(gas_multiplier: f64) -> Result<f64, String> {
        if !gas_multiplier.is_finite() || gas_multiplier <= 0.0 {
            return Err("Invalid token gas multiplier".to_string());
        }

        Ok(10.0 * gas_multiplier.max(1.0))
    }

    fn ensure_evm_sender_matches(
        info: &crate::modules::wallet::model::SwapAddressInfo,
        derived_sender: &str,
    ) -> Result<(), String> {
        if info.our_address.eq_ignore_ascii_case(derived_sender) {
            return Ok(());
        }

        Err(format!(
            "Stored internal address {} does not match derived EVM sender {}",
            info.our_address, derived_sender
        ))
    }

    fn ensure_exact_sender_matches(
        family: &str,
        info: &crate::modules::wallet::model::SwapAddressInfo,
        derived_sender: &str,
    ) -> Result<(), String> {
        if info.our_address == derived_sender {
            return Ok(());
        }

        Err(format!(
            "Stored internal {family} address {} does not match derived sender {}",
            info.our_address, derived_sender
        ))
    }

    fn payout_in_progress_message(swap_id: &str) -> String {
        format!("Payout already in progress for swap {}", swap_id)
    }

    fn chain_native_builder_required_message(
        route_name: &str,
        info: &crate::modules::wallet::model::SwapAddressInfo,
    ) -> String {
        format!(
            "{} payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            route_name, info.payout_ticker, info.payout_network
        )
    }
}

fn resolve_token_payout(
    metadata: &PayoutAssetMetadata,
    requested_network: &str,
) -> Result<Option<ResolvedTokenPayout>, String> {
    let Some(contract_address) = metadata
        .contract_address
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let requested_canonical = canonical_payout_asset_network(requested_network);
    let metadata_canonical = canonical_payout_asset_network(&metadata.network);

    if let Some(chain_id) = resolve_evm_chain_id(&requested_canonical)
        .or_else(|| resolve_evm_chain_id(&metadata_canonical))
    {
        return Ok(Some(ResolvedTokenPayout {
            contract_address: contract_address.to_string(),
            decimals: metadata.decimals,
            gas_multiplier: if metadata.gas_multiplier.is_finite() && metadata.gas_multiplier > 0.0
            {
                metadata.gas_multiplier
            } else {
                3.0
            },
            route: TokenPayoutRoute::Evm { chain_id },
        }));
    }

    if requested_canonical == "tron" || metadata_canonical == "tron" {
        return Ok(Some(ResolvedTokenPayout {
            contract_address: contract_address.to_string(),
            decimals: metadata.decimals,
            gas_multiplier: metadata.gas_multiplier,
            route: TokenPayoutRoute::Trc20,
        }));
    }

    if requested_canonical == "solana" || metadata_canonical == "solana" {
        return Ok(Some(ResolvedTokenPayout {
            contract_address: contract_address.to_string(),
            decimals: metadata.decimals,
            gas_multiplier: metadata.gas_multiplier,
            route: TokenPayoutRoute::Spl,
        }));
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::{resolve_payout_route, resolve_token_payout, PayoutRoute, TokenPayoutRoute};
    use crate::modules::wallet::model::PayoutAssetMetadata;

    #[test]
    fn ethereum_mainnet_uses_native_evm_route() {
        assert_eq!(
            resolve_payout_route("ETH", "ethereum").unwrap(),
            PayoutRoute::EvmNative { chain_id: 1 }
        );
    }

    #[test]
    fn base_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("ETH", "base").unwrap(),
            PayoutRoute::EvmNative { chain_id: 8_453 }
        );
    }

    #[test]
    fn token_route_is_not_silently_treated_as_native_evm() {
        let error = resolve_payout_route("USDT", "ERC20").unwrap_err();
        assert!(error.contains("No exact payout handler"));
    }

    #[test]
    fn litecoin_is_not_silently_routed_through_bitcoin_handler() {
        let error = resolve_payout_route("LTC", "litecoin").unwrap_err();
        assert!(error.contains("No exact payout handler"));
    }

    #[test]
    fn trc20_token_metadata_stays_explicitly_non_evm_native() {
        let metadata = PayoutAssetMetadata {
            symbol: "USDT".to_string(),
            network: "tron".to_string(),
            contract_address: Some("TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t".to_string()),
            decimals: 6,
            gas_multiplier: 3.0,
        };

        let resolved = resolve_token_payout(&metadata, "TRC20").unwrap().unwrap();
        assert_eq!(resolved.route, TokenPayoutRoute::Trc20);
    }

    #[test]
    fn bep20_token_metadata_maps_to_bsc_chain_id() {
        let metadata = PayoutAssetMetadata {
            symbol: "USDT".to_string(),
            network: "bsc".to_string(),
            contract_address: Some("0x55d398326f99059fF775485246999027B3197955".to_string()),
            decimals: 18,
            gas_multiplier: 3.0,
        };

        let resolved = resolve_token_payout(&metadata, "BEP20").unwrap().unwrap();
        assert_eq!(resolved.route, TokenPayoutRoute::Evm { chain_id: 56 });
    }
}
