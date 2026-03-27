use super::bitcoin_rpc::{build_bitcoin_transaction_sats, estimate_bitcoin_fee_sats};
use super::cosmos_rpc::{supported_cosmos_chain, CosmosChainConfig};
use super::derivation;
use super::rpc::BlockchainProvider;
use super::signing::SigningService;
use super::solana_rpc::{build_solana_transaction, sign_solana_transaction};
use crate::modules::wallet::crud::{PayoutLockResult, WalletCrud};
use crate::modules::wallet::model::PayoutAssetMetadata;
use crate::modules::wallet::schema::{
    GenerateAddressRequest, PayoutRequest, PayoutResponse, WalletAddressResponse,
};
use crate::services::rpc::{canonical_chain_key, chain_key_candidates};
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
    EvmNative { chain_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenPayoutRoute {
    Evm { chain_id: u64 },
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

#[derive(Debug, Clone, Copy)]
struct SupportedCosmosRoute {
    config: CosmosChainConfig,
}

#[derive(Debug, Clone, Copy)]
struct EvmNativeChainConfig {
    chain_id: u64,
    native_tickers: &'static [&'static str],
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
        | "stargaze" | "secret" | "band" | "ion" | "gravity" | "terra" | "terra_classic"
        | "agoric" | "axelar" | "cheqd" | "coreum" | "shentu" | "dydx" | "dymension" | "fetch"
        | "initia" | "kyve" | "neutron" | "oraichain" | "persistence" | "sei" | "celestia"
        | "thorchain" => Ok(PayoutRoute::Cosmos),
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
        | MainnetFamily::Cronos
        | MainnetFamily::Kava
        | MainnetFamily::Agoric
        | MainnetFamily::Axelar
        | MainnetFamily::Cheqd
        | MainnetFamily::Coreum
        | MainnetFamily::Shentu
        | MainnetFamily::Dydx
        | MainnetFamily::Dymension
        | MainnetFamily::Fetch
        | MainnetFamily::Initia
        | MainnetFamily::Kyve
        | MainnetFamily::Neutron
        | MainnetFamily::Oraichain
        | MainnetFamily::Persistence
        | MainnetFamily::Sei
        | MainnetFamily::Celestia
        | MainnetFamily::Terra
        | MainnetFamily::Thorchain => Ok(PayoutRoute::Cosmos),
        MainnetFamily::Polkadot
        | MainnetFamily::Kusama
        | MainnetFamily::Acala
        | MainnetFamily::Astar
        | MainnetFamily::Shiden => Ok(PayoutRoute::Substrate),
        MainnetFamily::Evm => resolve_evm_native_chain_config(ticker_lower, network)
            .map(|config| PayoutRoute::EvmNative {
                chain_id: config.chain_id,
            })
            .ok_or_else(|| unsupported_payout_route_message(ticker_lower, network)),
        _ => Err(unsupported_payout_route_message(ticker_lower, network)),
    }
}

fn resolve_evm_payout_route(ticker_lower: &str, network_lower: &str) -> Option<PayoutRoute> {
    resolve_evm_native_chain_config(ticker_lower, network_lower).map(|config| {
        PayoutRoute::EvmNative {
            chain_id: config.chain_id,
        }
    })
}

pub(crate) fn canonical_payout_asset_network(network: &str) -> String {
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

fn resolve_evm_chain_id(network_lower: &str) -> Option<u64> {
    let canonical_network = canonical_chain_key(network_lower);
    if let Some(config) = resolve_evm_chain_config_by_key(&canonical_network) {
        return Some(config.chain_id);
    }

    for candidate in chain_key_candidates("ETH", network_lower) {
        if let Some(config) = resolve_evm_chain_config_by_key(&candidate) {
            return Some(config.chain_id);
        }
    }

    None
}

fn resolve_evm_native_chain_config(ticker: &str, network: &str) -> Option<EvmNativeChainConfig> {
    let ticker_key = canonical_chain_key(ticker);

    for candidate in chain_key_candidates(ticker, network) {
        if let Some(config) = resolve_evm_chain_config_by_key(&candidate) {
            if config
                .native_tickers
                .iter()
                .any(|native| *native == ticker_key)
            {
                return Some(config);
            }
        }
    }

    None
}

fn resolve_evm_chain_config_by_key(chain_key: &str) -> Option<EvmNativeChainConfig> {
    match chain_key {
        "ethereum" => Some(EvmNativeChainConfig {
            chain_id: 1,
            native_tickers: &["eth"],
        }),
        "polygon" => Some(EvmNativeChainConfig {
            chain_id: 137,
            native_tickers: &["matic", "pol"],
        }),
        "bnb_smart_chain" | "bsc" => Some(EvmNativeChainConfig {
            chain_id: 56,
            native_tickers: &["bnb"],
        }),
        "arbitrum_one" | "arbitrum" => Some(EvmNativeChainConfig {
            chain_id: 42_161,
            native_tickers: &["eth"],
        }),
        "optimism" => Some(EvmNativeChainConfig {
            chain_id: 10,
            native_tickers: &["eth"],
        }),
        "base" => Some(EvmNativeChainConfig {
            chain_id: 8_453,
            native_tickers: &["eth"],
        }),
        "avalanche_c_chain" | "avalanche" | "avaxc" => Some(EvmNativeChainConfig {
            chain_id: 43_114,
            native_tickers: &["avax"],
        }),
        "fantom" => Some(EvmNativeChainConfig {
            chain_id: 250,
            native_tickers: &["ftm"],
        }),
        "celo" => Some(EvmNativeChainConfig {
            chain_id: 42_220,
            native_tickers: &["celo"],
        }),
        "moonbeam" => Some(EvmNativeChainConfig {
            chain_id: 1_284,
            native_tickers: &["glmr"],
        }),
        "moonriver" => Some(EvmNativeChainConfig {
            chain_id: 1_285,
            native_tickers: &["movr"],
        }),
        "cronos" => Some(EvmNativeChainConfig {
            chain_id: 25,
            native_tickers: &["cro"],
        }),
        "aurora" => Some(EvmNativeChainConfig {
            chain_id: 1_313_161_554,
            native_tickers: &["eth"],
        }),
        "evmos" => Some(EvmNativeChainConfig {
            chain_id: 9_001,
            native_tickers: &["evmos"],
        }),
        "kava_evm" | "kava" => Some(EvmNativeChainConfig {
            chain_id: 2_222,
            native_tickers: &["kava"],
        }),
        "harmony" => Some(EvmNativeChainConfig {
            chain_id: 1_666_600_000,
            native_tickers: &["one"],
        }),
        "ronin" => Some(EvmNativeChainConfig {
            chain_id: 2_020,
            native_tickers: &["ron"],
        }),
        "flare" => Some(EvmNativeChainConfig {
            chain_id: 14,
            native_tickers: &["flr"],
        }),
        "rootstock" | "rsk" => Some(EvmNativeChainConfig {
            chain_id: 30,
            native_tickers: &["rbtc"],
        }),
        "opbnb" => Some(EvmNativeChainConfig {
            chain_id: 204,
            native_tickers: &["bnb"],
        }),
        "gnosis" | "xdai" => Some(EvmNativeChainConfig {
            chain_id: 100,
            native_tickers: &["xdai"],
        }),
        "scroll" => Some(EvmNativeChainConfig {
            chain_id: 534_352,
            native_tickers: &["eth"],
        }),
        "zksync_era" | "zksync" => Some(EvmNativeChainConfig {
            chain_id: 324,
            native_tickers: &["eth"],
        }),
        "linea" => Some(EvmNativeChainConfig {
            chain_id: 59_144,
            native_tickers: &["eth"],
        }),
        "blast" => Some(EvmNativeChainConfig {
            chain_id: 81_457,
            native_tickers: &["eth"],
        }),
        "mode" => Some(EvmNativeChainConfig {
            chain_id: 34_443,
            native_tickers: &["eth"],
        }),
        "taiko" => Some(EvmNativeChainConfig {
            chain_id: 167_000,
            native_tickers: &["eth"],
        }),
        "zora" => Some(EvmNativeChainConfig {
            chain_id: 7_777_777,
            native_tickers: &["eth"],
        }),
        "morph" => Some(EvmNativeChainConfig {
            chain_id: 2_818,
            native_tickers: &["eth"],
        }),
        "metis" => Some(EvmNativeChainConfig {
            chain_id: 1_088,
            native_tickers: &["metis"],
        }),
        "mantle" => Some(EvmNativeChainConfig {
            chain_id: 5_000,
            native_tickers: &["mnt"],
        }),
        "syscoin_nevm" | "sysnevm" => Some(EvmNativeChainConfig {
            chain_id: 57,
            native_tickers: &["sys"],
        }),
        "songbird" => Some(EvmNativeChainConfig {
            chain_id: 19,
            native_tickers: &["sgb"],
        }),
        "wanchain" => Some(EvmNativeChainConfig {
            chain_id: 888,
            native_tickers: &["wan"],
        }),
        "telos" => Some(EvmNativeChainConfig {
            chain_id: 40,
            native_tickers: &["tlos"],
        }),
        "pulsechain" | "pulse" => Some(EvmNativeChainConfig {
            chain_id: 369,
            native_tickers: &["pls"],
        }),
        "bouncebit" => Some(EvmNativeChainConfig {
            chain_id: 6_001,
            native_tickers: &["bb"],
        }),
        "beam" => Some(EvmNativeChainConfig {
            chain_id: 4_337,
            native_tickers: &["beam"],
        }),
        "bahamut" => Some(EvmNativeChainConfig {
            chain_id: 5_165,
            native_tickers: &["ftn"],
        }),
        "canto" => Some(EvmNativeChainConfig {
            chain_id: 7_700,
            native_tickers: &["canto"],
        }),
        "chiliz" => Some(EvmNativeChainConfig {
            chain_id: 88_888,
            native_tickers: &["chz"],
        }),
        "core_dao" => Some(EvmNativeChainConfig {
            chain_id: 1_116,
            native_tickers: &["core"],
        }),
        "electroneum" => Some(EvmNativeChainConfig {
            chain_id: 52_014,
            native_tickers: &["etn"],
        }),
        "energy_web" => Some(EvmNativeChainConfig {
            chain_id: 246,
            native_tickers: &["ewt"],
        }),
        "ethereum_classic" => Some(EvmNativeChainConfig {
            chain_id: 61,
            native_tickers: &["etc"],
        }),
        "ethereumpow" => Some(EvmNativeChainConfig {
            chain_id: 10_001,
            native_tickers: &["ethw"],
        }),
        "filecoin" => Some(EvmNativeChainConfig {
            chain_id: 314,
            native_tickers: &["fil"],
        }),
        "findora" => Some(EvmNativeChainConfig {
            chain_id: 2_152,
            native_tickers: &["fra"],
        }),
        "fuse" => Some(EvmNativeChainConfig {
            chain_id: 122,
            native_tickers: &["fuse"],
        }),
        "graphlinq" => Some(EvmNativeChainConfig {
            chain_id: 614,
            native_tickers: &["glq"],
        }),
        "gmmt" => Some(EvmNativeChainConfig {
            chain_id: 8_989,
            native_tickers: &["gmmt"],
        }),
        "haqq" => Some(EvmNativeChainConfig {
            chain_id: 11_235,
            native_tickers: &["islm"],
        }),
        "hyper_evm" => Some(EvmNativeChainConfig {
            chain_id: 1_000,
            native_tickers: &["hype"],
        }),
        "humanode" => Some(EvmNativeChainConfig {
            chain_id: 5_234,
            native_tickers: &["hmnd"],
        }),
        "iota_evm" => Some(EvmNativeChainConfig {
            chain_id: 8_822,
            native_tickers: &["iota"],
        }),
        "iotex" => Some(EvmNativeChainConfig {
            chain_id: 4_689,
            native_tickers: &["iotx"],
        }),
        "japan_open_chain" => Some(EvmNativeChainConfig {
            chain_id: 81,
            native_tickers: &["joc"],
        }),
        "kaichain" => Some(EvmNativeChainConfig {
            chain_id: 2_989,
            native_tickers: &["kai"],
        }),
        "kcc" => Some(EvmNativeChainConfig {
            chain_id: 321,
            native_tickers: &["kcs"],
        }),
        "lisk" => Some(EvmNativeChainConfig {
            chain_id: 1_135,
            native_tickers: &["lsk"],
        }),
        "lukso" => Some(EvmNativeChainConfig {
            chain_id: 42,
            native_tickers: &["lyx"],
        }),
        "map_protocol" => Some(EvmNativeChainConfig {
            chain_id: 22_776,
            native_tickers: &["mapo"],
        }),
        "meter" => Some(EvmNativeChainConfig {
            chain_id: 82,
            native_tickers: &["mtrg"],
        }),
        "neon" => Some(EvmNativeChainConfig {
            chain_id: 245_022_934,
            native_tickers: &["neon"],
        }),
        "okx_chain" => Some(EvmNativeChainConfig {
            chain_id: 66,
            native_tickers: &["okt"],
        }),
        "redbelly" => Some(EvmNativeChainConfig {
            chain_id: 151,
            native_tickers: &["rbnt"],
        }),
        "rei_network" | "rei" => Some(EvmNativeChainConfig {
            chain_id: 47_805,
            native_tickers: &["rei"],
        }),
        "sei" => Some(EvmNativeChainConfig {
            chain_id: 1_329,
            native_tickers: &["sei"],
        }),
        "sophon" => Some(EvmNativeChainConfig {
            chain_id: 50_168,
            native_tickers: &["soph"],
        }),
        "supra" => Some(EvmNativeChainConfig {
            chain_id: 523_994_005_626,
            native_tickers: &["supra"],
        }),
        "step_network" => Some(EvmNativeChainConfig {
            chain_id: 1_234,
            native_tickers: &["fitfi"],
        }),
        "stratis_evm" | "strax" => Some(EvmNativeChainConfig {
            chain_id: 105_105,
            native_tickers: &["strx"],
        }),
        "thundercore" => Some(EvmNativeChainConfig {
            chain_id: 108,
            native_tickers: &["tt"],
        }),
        "tomochain" => Some(EvmNativeChainConfig {
            chain_id: 88,
            native_tickers: &["tomo"],
        }),
        "u2u" => Some(EvmNativeChainConfig {
            chain_id: 39,
            native_tickers: &["u2u"],
        }),
        "vanar" => Some(EvmNativeChainConfig {
            chain_id: 2_040,
            native_tickers: &["vanry"],
        }),
        "velas" => Some(EvmNativeChainConfig {
            chain_id: 106,
            native_tickers: &["vlx"],
        }),
        "viction" => Some(EvmNativeChainConfig {
            chain_id: 88,
            native_tickers: &["vic"],
        }),
        "x_layer" => Some(EvmNativeChainConfig {
            chain_id: 196,
            native_tickers: &["okb"],
        }),
        "zetachain" => Some(EvmNativeChainConfig {
            chain_id: 7_000,
            native_tickers: &["zeta"],
        }),
        _ => None,
    }
}

fn unsupported_payout_route_message(ticker: &str, network: &str) -> String {
    format!(
        "No exact payout handler is implemented for {}/{}. This route still needs a network-specific sender.",
        ticker, network
    )
}

fn resolve_supported_cosmos_route(ticker: &str, network: &str) -> Option<SupportedCosmosRoute> {
    let ticker_lower = ticker.to_ascii_lowercase();
    let network_lower = network.to_ascii_lowercase();

    let chain_key = if network_lower == "mainnet" {
        match mainnet_family(&ticker_lower) {
            MainnetFamily::Agoric => "agoric",
            MainnetFamily::Akash => "akash",
            MainnetFamily::Axelar => "axelar",
            MainnetFamily::Band => "band",
            MainnetFamily::Celestia => "celestia",
            MainnetFamily::Cheqd => "cheqd",
            MainnetFamily::Coreum => "coreum",
            MainnetFamily::CosmosHub => "cosmos_hub",
            MainnetFamily::Dydx => "dydx",
            MainnetFamily::Dymension => "dymension",
            MainnetFamily::Fetch => "fetch",
            MainnetFamily::Initia => "initia",
            MainnetFamily::Juno => "juno",
            MainnetFamily::Kyve => "kyve",
            MainnetFamily::Neutron => "neutron",
            MainnetFamily::Oraichain => "oraichain",
            MainnetFamily::Osmosis => "osmosis",
            MainnetFamily::Persistence => "persistence",
            MainnetFamily::Regen => "regen",
            MainnetFamily::Secret => "secret",
            MainnetFamily::Shentu => "shentu",
            MainnetFamily::Stargaze => "stargaze",
            MainnetFamily::Terra => "terra",
            _ => return None,
        }
    } else {
        match network_lower.as_str() {
            "agoric" => "agoric",
            "akash" => "akash",
            "axelar" => "axelar",
            "band" => "band",
            "celestia" => "celestia",
            "cheqd" => "cheqd",
            "coreum" => "coreum",
            "cosmos" | "cosmos_hub" => "cosmos_hub",
            "dydx" => "dydx",
            "dymension" => "dymension",
            "fetch" | "fetchhub" => "fetch",
            "initia" => "initia",
            "juno" => "juno",
            "kyve" => "kyve",
            "neutron" => "neutron",
            "oraichain" => "oraichain",
            "osmosis" => "osmosis",
            "persistence" => "persistence",
            "regen" => "regen",
            "secret" => "secret",
            "shentu" => "shentu",
            "stargaze" => "stargaze",
            "terra" => "terra",
            _ => return None,
        }
    };

    supported_cosmos_chain(chain_key).map(|config| SupportedCosmosRoute { config })
}

pub(crate) fn ensure_local_payout_capability(
    ticker: &str,
    network: &str,
    token_metadata: Option<&PayoutAssetMetadata>,
) -> Result<(), String> {
    if let Some(token_asset) = token_metadata
        .map(|metadata| resolve_token_payout(metadata, network))
        .transpose()?
        .flatten()
    {
        return match token_asset.route {
            TokenPayoutRoute::Evm { .. } | TokenPayoutRoute::Trc20 => Ok(()),
            TokenPayoutRoute::Spl => Err(format!(
                "SPL token payout broadcasting is not implemented yet for {}/{}.",
                ticker, network
            )),
        };
    }

    match resolve_payout_route(ticker, network)? {
        PayoutRoute::Bitcoin
        | PayoutRoute::Solana
        | PayoutRoute::Algorand
        | PayoutRoute::Near
        | PayoutRoute::Xrp
        | PayoutRoute::Tron
        | PayoutRoute::Stellar
        | PayoutRoute::EvmNative { .. } => Ok(()),
        PayoutRoute::Cosmos => resolve_supported_cosmos_route(ticker, network)
            .map(|_| ())
            .ok_or_else(|| {
                format!(
                    "Cosmos payout broadcasting for {}/{} is not implemented for this exact chain yet.",
                    ticker, network
                )
            }),
        PayoutRoute::Substrate => Err(format!(
            "Substrate payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
        PayoutRoute::Cardano => Err(format!(
            "Cardano payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
        PayoutRoute::Tezos => Err(format!(
            "Tezos payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
        PayoutRoute::Waves => Err(format!(
            "Waves payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
        PayoutRoute::Stacks => Err(format!(
            "Stacks payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
        PayoutRoute::Ton => Err(format!(
            "TON payout broadcasting for {}/{} is disabled until a chain-native transaction builder is implemented.",
            ticker, network
        )),
    }
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
        chain_id: u64,
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
        chain_id: u64,
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
        swap_id: &str,
        service_fee: f64,
    ) -> Result<PayoutResponse, String> {
        use crate::services::wallet::tx_builders::cosmos::CosmosSendTransaction;
        use secp256k1::{PublicKey, Secp256k1, SecretKey};

        let route = resolve_supported_cosmos_route(&info.payout_ticker, &info.payout_network)
            .ok_or_else(|| Self::chain_native_builder_required_message("Cosmos", info))?;

        let actual_balance = self
            .provider
            .get_balance(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Cosmos balance: {}", e))?;

        let fee_amount_base_units = route.config.fee_amount_base_units();
        let network_fee = route.config.network_fee_native();
        let payout_amount =
            Self::calculate_payout_amount(actual_balance, 0.0, service_fee, network_fee)?;

        let payout_decimal = Self::decimal_from_f64(payout_amount, "Cosmos payout")?;
        let send_amount_base =
            to_base_units(payout_decimal, route.config.decimals).map_err(|e| e.to_string())?;

        let private_key_hex = derivation::derive_exact_key(
            &self.master_seed,
            &info.payout_ticker,
            &info.payout_network,
            info.address_index,
        )
        .await?;

        let account_state = self
            .provider
            .cosmos_get_account_state(&info.our_address)
            .await
            .map_err(|e| format!("Failed to get Cosmos account state: {}", e))?;

        let private_key_bytes = hex::decode(private_key_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid Cosmos private key: {}", e))?;
        let secret_key = SecretKey::from_slice(&private_key_bytes)
            .map_err(|e| format!("Invalid Cosmos private key: {}", e))?;
        let public_key = PublicKey::from_secret_key(&Secp256k1::new(), &secret_key);
        let compressed_public_key = public_key.serialize();

        let memo = normalize_supported_recipient_extra_id(
            &info.payout_ticker,
            &info.payout_network,
            info.recipient_extra_id.as_deref(),
        )?;

        let tx = CosmosSendTransaction::new(
            info.our_address.clone(),
            info.recipient_address.clone(),
            send_amount_base.to_string(),
            route.config.denom.to_string(),
            fee_amount_base_units.to_string(),
            route.config.denom.to_string(),
            route.config.gas_limit,
            account_state.chain_id,
            account_state.account_number,
            account_state.sequence,
            memo,
        );

        let sign_doc_hex = hex::encode(tx.sign_doc_bytes(&compressed_public_key));
        let signature_hex =
            SigningService::sign_cosmos_transaction(&private_key_hex, &sign_doc_hex)
                .map_err(|e| format!("Failed to sign Cosmos tx: {}", e))?;
        let signature =
            hex::decode(signature_hex).map_err(|e| format!("Invalid Cosmos signature: {}", e))?;

        let tx_hex = format!(
            "0x{}",
            hex::encode(tx.signed_tx_bytes(&compressed_public_key, &signature))
        );
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
    use super::{
        ensure_local_payout_capability, resolve_evm_chain_config_by_key, resolve_payout_route,
        resolve_token_payout, PayoutRoute, TokenPayoutRoute,
    };
    use crate::modules::wallet::model::PayoutAssetMetadata;
    use crate::services::rpc::build_default_rpc_configs;
    use crate::services::wallet::cosmos_rpc::supported_cosmos_chain;

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
    fn metis_mainnet_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("METIS", "Mainnet").unwrap(),
            PayoutRoute::EvmNative { chain_id: 1_088 }
        );
    }

    #[test]
    fn scroll_eth_route_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("ETH", "SCROLL").unwrap(),
            PayoutRoute::EvmNative { chain_id: 534_352 }
        );
    }

    #[test]
    fn syscoin_mainnet_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("SYS", "Mainnet").unwrap(),
            PayoutRoute::EvmNative { chain_id: 57 }
        );
    }

    #[test]
    fn bouncebit_mainnet_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("BB", "Mainnet").unwrap(),
            PayoutRoute::EvmNative { chain_id: 6_001 }
        );
    }

    #[test]
    fn supra_mainnet_uses_exact_chain_id() {
        assert_eq!(
            resolve_payout_route("SUPRA", "Mainnet").unwrap(),
            PayoutRoute::EvmNative {
                chain_id: 523_994_005_626
            }
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

    #[test]
    fn local_payout_capability_accepts_supported_cosmos_routes() {
        ensure_local_payout_capability("ATOM", "Mainnet", None)
            .expect("cosmos hub should now be locally executable");
        ensure_local_payout_capability("AKT", "Mainnet", None)
            .expect("akash should now be locally executable");
        ensure_local_payout_capability("AXEL", "Mainnet", None)
            .expect("axelar should now be locally executable");
        ensure_local_payout_capability("CHEQ", "MAINNET", None)
            .expect("cheqd should now be locally executable");
        ensure_local_payout_capability("COREUM", "MAINNET", None)
            .expect("coreum should now be locally executable");
        ensure_local_payout_capability("DYDX", "MAINNET", None)
            .expect("dydx should now be locally executable");
        ensure_local_payout_capability("DYM", "MAINNET", None)
            .expect("dymension should now be locally executable");
        ensure_local_payout_capability("FET", "Mainnet", None)
            .expect("fetch should now be locally executable");
        ensure_local_payout_capability("INIT", "MAINNET", None)
            .expect("initia should now be locally executable");
        ensure_local_payout_capability("KYVE", "MAINNET", None)
            .expect("kyve should now be locally executable");
        ensure_local_payout_capability("NTRN", "MAINNET", None)
            .expect("neutron should now be locally executable");
        ensure_local_payout_capability("REGEN", "Mainnet", None)
            .expect("regen should now be locally executable");
    }

    #[test]
    fn local_payout_capability_rejects_unsupported_cosmos_routes() {
        let err = ensure_local_payout_capability("INJ", "MAINNET", None)
            .expect_err("unsupported cosmos variants should still be rejected");
        assert!(err.contains("not implemented for this exact chain"));
    }

    #[test]
    fn local_payout_capability_accepts_native_evm_routes() {
        ensure_local_payout_capability("ETH", "ERC20", None)
            .expect("ethereum native payouts should be locally executable");
        ensure_local_payout_capability("METIS", "Mainnet", None)
            .expect("metis mainnet payouts should now be locally executable");
        ensure_local_payout_capability("ETH", "SCROLL", None)
            .expect("scroll native eth payouts should now be locally executable");
        ensure_local_payout_capability("SYS", "Mainnet", None)
            .expect("syscoin nevm mainnet payouts should now be locally executable");
        ensure_local_payout_capability("SUPRA", "Mainnet", None)
            .expect("supra mainnet payouts should now be locally executable");
    }

    #[test]
    fn local_payout_capability_rejects_spl_tokens() {
        let metadata = PayoutAssetMetadata {
            symbol: "USDC".to_string(),
            network: "solana".to_string(),
            contract_address: Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()),
            decimals: 6,
            gas_multiplier: 1.0,
        };

        let err = ensure_local_payout_capability("USDC", "SPL", Some(&metadata))
            .expect_err("spl token payouts should remain disabled");
        assert!(err.contains("SPL token payout broadcasting"));
    }

    #[test]
    fn configured_rpc_catalog_reports_current_direct_local_send_coverage() {
        let configs = build_default_rpc_configs();

        let direct_local = configs
            .keys()
            .filter(|chain_key| {
                matches!(
                    chain_key.as_str(),
                    "bitcoin" | "solana" | "tron" | "algorand"
                ) || resolve_evm_chain_config_by_key(chain_key).is_some()
                    || supported_cosmos_chain(chain_key).is_some()
            })
            .count();

        assert_eq!(configs.len(), 223);
        assert_eq!(direct_local, 106);
    }
}
