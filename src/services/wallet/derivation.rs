/// High-level dispatcher for blockchain address derivation
/// 
/// This module routes address derivation requests to the appropriate
/// blockchain-specific implementation in the blockchains/ folder.
/// 
/// All actual derivation logic lives in src/services/wallet/blockchains/

use crate::services::wallet::blockchains::{
    BlockchainDerivation,
    Bitcoin, Litecoin, Dogecoin, BitcoinCash, DashDerivation, RavencoinDerivation, ZcashDerivation,
    Brc20Derivation, BitcoinLightningDerivation, BitcoinSvDerivation, BitcoinzDerivation,
    EvmChain,
    Solana,
    CosmosHubDerivation, OsmosisDerivation,
    PolkadotDerivation, KusamaDerivation,
    CardanoDerivation,
    MoneroDerivation,
    NeoDerivation,
    IconDerivation,
    Algorand, Near, TezosDerivation, XrpDerivation, StacksDerivation, StellarDerivation,
    TronDerivation, WavesDerivation, TonDerivation, VechainDerivation,
    SuiDerivation, EosDerivation, HederaDerivation, MinaDerivation,
    AptosDerivation, FlowDerivation, StarknetDerivation, ThetaDerivation,
    ZilliqaDerivation, MultiversxDerivation,
    NimiqDerivation, FluxDerivation, OntologyDerivation, PocketDerivation,
    OmniDerivation, ZanoDerivation, BinanceChainDerivation, PartisiaDerivation,
    DockDerivation, DefichainDerivation, BeamDerivation, EverscaleDerivation,
    TerraDerivation, FactomDerivation, AvalancheXDerivation,
};

// Re-export key derivation functions
pub use crate::services::wallet::blockchains::special::{derive_algorand_key, derive_near_key};

// Re-export trait helper
pub use crate::services::wallet::blockchains::traits::is_valid_seed_phrase;

/// Main entry point for address derivation
/// Routes to the appropriate blockchain implementation based on network
pub async fn derive_address(
    seed_phrase: &str,
    ticker: &str,
    network: &str,
    index: u32,
) -> Result<String, String> {
    let network_lower = network.to_lowercase();
    let ticker_lower = ticker.to_lowercase();

    // First, check ticker for unambiguous matches
    match ticker_lower.as_str() {
        "xmr" => return MoneroDerivation.derive_address(seed_phrase, index),
        "btc" if network_lower == "bitcoin" || network_lower == "mainnet" => {
            return Bitcoin.derive_address(seed_phrase, index);
        }
        "sol" => return Solana.derive_address(seed_phrase, index),
        "algo" => return Algorand.derive_address(seed_phrase, index),
        "near" => return Near.derive_address(seed_phrase, index),
        "ada" => return CardanoDerivation.derive_address(seed_phrase, index),
        "dot" => return PolkadotDerivation.derive_address(seed_phrase, index),
        "ksm" => return KusamaDerivation.derive_address(seed_phrase, index),
        "xrp" => return XrpDerivation.derive_address(seed_phrase, index),
        "trx" => return TronDerivation.derive_address(seed_phrase, index),
        "atom" => return CosmosHubDerivation.derive_address(seed_phrase, index),
        "sui" => return SuiDerivation.derive_address(seed_phrase, index),
        _ => {}
    }

    // Then check network for specific matches
    match network_lower.as_str() {
        // ===== BITCOIN FAMILY =====
        "bitcoin" | "btc" => Bitcoin.derive_address(seed_phrase, index),
        "litecoin" | "ltc" => Litecoin.derive_address(seed_phrase, index),
        "dogecoin" | "doge" => Dogecoin.derive_address(seed_phrase, index),
        "bitcoin_cash" | "bch" => BitcoinCash.derive_address(seed_phrase, index),
        "dash" => DashDerivation.derive_address(seed_phrase, index),
        "ravencoin" | "rvn" => RavencoinDerivation.derive_address(seed_phrase, index),
        "zcash" | "zec" => ZcashDerivation.derive_address(seed_phrase, index),
        "brc20" | "bitcoin_brc20" => Brc20Derivation.derive_address(seed_phrase, index),
        "lightning" | "bitcoin_lightning" => BitcoinLightningDerivation.derive_address(seed_phrase, index),
        "bitcoin_sv" | "bsv" | "bchsv" => BitcoinSvDerivation.derive_address(seed_phrase, index),
        "bitcoinz" | "btcz" => BitcoinzDerivation.derive_address(seed_phrase, index),
        
        // ===== SOLANA =====
        "solana" | "sol" => Solana.derive_address(seed_phrase, index),
        
        // ===== COSMOS SDK =====
        "cosmos" | "cosmos_hub" => CosmosHubDerivation.derive_address(seed_phrase, index),
        "osmosis" => OsmosisDerivation.derive_address(seed_phrase, index),
        
        // ===== SUBSTRATE =====
        "polkadot" | "dot" => PolkadotDerivation.derive_address(seed_phrase, index),
        "kusama" | "ksm" => KusamaDerivation.derive_address(seed_phrase, index),
        
        // ===== SPECIAL CHAINS =====
        "cardano" | "ada" => CardanoDerivation.derive_address(seed_phrase, index),
        "monero" | "xmr" => MoneroDerivation.derive_address(seed_phrase, index),
        "neo" => NeoDerivation.derive_address(seed_phrase, index),
        "neo_n2" | "n2" | "n3" | "neo3" => NeoDerivation.derive_address(seed_phrase, index), // Neo N2 is old Neo, N3 is new Neo
        "icon" | "icx" => IconDerivation.derive_address(seed_phrase, index),
        "algorand" | "algo" => Algorand.derive_address(seed_phrase, index),
        "near" => Near.derive_address(seed_phrase, index),
        "tezos" | "xtz" => TezosDerivation.derive_address(seed_phrase, index),
        "ripple" | "xrp" => XrpDerivation.derive_address(seed_phrase, index),
        "stacks" | "stx" => StacksDerivation.derive_address(seed_phrase, index),
        "stellar" | "xlm" => StellarDerivation.derive_address(seed_phrase, index),
        "tron" | "trx" | "trc20" => TronDerivation.derive_address(seed_phrase, index),
        "waves" => WavesDerivation.derive_address(seed_phrase, index),
        "ton" => TonDerivation.derive_address(seed_phrase, index),
        "vechain" | "vet" => VechainDerivation.derive_address(seed_phrase, index),
        "sui" => SuiDerivation.derive_address(seed_phrase, index),
        "eos" => EosDerivation.derive_address(seed_phrase, index),
        "hedera" | "hbar" => HederaDerivation.derive_address(seed_phrase, index),
        "mina" => MinaDerivation.derive_address(seed_phrase, index),
        "aptos" | "apt" => AptosDerivation.derive_address(seed_phrase, index),
        "flow" => FlowDerivation.derive_address(seed_phrase, index),
        "starknet" | "stark" => StarknetDerivation.derive_address(seed_phrase, index),
        "theta" => ThetaDerivation.derive_address(seed_phrase, index),
        "zilliqa" | "zil" => ZilliqaDerivation.derive_address(seed_phrase, index),
        "multiversx" | "egld" => MultiversxDerivation.derive_address(seed_phrase, index),
        "nimiq" | "nim" => NimiqDerivation.derive_address(seed_phrase, index),
        "flux" | "zel" => FluxDerivation.derive_address(seed_phrase, index),
        "ontology" | "ont" => OntologyDerivation.derive_address(seed_phrase, index),
        "pocket" | "pokt" => PocketDerivation.derive_address(seed_phrase, index),
        "omni" => OmniDerivation.derive_address(seed_phrase, index),
        "zano" => ZanoDerivation.derive_address(seed_phrase, index),
        "binance_chain" | "bep2" => BinanceChainDerivation.derive_address(seed_phrase, index),
        "partisia" | "mpc" => PartisiaDerivation.derive_address(seed_phrase, index),
        "dock" => DockDerivation.derive_address(seed_phrase, index),
        "defichain" | "dfi" => DefichainDerivation.derive_address(seed_phrase, index),
        "beam" => BeamDerivation.derive_address(seed_phrase, index),
        "everscale" | "freeton" | "ever" => EverscaleDerivation.derive_address(seed_phrase, index),
        "terra" | "terra_classic" | "luna" | "lunc" => TerraDerivation.derive_address(seed_phrase, index),
        "factom" | "fct" => FactomDerivation.derive_address(seed_phrase, index),
        "avalanche_x" | "avaxx" => AvalancheXDerivation.derive_address(seed_phrase, index),
        "a2z" => EvmChain::ethereum().derive_address(seed_phrase, index), // A2Z is Ethereum fork
        "shielded" => ZcashDerivation.derive_address(seed_phrase, index), // Zcash shielded variant
        "strk" => StarknetDerivation.derive_address(seed_phrase, index), // Starknet duplicate ticker
        
        // ===== EVM FAMILY (80+ chains) =====
        // Trocador aliases: ETH, MAINNET, MATIC, AVAXC, FTM, KAI, KAIA, KIP7, KLAY, 
        // MANTA, METALL2, SEIEVM, SMARTCHAIN, SYSNEVM, TLOSEVM, HAQQ, HYPEREVM, 
        // ISLMEVM, FILEVM, FITFI, FLR, CHZ, CFXCORE, BTT, BERA, OAS, PULSE, RSK, 
        // STARK, STRAX, KATANA, LAVA, KLC
        "ethereum" | "eth" | "mainnet" | "polygon" | "matic" | "bsc" | "smartchain" 
        | "arbitrum" | "optimism" | "erc20" | "bep20" 
        | "base" | "avalanche" | "avaxc" | "fantom" | "ftm" | "celo" | "harmony" 
        | "klaytn" | "klay" | "kai" | "kaia" | "kip7" | "metis" | "metall2"
        | "boba" | "gnosis" | "fuse" | "iotex" | "scroll" | "zksync" | "linea" 
        | "mantle" | "manta_pacific" | "manta" | "mode" | "blast" | "taiko" | "zora" | "sonic" 
        | "moonbeam" | "moonriver" | "aurora" | "cronos" | "evmos" | "kava" 
        | "oasis" | "oasis sapphire" | "rootstock" | "rsk" | "syscoin" | "sysnevm" 
        | "telos" | "tlosevm" | "thundercore" 
        | "tomochain" | "velas" | "wanchain" | "whitechain" | "x_layer" | "zkfair" 
        | "shibarium" | "opbnb" | "fraxtal" | "merlin" | "morph" | "redbelly" 
        | "rei" | "step_network" | "fitfi" | "stratis" | "strax" | "cyber" | "endurance" | "gravity" 
        | "hyper_evm" | "hyperevm" | "iota_evm" | "islm_evm" | "islmevm" | "haqq" 
        | "okx_chain" | "oasys" | "oas" | "peaq" 
        | "pulsechain" | "pulse" | "ronin" | "zeta" | "astar" | "bitgert" | "botanix" 
        | "bttc" | "btt" | "cfx" | "cfxcore" | "chiliz" | "chz" | "conflux_espace" 
        | "core" | "filecoin" | "filevm" 
        | "flare" | "flr" | "kcc" | "klc" | "bahamut" | "b2" | "berachain" | "bera" 
        | "apechain" | "katana" | "lava" | "sei" | "seievm" => {
            EvmChain::ethereum().derive_address(seed_phrase, index)
        }
        
        // ===== LEGACY IMPLEMENTATIONS (in old module files) =====
        _ => {
            Err(format!("Blockchain '{}' not yet migrated to modular structure", network))
        }
    }
}

// Stub functions for backward compatibility - these call the modular implementations
pub async fn derive_evm_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    EvmChain::ethereum().derive_address(seed_phrase, index)
}

pub async fn derive_btc_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Bitcoin.derive_address(seed_phrase, index)
}

pub async fn derive_solana_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Solana.derive_address(seed_phrase, index)
}

pub async fn derive_algorand_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Algorand.derive_address(seed_phrase, index)
}

pub async fn derive_near_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Near.derive_address(seed_phrase, index)
}

pub async fn derive_cardano_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    CardanoDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_polkadot_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    PolkadotDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_ripple_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    XrpDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_tron_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    TronDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_cosmos_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    CosmosHubDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_sui_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    SuiDerivation.derive_address(seed_phrase, index)
}

pub async fn sign_message_with_seed(_seed_phrase: &str, _index: u32, _message: &str) -> Result<String, String> {
    // TODO: Implement message signing per blockchain
    Err("Message signing not yet implemented".to_string())
}

// Key derivation functions (for signing) - these route to blockchain implementations
pub async fn derive_evm_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    EvmChain::ethereum().derive_private_key(seed_phrase, index)
}

pub async fn derive_btc_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    Bitcoin.derive_private_key(seed_phrase, index)
}

pub async fn derive_solana_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    Solana.derive_private_key(seed_phrase, index)
}

pub async fn derive_cosmos_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    CosmosHubDerivation.derive_private_key(seed_phrase, index)
}

pub async fn derive_substrate_seed(seed_phrase: &str, index: u32) -> Result<Vec<u8>, String> {
    // Substrate uses seed bytes instead of hex key
    let key_hex = PolkadotDerivation.derive_private_key(seed_phrase, index)?;
    hex::decode(key_hex.trim_start_matches("0x"))
        .map_err(|e| format!("Failed to decode substrate seed: {}", e))
}


