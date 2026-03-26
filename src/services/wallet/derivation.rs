/// High-level dispatcher for blockchain address derivation
///
/// This module routes address derivation requests to the appropriate
/// blockchain-specific implementation in the blockchains/ folder.
///
/// All actual derivation logic lives in src/services/wallet/blockchains/
use crate::services::wallet::blockchains::encoding::{
    base58check_decode, base58check_encode, bech32_encode, hash160,
};
use crate::services::wallet::blockchains::{
    Algorand, AptosDerivation, AvalancheXDerivation, BeamDerivation, BinanceChainDerivation,
    Bitcoin, BitcoinCash, BitcoinLightningDerivation, BitcoinSvDerivation, BitcoinzDerivation,
    BlockchainDerivation, Brc20Derivation, CardanoDerivation, CosmosHubDerivation, DashDerivation,
    DefichainDerivation, DockDerivation, Dogecoin, EosDerivation, EverscaleDerivation, EvmChain,
    FactomDerivation, FlowDerivation, FluxDerivation, HederaDerivation, IconDerivation,
    KusamaDerivation, Litecoin, MinaDerivation, MoneroDerivation, MultiversxDerivation, Near,
    NeoDerivation, NimiqDerivation, OmniDerivation, OntologyDerivation, OsmosisDerivation,
    PartisiaDerivation, PocketDerivation, PolkadotDerivation, RavencoinDerivation, Solana,
    StacksDerivation, StarknetDerivation, StellarDerivation, SuiDerivation, TerraDerivation,
    TezosDerivation, ThetaDerivation, TonDerivation, TronDerivation, VechainDerivation,
    WavesDerivation, XrpDerivation, ZanoDerivation, ZcashDerivation, ZilliqaDerivation,
};
use crate::services::wallet::catalog::{mainnet_family, MainnetFamily};

// Re-export key derivation functions
pub use crate::services::wallet::blockchains::special::{derive_algorand_key, derive_near_key};

// Re-export trait helper
pub use crate::services::wallet::blockchains::traits::is_valid_seed_phrase;

struct GenericCosmosDerivation {
    prefix: &'static str,
    name: &'static str,
}

impl BlockchainDerivation for GenericCosmosDerivation {
    fn coin_type(&self) -> u32 {
        118
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.prefix.as_bytes());
        let derived = hasher.finalize();

        let secret_key =
            SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();
        let account_id = hash160(&pub_bytes);

        bech32_encode(self.prefix, &account_id)
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.prefix.as_bytes());
        let derived = hasher.finalize();

        Ok(hex::encode(derived))
    }
}

struct GenericSubstrateDerivation {
    coin_type: u32,
    name: &'static str,
    network_id: u8,
    salt: &'static [u8],
}

impl BlockchainDerivation for GenericSubstrateDerivation {
    fn coin_type(&self) -> u32 {
        self.coin_type
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use blake2::Blake2b512;
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.salt);
        let derived = hasher.finalize();

        let mut payload = vec![self.network_id];
        payload.extend_from_slice(&derived);

        let mut hasher = Blake2b512::new();
        hasher.update(b"SS58PRE");
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        payload.extend_from_slice(&checksum_hash[0..2]);

        Ok(bs58::encode(payload).into_string())
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.salt);
        let derived = hasher.finalize();

        Ok(hex::encode(derived))
    }
}

struct MonacoinDerivation;

impl BlockchainDerivation for MonacoinDerivation {
    fn coin_type(&self) -> u32 {
        22
    }

    fn name(&self) -> &'static str {
        "Monacoin"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        let litecoin_address = Litecoin.derive_address(seed, index)?;
        let body = base58check_decode(&litecoin_address, bs58::Alphabet::DEFAULT)?;

        if body.len() < 2 {
            return Err("Litecoin payload is too short to convert to Monacoin".to_string());
        }

        Ok(base58check_encode(&[0x32], &body[1..]))
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        Litecoin.derive_private_key(seed, index)
    }
}

/// Main entry point for address derivation
/// Routes to the appropriate blockchain implementation based on network
pub async fn derive_address(
    seed_phrase: &str,
    ticker: &str,
    network: &str,
    index: u32,
) -> Result<String, String> {
    let chain = select_derivation(ticker, network)?;
    chain.derive_address(seed_phrase, index)
}

/// Resolve the persisted coin type for a ticker/network pair.
/// This must stay aligned with `derive_address` so payout routing
/// follows the same blockchain family as address generation.
pub fn resolve_coin_type(ticker: &str, network: &str) -> Result<u32, String> {
    let chain = select_derivation(ticker, network)?;
    Ok(chain.coin_type())
}

fn boxed<T: BlockchainDerivation + 'static>(chain: T) -> Box<dyn BlockchainDerivation> {
    Box::new(chain)
}

fn cosmos(prefix: &'static str, name: &'static str) -> Box<dyn BlockchainDerivation> {
    Box::new(GenericCosmosDerivation { prefix, name })
}

fn substrate(
    coin_type: u32,
    name: &'static str,
    network_id: u8,
    salt: &'static [u8],
) -> Box<dyn BlockchainDerivation> {
    Box::new(GenericSubstrateDerivation {
        coin_type,
        name,
        network_id,
        salt,
    })
}

fn select_mainnet_derivation(ticker_lower: &str) -> Box<dyn BlockchainDerivation> {
    match mainnet_family(ticker_lower) {
        MainnetFamily::Monero => boxed(MoneroDerivation),
        MainnetFamily::Bitcoin => boxed(Bitcoin),
        MainnetFamily::Litecoin => boxed(Litecoin),
        MainnetFamily::Dogecoin => boxed(Dogecoin),
        MainnetFamily::BitcoinCash => boxed(BitcoinCash),
        MainnetFamily::BitcoinSv => boxed(BitcoinSvDerivation),
        MainnetFamily::Dash => boxed(DashDerivation),
        MainnetFamily::Zcash => boxed(ZcashDerivation),
        MainnetFamily::Ravencoin => boxed(RavencoinDerivation),
        MainnetFamily::Bitcoinz => boxed(BitcoinzDerivation),
        MainnetFamily::Monacoin => boxed(MonacoinDerivation),
        MainnetFamily::BitcoinLike => boxed(Bitcoin),
        MainnetFamily::Solana => boxed(Solana),
        MainnetFamily::Algorand => boxed(Algorand),
        MainnetFamily::Near => boxed(Near),
        MainnetFamily::Cardano => boxed(CardanoDerivation),
        MainnetFamily::Polkadot => boxed(PolkadotDerivation),
        MainnetFamily::Kusama => boxed(KusamaDerivation),
        MainnetFamily::Acala => substrate(354, "Acala", 10, b"acala"),
        MainnetFamily::Astar => substrate(354, "Astar", 11, b"astar"),
        MainnetFamily::Shiden => substrate(354, "Shiden", 12, b"shiden"),
        MainnetFamily::Ripple => boxed(XrpDerivation),
        MainnetFamily::Tron => boxed(TronDerivation),
        MainnetFamily::Stellar => boxed(StellarDerivation),
        MainnetFamily::Sui => boxed(SuiDerivation),
        MainnetFamily::Aptos => boxed(AptosDerivation),
        MainnetFamily::Multiversx => boxed(MultiversxDerivation),
        MainnetFamily::Eos => boxed(EosDerivation),
        MainnetFamily::Hedera => boxed(HederaDerivation),
        MainnetFamily::Icon => boxed(IconDerivation),
        MainnetFamily::Mina => boxed(MinaDerivation),
        MainnetFamily::Neo3 => boxed(NeoDerivation),
        MainnetFamily::Nimiq => boxed(NimiqDerivation),
        MainnetFamily::Ontology => boxed(OntologyDerivation),
        MainnetFamily::Pocket => boxed(PocketDerivation),
        MainnetFamily::Dock => boxed(DockDerivation),
        MainnetFamily::Defichain => boxed(DefichainDerivation),
        MainnetFamily::Flow => boxed(FlowDerivation),
        MainnetFamily::Stacks => boxed(StacksDerivation),
        MainnetFamily::Starknet => boxed(StarknetDerivation),
        MainnetFamily::Tezos => boxed(TezosDerivation),
        MainnetFamily::Theta => boxed(ThetaDerivation),
        MainnetFamily::Ton => boxed(TonDerivation),
        MainnetFamily::Terra => boxed(TerraDerivation),
        MainnetFamily::Vechain => boxed(VechainDerivation),
        MainnetFamily::Waves => boxed(WavesDerivation),
        MainnetFamily::Zilliqa => boxed(ZilliqaDerivation),
        MainnetFamily::Everscale => boxed(EverscaleDerivation),
        MainnetFamily::Factom => boxed(FactomDerivation),
        MainnetFamily::Flux => boxed(FluxDerivation),
        MainnetFamily::CosmosHub => boxed(CosmosHubDerivation),
        MainnetFamily::Osmosis => boxed(OsmosisDerivation),
        MainnetFamily::Juno => cosmos("juno", "Juno"),
        MainnetFamily::Akash => cosmos("akash", "Akash"),
        MainnetFamily::Injective => cosmos("inj", "Injective"),
        MainnetFamily::Regen => cosmos("regen", "Regen"),
        MainnetFamily::Stargaze => cosmos("stars", "Stargaze"),
        MainnetFamily::Secret => cosmos("secret", "Secret"),
        MainnetFamily::Band => cosmos("band", "Band"),
        MainnetFamily::Ion => cosmos("ion", "Ion"),
        MainnetFamily::GravityBridge => cosmos("gravity", "Gravity Bridge"),
        MainnetFamily::Cronos => cosmos("cro", "Cronos"),
        MainnetFamily::Kava => cosmos("kava", "Kava"),
        MainnetFamily::Agoric => cosmos("agoric", "Agoric"),
        MainnetFamily::Axelar => cosmos("axelar", "Axelar"),
        MainnetFamily::Cheqd => cosmos("cheqd", "cheqd"),
        MainnetFamily::Coreum => cosmos("core", "Coreum"),
        MainnetFamily::Shentu => cosmos("shentu", "Shentu"),
        MainnetFamily::Dydx => cosmos("dydx", "dYdX"),
        MainnetFamily::Dymension => cosmos("dym", "Dymension"),
        MainnetFamily::Fetch => cosmos("fetch", "Fetch"),
        MainnetFamily::Initia => cosmos("init", "Initia"),
        MainnetFamily::Kyve => cosmos("kyve", "KYVE"),
        MainnetFamily::Neutron => cosmos("neutron", "Neutron"),
        MainnetFamily::Oraichain => cosmos("orai", "Oraichain"),
        MainnetFamily::Persistence => cosmos("persistence", "Persistence"),
        MainnetFamily::Sei => cosmos("sei", "Sei"),
        MainnetFamily::Celestia => cosmos("celestia", "Celestia"),
        MainnetFamily::Thorchain => cosmos("thor", "THORChain"),
        MainnetFamily::AlephZero => substrate(643, "Aleph Zero", 42, b"alephzero"),
        MainnetFamily::Avail => substrate(354, "Avail", 42, b"avail"),
        MainnetFamily::Bittensor => substrate(354, "Bittensor", 42, b"bittensor"),
        MainnetFamily::Centrifuge => substrate(354, "Centrifuge", 42, b"centrifuge"),
        MainnetFamily::Karura => substrate(434, "Karura", 8, b"karura"),
        MainnetFamily::Picasso => substrate(354, "Picasso", 42, b"picasso"),
        MainnetFamily::Polkadex => substrate(354, "Polkadex", 42, b"polkadex"),
        MainnetFamily::Polymesh => substrate(595, "Polymesh", 12, b"polymesh"),
        MainnetFamily::Ternoa => substrate(354, "Ternoa", 42, b"ternoa"),
        MainnetFamily::Vara => substrate(354, "Vara", 42, b"vara"),
        MainnetFamily::Evm => boxed(EvmChain::ethereum()),
    }
}

fn select_derivation(ticker: &str, network: &str) -> Result<Box<dyn BlockchainDerivation>, String> {
    let network_lower = network.to_lowercase();
    let ticker_lower = ticker.to_lowercase();

    if network_lower == "mainnet" {
        return Ok(select_mainnet_derivation(&ticker_lower));
    }

    if let Some(chain) = select_network_derivation(&network_lower) {
        return Ok(chain);
    }

    if let Some(chain) = select_ticker_fallback(&ticker_lower) {
        return Ok(chain);
    }

    Err(format!(
        "Blockchain '{}' not yet migrated to modular structure",
        network
    ))
}

fn select_ticker_fallback(ticker_lower: &str) -> Option<Box<dyn BlockchainDerivation>> {
    match ticker_lower {
        "xmr" => Some(boxed(MoneroDerivation)),
        "btc" => Some(boxed(Bitcoin)),
        "dash" => Some(boxed(DashDerivation)),
        "zec" => Some(boxed(ZcashDerivation)),
        "rvn" => Some(boxed(RavencoinDerivation)),
        "mona" => Some(boxed(MonacoinDerivation)),
        "vtc" | "dgb" | "grs" | "nmc" | "via" | "pivx" | "sys" => Some(boxed(Bitcoin)),
        "sol" => Some(boxed(Solana)),
        "algo" => Some(boxed(Algorand)),
        "near" => Some(boxed(Near)),
        "ada" => Some(boxed(CardanoDerivation)),
        "xrp" => Some(boxed(XrpDerivation)),
        "trx" => Some(boxed(TronDerivation)),
        "sui" => Some(boxed(SuiDerivation)),
        _ => None,
    }
}

fn select_network_derivation(network_lower: &str) -> Option<Box<dyn BlockchainDerivation>> {
    match network_lower {
        // ===== BITCOIN FAMILY =====
        "bitcoin" | "btc" => Some(boxed(Bitcoin)),
        "litecoin" | "ltc" => Some(boxed(Litecoin)),
        "dogecoin" | "doge" => Some(boxed(Dogecoin)),
        "bitcoin_cash" | "bch" => Some(boxed(BitcoinCash)),
        "dash" => Some(boxed(DashDerivation)),
        "ravencoin" | "rvn" => Some(boxed(RavencoinDerivation)),
        "zcash" | "zec" => Some(boxed(ZcashDerivation)),
        "brc20" | "bitcoin_brc20" => Some(boxed(Brc20Derivation)),
        "lightning" | "bitcoin_lightning" => Some(boxed(BitcoinLightningDerivation)),
        "bitcoin_sv" | "bsv" | "bchsv" => Some(boxed(BitcoinSvDerivation)),
        "bitcoinz" | "btcz" => Some(boxed(BitcoinzDerivation)),

        // ===== SOLANA =====
        "solana" | "sol" => Some(boxed(Solana)),

        // ===== COSMOS SDK =====
        "cosmos" | "cosmos_hub" => Some(boxed(CosmosHubDerivation)),
        "osmosis" => Some(boxed(OsmosisDerivation)),
        "juno" => Some(cosmos("juno", "Juno")),
        "akash" => Some(cosmos("akash", "Akash")),
        "injective" => Some(cosmos("inj", "Injective")),
        "regen" => Some(cosmos("regen", "Regen")),
        "stargaze" => Some(cosmos("stars", "Stargaze")),
        "secret" => Some(cosmos("secret", "Secret")),
        "band" => Some(cosmos("band", "Band")),
        "ion" => Some(cosmos("ion", "Ion")),
        "gravity" => Some(cosmos("gravity", "Gravity Bridge")),

        // ===== SUBSTRATE =====
        "polkadot" | "dot" => Some(boxed(PolkadotDerivation)),
        "kusama" | "ksm" => Some(boxed(KusamaDerivation)),
        "acala" => Some(substrate(354, "Acala", 10, b"acala")),
        "astar" => Some(substrate(354, "Astar", 11, b"astar")),
        "shiden" => Some(substrate(354, "Shiden", 12, b"shiden")),
        "parallel" => Some(substrate(354, "Parallel", 13, b"parallel")),

        // ===== SPECIAL CHAINS =====
        "cardano" | "ada" => Some(boxed(CardanoDerivation)),
        "monero" | "xmr" => Some(boxed(MoneroDerivation)),
        "neo" => Some(boxed(NeoDerivation)),
        "neo_n2" | "n2" | "n3" | "neo3" => Some(boxed(NeoDerivation)),
        "icon" | "icx" => Some(boxed(IconDerivation)),
        "algorand" | "algo" => Some(boxed(Algorand)),
        "near" => Some(boxed(Near)),
        "tezos" | "xtz" => Some(boxed(TezosDerivation)),
        "ripple" | "xrp" => Some(boxed(XrpDerivation)),
        "stacks" | "stx" => Some(boxed(StacksDerivation)),
        "stellar" | "xlm" => Some(boxed(StellarDerivation)),
        "tron" | "trx" | "trc20" => Some(boxed(TronDerivation)),
        "waves" => Some(boxed(WavesDerivation)),
        "ton" => Some(boxed(TonDerivation)),
        "vechain" | "vet" => Some(boxed(VechainDerivation)),
        "sui" => Some(boxed(SuiDerivation)),
        "eos" => Some(boxed(EosDerivation)),
        "hedera" | "hbar" => Some(boxed(HederaDerivation)),
        "mina" => Some(boxed(MinaDerivation)),
        "aptos" | "apt" => Some(boxed(AptosDerivation)),
        "flow" => Some(boxed(FlowDerivation)),
        "starknet" | "stark" => Some(boxed(StarknetDerivation)),
        "theta" => Some(boxed(ThetaDerivation)),
        "zilliqa" | "zil" => Some(boxed(ZilliqaDerivation)),
        "multiversx" | "egld" => Some(boxed(MultiversxDerivation)),
        "nimiq" | "nim" => Some(boxed(NimiqDerivation)),
        "flux" | "zel" => Some(boxed(FluxDerivation)),
        "ontology" | "ont" => Some(boxed(OntologyDerivation)),
        "pocket" | "pokt" => Some(boxed(PocketDerivation)),
        "omni" => Some(boxed(OmniDerivation)),
        "zano" => Some(boxed(ZanoDerivation)),
        "binance_chain" | "bep2" => Some(boxed(BinanceChainDerivation)),
        "partisia" | "mpc" => Some(boxed(PartisiaDerivation)),
        "dock" => Some(boxed(DockDerivation)),
        "defichain" | "dfi" => Some(boxed(DefichainDerivation)),
        "beam" => Some(boxed(BeamDerivation)),
        "everscale" | "freeton" | "ever" => Some(boxed(EverscaleDerivation)),
        "terra" | "terra_classic" | "luna" | "lunc" => Some(boxed(TerraDerivation)),
        "factom" | "fct" => Some(boxed(FactomDerivation)),
        "avalanche_x" | "avaxx" => Some(boxed(AvalancheXDerivation)),
        "a2z" => Some(boxed(EvmChain::ethereum())),
        "shielded" => Some(boxed(ZcashDerivation)),
        "strk" => Some(boxed(StarknetDerivation)),

        // ===== EVM FAMILY (80+ chains) =====
        // Trocador aliases: ETH, MAINNET, MATIC, AVAXC, FTM, KAI, KAIA, KIP7, KLAY,
        // MANTA, METALL2, SEIEVM, SMARTCHAIN, SYSNEVM, TLOSEVM, HAQQ, HYPEREVM,
        // ISLMEVM, FILEVM, FITFI, FLR, CHZ, CFXCORE, BTT, BERA, OAS, PULSE, RSK,
        // STARK, STRAX, KATANA, LAVA, KLC
        "ethereum" | "eth" | "polygon" | "matic" | "bsc" | "smartchain" | "arbitrum"
        | "optimism" | "erc20" | "bep20" | "base" | "avalanche" | "avaxc" | "fantom" | "ftm"
        | "celo" | "harmony" | "klaytn" | "klay" | "kai" | "kaia" | "kip7" | "metis"
        | "metall2" | "boba" | "gnosis" | "fuse" | "iotex" | "scroll" | "zksync" | "linea"
        | "mantle" | "manta_pacific" | "manta" | "mode" | "blast" | "taiko" | "zora" | "sonic"
        | "moonbeam" | "moonriver" | "aurora" | "evmos" | "kava" | "oasis" | "oasis sapphire"
        | "rootstock" | "rsk" | "syscoin" | "sysnevm" | "telos" | "tlosevm" | "thundercore"
        | "tomochain" | "velas" | "wanchain" | "whitechain" | "x_layer" | "zkfair"
        | "shibarium" | "opbnb" | "fraxtal" | "merlin" | "morph" | "redbelly" | "rei"
        | "step_network" | "fitfi" | "stratis" | "strax" | "cyber" | "endurance" | "hyper_evm"
        | "hyperevm" | "iota_evm" | "islm_evm" | "islmevm" | "haqq" | "okx_chain" | "oasys"
        | "oas" | "peaq" | "pulsechain" | "pulse" | "ronin" | "zeta" | "bitgert" | "botanix"
        | "bttc" | "btt" | "cfx" | "cfxcore" | "chiliz" | "chz" | "conflux_espace" | "core"
        | "filecoin" | "filevm" | "flare" | "flr" | "kcc" | "klc" | "bahamut" | "b2"
        | "berachain" | "bera" | "apechain" | "katana" | "lava" | "sei" | "seievm" | "cronos" => {
            Some(boxed(EvmChain::ethereum()))
        }
        _ => None,
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

pub async fn derive_ripple_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    XrpDerivation.derive_private_key(seed_phrase, index)
}

pub async fn derive_tron_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    TronDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_cosmos_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    CosmosHubDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_stellar_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    StellarDerivation.derive_private_key(seed_phrase, index)
}

pub async fn derive_sui_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    SuiDerivation.derive_address(seed_phrase, index)
}

pub async fn sign_message_with_seed(
    seed_phrase: &str,
    index: u32,
    message: &str,
) -> Result<String, String> {
    use secp256k1::{Message, Secp256k1, SecretKey};
    use sha2::{Digest, Sha256};

    let private_key = derive_evm_key(seed_phrase, index).await?;
    let key_bytes = hex::decode(private_key.trim_start_matches("0x"))
        .map_err(|e| format!("Invalid derived key hex: {}", e))?;
    let secret_key =
        SecretKey::from_slice(&key_bytes).map_err(|e| format!("Invalid secret key: {}", e))?;

    let digest = Sha256::digest(message.as_bytes());
    let message = Message::from_digest_slice(&digest)
        .map_err(|e| format!("Invalid message digest: {}", e))?;

    let secp = Secp256k1::new();
    let signature = secp.sign_ecdsa(&message, &secret_key);

    Ok(hex::encode(signature.serialize_compact()))
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

pub async fn derive_tron_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    TronDerivation.derive_private_key(seed_phrase, index)
}

pub async fn derive_substrate_seed(seed_phrase: &str, index: u32) -> Result<Vec<u8>, String> {
    // Substrate uses seed bytes instead of hex key
    let key_hex = PolkadotDerivation.derive_private_key(seed_phrase, index)?;
    hex::decode(key_hex.trim_start_matches("0x"))
        .map_err(|e| format!("Failed to decode substrate seed: {}", e))
}
