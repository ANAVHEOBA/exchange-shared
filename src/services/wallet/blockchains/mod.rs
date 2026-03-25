pub mod bitcoin;
pub mod cardano;
pub mod cosmos;
pub(crate) mod encoding;
pub mod evm;
pub mod icon;
pub mod monero;
pub mod neo;
pub mod solana;
pub mod special;
pub mod substrate;
/// Blockchain-specific address derivation implementations
///
/// Each blockchain family has its own folder with individual implementations:
/// - bitcoin/: Bitcoin-compatible UTXO chains (BTC, LTC, DOGE, BCH, DASH, RVN, ZEC)
/// - evm/: EVM-compatible chains (Ethereum, Polygon, Arbitrum, etc.)
/// - solana/: Solana blockchain
/// - cosmos/: Cosmos SDK chains (Cosmos Hub, Osmosis)
/// - substrate/: Substrate chains (Polkadot, Kusama)
/// - cardano/: Cardano blockchain
/// - monero/: Monero blockchain
/// - neo/: Neo blockchain
/// - icon/: ICON blockchain
/// - special/: Specialized implementations (Algorand, NEAR, Stellar, Tezos, etc.)
pub mod traits;

// Re-export trait
pub use traits::BlockchainDerivation;

// Re-export implementations
pub use bitcoin::{
    Bitcoin, BitcoinCash, BitcoinLightningDerivation, BitcoinSvDerivation, BitcoinzDerivation,
    Brc20Derivation, DashDerivation, Dogecoin, Litecoin, RavencoinDerivation, ZcashDerivation,
};
pub use cardano::CardanoDerivation;
pub use cosmos::{CosmosHubDerivation, OsmosisDerivation};
pub use evm::EvmChain;
pub use icon::IconDerivation;
pub use monero::MoneroDerivation;
pub use neo::NeoDerivation;
pub use solana::Solana;
pub use special::{
    derive_algorand_key, derive_near_key, Algorand, AptosDerivation, AvalancheXDerivation,
    BeamDerivation, BinanceChainDerivation, DefichainDerivation, DockDerivation, EosDerivation,
    EverscaleDerivation, FactomDerivation, FlowDerivation, FluxDerivation, HederaDerivation,
    MinaDerivation, MultiversxDerivation, Near, NimiqDerivation, OmniDerivation,
    OntologyDerivation, PartisiaDerivation, PocketDerivation, StacksDerivation, StarknetDerivation,
    StellarDerivation, SuiDerivation, TerraDerivation, TezosDerivation, ThetaDerivation,
    TonDerivation, TronDerivation, VechainDerivation, WavesDerivation, XrpDerivation,
    ZanoDerivation, ZilliqaDerivation,
};
pub use substrate::{KusamaDerivation, PolkadotDerivation};
