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
pub mod bitcoin;
pub mod evm;
pub mod solana;
pub mod cosmos;
pub mod substrate;
pub mod cardano;
pub mod monero;
pub mod neo;
pub mod icon;
pub mod special;

// Re-export trait
pub use traits::BlockchainDerivation;

// Re-export implementations
pub use bitcoin::{Bitcoin, Litecoin, Dogecoin, BitcoinCash, DashDerivation, RavencoinDerivation, ZcashDerivation, 
                  Brc20Derivation, BitcoinLightningDerivation, BitcoinSvDerivation, BitcoinzDerivation};
pub use evm::EvmChain;
pub use solana::Solana;
pub use cosmos::{CosmosHubDerivation, OsmosisDerivation};
pub use substrate::{PolkadotDerivation, KusamaDerivation};
pub use cardano::CardanoDerivation;
pub use monero::MoneroDerivation;
pub use neo::NeoDerivation;
pub use icon::IconDerivation;
pub use special::{
    Algorand, Near,
    TezosDerivation, XrpDerivation, StacksDerivation, StellarDerivation,
    TronDerivation, WavesDerivation, TonDerivation, VechainDerivation,
    SuiDerivation, EosDerivation, HederaDerivation, MinaDerivation,
    AptosDerivation, FlowDerivation, StarknetDerivation, ThetaDerivation,
    ZilliqaDerivation, MultiversxDerivation,
    NimiqDerivation, FluxDerivation, OntologyDerivation, PocketDerivation,
    OmniDerivation, ZanoDerivation, BinanceChainDerivation, PartisiaDerivation,
    DockDerivation, DefichainDerivation, BeamDerivation, EverscaleDerivation,
    TerraDerivation, FactomDerivation, AvalancheXDerivation,
    derive_algorand_key, derive_near_key
};
