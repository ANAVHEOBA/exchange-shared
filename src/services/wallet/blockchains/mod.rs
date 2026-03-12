/// Blockchain-specific address derivation implementations
/// 
/// This module organizes address derivation by blockchain type:
/// - bitcoin_like: Bitcoin-compatible UTXO chains (Dash, Zcash, etc.)
/// - cosmos_like: Cosmos SDK chains (Osmosis, Juno, etc.)
/// - substrate_like: Substrate-based chains (Kusama, Acala, etc.)
/// - evm_compatible: EVM-compatible chains (Avalanche, Fantom, etc.)
/// - special: Specialized implementations (Monero, privacy coins, etc.)

pub mod bitcoin_like;
pub mod cosmos_like;
pub mod substrate_like;
pub mod evm_compatible;
pub mod special;

// Re-export generic functions from parent module
pub use crate::services::wallet::derivation::{
    derive_bitcoin_like_address as derive_bitcoin_like,
    derive_cosmos_like_address as derive_cosmos_like,
    derive_substrate_like_address as derive_substrate_like,
    derive_evm_address as derive_evm_compatible,
};
