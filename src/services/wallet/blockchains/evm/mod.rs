pub mod base;

pub use base::EvmChain;

// All EVM chains use the same derivation logic (coin_type 60)
// Individual chain files only needed if chain-specific logic required
