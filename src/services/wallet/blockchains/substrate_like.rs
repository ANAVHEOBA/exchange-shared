/// Substrate-based blockchain address derivation
/// 
/// This module handles Substrate framework chains that use:
/// - Ed25519 elliptic curve
/// - SS58 address encoding
/// - Custom SS58 prefixes per chain
/// 
/// Covers: Kusama, Acala, Astar, Shiden, Parallel, and 15+ more

use super::derive_substrate_like;

// =============================================================================
// TIER 3 PHASE 1: Substrate-like networks (framework ready)
// =============================================================================

/// Kusama (KSM) - Polkadot's Canary Network
/// Coin type: 2 | SS58 prefix: 2
pub async fn derive_kusama(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 2, index).await
}

/// Acala (ACA) - DeFi Platform on Polkadot
/// Coin type: 313 | SS58 prefix: 10
pub async fn derive_acala(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 10, index).await
}

/// Astar (ASTR) - Smart Contract Platform on Polkadot
/// Coin type: 810 | SS58 prefix: 5
pub async fn derive_astar(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 5, index).await
}

/// Shiden (SDN) - Astar's Canary Network (Kusama)
/// Coin type: 336 | SS58 prefix: 5
pub async fn derive_shiden(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 5, index).await
}

/// Parallel (PARA) - Lending Protocol on Polkadot
/// Coin type: 172 | SS58 prefix: 172
pub async fn derive_parallel(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 172, index).await
}

/// Bifrost (BNC) - Liquid Staking Protocol
/// Coin type: 6 | SS58 prefix: 6
pub async fn derive_bifrost(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 6, index).await
}

/// Clover Finance (CLV) - DeFi Platform
/// Coin type: 9 | SS58 prefix: 9
pub async fn derive_clover_finance(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 9, index).await
}

/// Equilibrium (EQ) - DeFi Protocol
/// Coin type: 67 | SS58 prefix: 67
pub async fn derive_equilibrium(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 67, index).await
}

/// Hydra DX (HDX) - DEX Protocol
/// Coin type: 63 | SS58 prefix: 63
pub async fn derive_hydradx(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 63, index).await
}

/// Khala Network (PHA) - Privacy Protocol
/// Coin type: 30 | SS58 prefix: 30
pub async fn derive_khala(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 30, index).await
}

/// Manta Network (MANTA) - Privacy & Interoperability
/// Coin type: 77 | SS58 prefix: 77
pub async fn derive_manta(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 77, index).await
}

/// Phala Network (PHA) - Confidential Computing
/// Coin type: 30 | SS58 prefix: 30
pub async fn derive_phala(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 30, index).await
}

/// Ternoa (CAPS) - NFT Protocol
/// Coin type: 51 | SS58 prefix: 51
pub async fn derive_ternoa(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_substrate_like(seed_phrase, 51, index).await
}

// =============================================================================
// ALIASES & ALTERNATIVE NAMES
// =============================================================================

/// Alias for derive_kusama
pub async fn derive_ksm(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_kusama(seed_phrase, index).await
}

/// Alias for derive_acala
pub async fn derive_aca(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_acala(seed_phrase, index).await
}

/// Alias for derive_astar
pub async fn derive_astr(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_astar(seed_phrase, index).await
}

/// Alias for derive_shiden
pub async fn derive_sdn(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_shiden(seed_phrase, index).await
}

/// Alias for derive_parallel
pub async fn derive_para(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_parallel(seed_phrase, index).await
}

/// Alias for derive_bifrost
pub async fn derive_bnc(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bifrost(seed_phrase, index).await
}

/// Alias for derive_clover_finance
pub async fn derive_clv(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_clover_finance(seed_phrase, index).await
}
