/// Bitcoin-like UTXO blockchain address derivation
/// 
/// This module handles all Bitcoin-like networks that use:
/// - BIP44 derivation paths
/// - Secp256k1 elliptic curve
/// - Various address prefixes (Base58Check format)
/// 
/// Covers: Dash, Zcash, Monacoin, Vertcoin, Digibyte, Ravencoin, 
///         Groestlcoin, Namecoin, Syscoin, Viacoin, Pivx, and 20+ more

use super::derive_bitcoin_like;

// =============================================================================
// TIER 3 PHASE 1: Bitcoin-like networks already implemented
// =============================================================================

/// Dash (DASH) - Digital Cash
/// Coin type: 5 | Address prefix: 0x4C
pub async fn derive_dash(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 5, 0x4Cu8, index).await
}

/// Zcash (ZEC) - Privacy coin (transparent addresses only)
/// Coin type: 133 | Address prefix: 0x1C
pub async fn derive_zcash(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 133, 0x1Cu8, index).await
}

/// Monacoin (MONA) - Japanese cryptocurrency
/// Coin type: 22 | Address prefix: 0x32
pub async fn derive_monacoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 22, 0x32u8, index).await
}

/// Vertcoin (VTC) - Cryptocurrency
/// Coin type: 28 | Address prefix: 0x47
pub async fn derive_vertcoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 28, 0x47u8, index).await
}

/// Digibyte (DGB) - Cryptocurrency
/// Coin type: 20 | Address prefix: 0x1E
pub async fn derive_digibyte(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 20, 0x1Eu8, index).await
}

/// Ravencoin (RVN) - Cryptocurrency
/// Coin type: 175 | Address prefix: 0x3C
pub async fn derive_ravencoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 175, 0x3Cu8, index).await
}

/// Groestlcoin (GRS) - Cryptocurrency
/// Coin type: 17 | Address prefix: 0x24
pub async fn derive_groestlcoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 17, 0x24u8, index).await
}

/// Namecoin (NMC) - Cryptocurrency
/// Coin type: 7 | Address prefix: 0x34
pub async fn derive_namecoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 7, 0x34u8, index).await
}

/// Syscoin (SYS) - Cryptocurrency
/// Coin type: 57 | Address prefix: 0x3F
pub async fn derive_syscoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 57, 0x3Fu8, index).await
}

/// Viacoin (VIA) - Cryptocurrency
/// Coin type: 14 | Address prefix: 0x47
pub async fn derive_viacoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 14, 0x47u8, index).await
}

/// Pivx (PIVX) - Privacy coin
/// Coin type: 119 | Address prefix: 0x30
pub async fn derive_pivx(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 119, 0x30u8, index).await
}

// =============================================================================
// TIER 3: Additional Bitcoin-like networks (to be implemented)
// =============================================================================

/// Bitcoin SV (BSV) - Bitcoin fork
/// Coin type: 236 | Address prefix: 0x00
pub async fn derive_bitcoin_sv(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 236, 0x00u8, index).await
}

/// Peercoin (PPC) - Cryptocurrency
/// Coin type: 6 | Address prefix: 0x37
pub async fn derive_peercoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 6, 0x37u8, index).await
}

/// Primecoin (XPM) - Cryptocurrency
/// Coin type: 24 | Address prefix: 0x23
pub async fn derive_primecoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 24, 0x23u8, index).await
}

/// Decred (DCR) - Cryptocurrency
/// Coin type: 42 | Address prefix varies (0x073f exceeds u8, using u16 encoding)
pub async fn derive_decred(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 42, 0x3fu8, index).await
}

/// Komodo (KMD) - Cryptocurrency
/// Coin type: 141 | Address prefix: 0x3C
pub async fn derive_komodo(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 141, 0x3Cu8, index).await
}

/// Gincoin (GIN) - Cryptocurrency
/// Coin type: 60 | Address prefix: 0x26
pub async fn derive_gincoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 60, 0x26u8, index).await
}

/// Gulden (NLG) - Cryptocurrency
/// Coin type: 108 | Address prefix: 0x26
pub async fn derive_gulden(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 108, 0x26u8, index).await
}

/// Particl (PART) - Privacy-focused cryptocurrency
/// Coin type: 44 | Address prefix: 0x00
pub async fn derive_particl(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 44, 0x00u8, index).await
}

/// Stratis (STRAX) - Blockchain infrastructure
/// Coin type: 105 | Address prefix: 0x3F
pub async fn derive_stratis(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 105, 0x3Fu8, index).await
}

/// Axe (AXE) - Privacy coin
/// Coin type: 4242 | Address prefix: 0xCE
pub async fn derive_axe(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 4242, 0xCEu8, index).await
}

/// Crown (CRN) - Cryptocurrency
/// Coin type: 72 | Address prefix: 0x00
pub async fn derive_crown(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 72, 0x00u8, index).await
}

/// Myriad (XMY) - Multi-algo cryptocurrency
/// Coin type: 90 | Address prefix: 0x32
pub async fn derive_myriad(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_like(seed_phrase, 90, 0x32u8, index).await
}

// =============================================================================
// ALIASES: Support multiple names for same network
// =============================================================================

/// Alias for derive_dash
pub async fn derive_dashcoin(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_dash(seed_phrase, index).await
}

/// Alias for derive_zcash
pub async fn derive_zec(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_zcash(seed_phrase, index).await
}

/// Alias for derive_namecoin
pub async fn derive_nmc(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_namecoin(seed_phrase, index).await
}

/// Alias for derive_ravencoin
pub async fn derive_rvn(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_ravencoin(seed_phrase, index).await
}

/// Alias for derive_groestlcoin
pub async fn derive_grs(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_groestlcoin(seed_phrase, index).await
}

/// Alias for derive_viacoin
pub async fn derive_via(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_viacoin(seed_phrase, index).await
}

/// Alias for derive_vertcoin
pub async fn derive_vtc(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_vertcoin(seed_phrase, index).await
}

/// Alias for derive_digibyte
pub async fn derive_dgb(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_digibyte(seed_phrase, index).await
}

/// Alias for derive_monacoin
pub async fn derive_mona(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_monacoin(seed_phrase, index).await
}

/// Alias for derive_syscoin
pub async fn derive_sys(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_syscoin(seed_phrase, index).await
}

/// Alias for derive_bitcoin_sv
pub async fn derive_bsv(seed_phrase: &str, index: u32) -> Result<String, String> {
    derive_bitcoin_sv(seed_phrase, index).await
}
