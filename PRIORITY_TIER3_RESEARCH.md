# Priority Tier 3: Remaining 100+ Blockchains - Implementation Research

**Status:** Research Phase  
**Target:** Full research before implementation  
**Total Estimated Time:** 40-60 hours  
**Coverage Impact:** ~5-8% additional trading volume

---

## Overview

Priority Tier 3 consists of all remaining blockchains from Trocador's 129-network support that are not covered by Tier 1 (5 networks) or Tier 2 Phase 1 (3 networks).

**Current Coverage:**
- Tier 1: 5 blockchains (Cardano, Polkadot, Ripple, Tron, Cosmos) ✅ COMPLETE
- Tier 2 Phase 1: 3 blockchains (Litecoin, Dogecoin, Bitcoin Cash) ✅ COMPLETE  
- **Tier 3: ~120 blockchains** ⏳ PLANNED

---

## Tier 3 Blockchain Categories

### A) Standard EVM-Compatible (40+ networks)
These use Ethereum's EVM standard and can mostly reuse EVM derivation with chain ID changes.

| Blockchain | Ticker | Chain ID | Status | Effort | Notes |
|-----------|--------|----------|--------|--------|-------|
| Avalanche C-Chain | AVAX | 43114 | ✅ Implemented | LOW | Uses derive_evm_address |
| Gnosis Chain | GNO | 100 | ✅ Implemented | LOW | Uses derive_evm_address |
| Celo | CELO | 42220 | ✅ Implemented | LOW | Uses derive_evm_address |
| Aurora (NEAR EVM) | AURORA | 1313161554 | ✅ Implemented | LOW | Uses derive_evm_address |
| Moonbeam | GLMR | 1284 | ✅ Implemented | LOW | Uses derive_evm_address |
| Moonriver | MOVR | 1285 | ✅ Implemented | LOW | Uses derive_evm_address |
| Fantom | FTM | 250 | ✅ Implemented | LOW | Uses derive_evm_address |
| Harmony One | ONE | 16666 | ✅ Implemented | LOW | Uses derive_evm_address |
| Klaytn | KLAY | 8217 | ✅ Implemented | LOW | Uses derive_evm_address |
| Metis Andromeda | METIS | 1088 | ✅ Implemented | LOW | Uses derive_evm_address |
| Boba Network | BOBA | 288 | ✅ Implemented | LOW | Uses derive_evm_address |
| Evmos | EVMOS | 9001 | ✅ Implemented | LOW | Uses derive_evm_address |
| Fuse | FUSE | 122 | ✅ Implemented | LOW | Uses derive_evm_address |
| Iotex | IOTX | 4689 | ✅ Implemented | LOW | Uses derive_evm_address |
| REI Network | REI | 55 | ✅ Implemented | LOW | Uses derive_evm_address |
| xDai | XDAI | 100 | ✅ Implemented | LOW | Uses derive_evm_address |
| OKX Chain | OKT | 66 | ✅ Implemented | LOW | Uses derive_evm_address |
| Huobi ECO Chain | HECO | 128 | ✅ Implemented | LOW | Uses derive_evm_address |

**Total:** ~40 EVM-compatible networks
**Implementation:** Most already covered by generic EVM; just need dispatcher entries

---

### B) Bitcoin-Like UTXO (35+ networks)
These use Bitcoin's derivation algorithm with different version bytes.

| Blockchain | Ticker | Coin Type (BIP44) | Prefix Byte | Status | Effort | Notes |
|-----------|--------|------------------|-------------|--------|--------|-------|
| Bitcoin | BTC | 0 | 0x00 | ✅ Done | DONE | Standard |
| Litecoin | LTC | 2 | 0x30 | ✅ Done | DONE | Tier 2 |
| Dogecoin | DOGE | 3 | 0x1E | ✅ Done | DONE | Tier 2 |
| Bitcoin Cash | BCH | 145 | 0x00 (CashAddr) | ✅ Done | DONE | Tier 2 |
| Dash | DASH | 5 | 0x4C | ⏳ PENDING | LOW | BIP44 m/44'/5'/0'/0/[index] |
| Zcash | ZEC | 133 | 0x1C,0x1D | ⏳ PENDING | LOW | BIP44 + shielded addresses |
| Monacoin | MONA | 22 | 0x32 | ⏳ PENDING | LOW | BIP44 m/44'/22'/0'/0/[index] |
| Vertcoin | VTC | 28 | 0x47 | ⏳ PENDING | LOW | BIP44 m/44'/28'/0'/0/[index] |
| Digibyte | DGB | 20 | 0x1E | ⏳ PENDING | LOW | BIP44 m/44'/20'/0'/0/[index] |
| Ravencoin | RVN | 175 | 0x3C | ⏳ PENDING | LOW | BIP44 m/44'/175'/0'/0/[index] |
| Groestlcoin | GRS | 17 | 0x24 | ⏳ PENDING | LOW | BIP44 m/44'/17'/0'/0/[index] |
| Namecoin | NMC | 7 | 0x34 | ⏳ PENDING | LOW | BIP44 m/44'/7'/0'/0/[index] |
| Syscoin | SYS | 57 | 0x3F | ⏳ PENDING | LOW | BIP44 m/44'/57'/0'/0/[index] |
| Viacoin | VIA | 14 | 0x47 | ⏳ PENDING | LOW | BIP44 m/44'/14'/0'/0/[index] |
| Pivx | PIVX | 119 | 0x30 | ⏳ PENDING | LOW | BIP44 m/44'/119'/0'/0/[index] |

**Total:** ~30+ Bitcoin-like networks (reuse existing Bitcoin code with different prefixes)
**Implementation:** Simple - copy Bitcoin logic, change version bytes in dispatcher

---

### C) Substrate-Based (20+ networks)
These use Polkadot's Substrate framework with custom coin types.

| Blockchain | Ticker | Coin Type (BIP44) | Curve | SS58 Prefix | Status | Effort | Notes |
|-----------|--------|------------------|-------|------------|--------|--------|-------|
| Polkadot | DOT | 354 | Ed25519 | 0 | ✅ Done | DONE | Tier 1 |
| Kusama | KSM | 2 | Ed25519 | 2 | ⏳ PENDING | LOW | Same as Polkadot, different prefix |
| Acala | ACA | 313 | Ed25519 | 10 | ⏳ PENDING | MED | Polkadot parachain |
| Moonbeam (Substrate) | GLMR | 1284 | Ed25519 | 1284 | ⏳ PENDING | LOW | Also EVM, but has Substrate support |
| Astar | ASTR | 810 | Ed25519 | 5 | ⏳ PENDING | MED | Polkadot parachain |
| Shiden | SDN | 336 | Ed25519 | 5 | ⏳ PENDING | LOW | Kusama parachain |
| Parallel | PARA | 172 | Ed25519 | 172 | ⏳ PENDING | MED | Polkadot parachain |

**Total:** ~20+ Substrate networks
**Implementation:** Reuse Polkadot logic, change derivation path + SS58 prefix

---

### D) Cosmos Ecosystem (50+ networks)
All use Cosmos SDK with bech32 encoding but different prefixes.

| Blockchain | Ticker | Derivation Path | Prefix | Status | Effort | Notes |
|-----------|--------|-----------------|--------|--------|--------|-------|
| Cosmos | ATOM | m/44'/118'/0'/0/[index] | cosmos | ✅ Done | DONE | Tier 1 |
| Osmosis | OSMO | m/44'/118'/0'/0/[index] | osmo | ⏳ PENDING | LOW | Same derivation, different prefix |
| Juno | JUNO | m/44'/118'/0'/0/[index] | juno | ⏳ PENDING | LOW | Cosmos chain |
| Akash | AKT | m/44'/118'/0'/0/[index] | akash | ⏳ PENDING | LOW | Cosmos chain |
| Regen | REGEN | m/44'/118'/0'/0/[index] | regen | ⏳ PENDING | LOW | Cosmos chain |
| Stargaze | STARS | m/44'/118'/0'/0/[index] | stars | ⏳ PENDING | LOW | Cosmos chain |
| Evmos | EVMOS | m/44'/60'/0'/0/[index] | evmos | ✅ Done | DONE | Cosmos EVM |
| Cronos | CRO | m/44'/60'/0'/0/[index] | cro | ⏳ PENDING | LOW | Cosmos EVM |
| Secret | SCRT | m/44'/529'/0'/0/[index] | secret | ⏳ PENDING | LOW | Custom coin type 529 |
| Injective | INJ | m/44'/60'/0'/0/[index] | inj | ⏳ PENDING | LOW | Cosmos EVM |
| Kava | KAVA | m/44'/459'/0'/0/[index] | kava | ⏳ PENDING | LOW | Cosmos EVM |
| Sei | SEI | m/44'/118'/0'/0/[index] | sei | ⏳ PENDING | LOW | Cosmos chain |
| Band | BAND | m/44'/118'/0'/0/[index] | band | ⏳ PENDING | LOW | Cosmos chain |
| ION | ION | m/44'/118'/0'/0/[index] | ion | ⏳ PENDING | LOW | Cosmos chain |
| Gravity Bridge | GRAV | m/44'/118'/0'/0/[index] | gravity | ⏳ PENDING | LOW | Cosmos chain |

**Total:** ~50+ Cosmos SDK networks
**Implementation:** Reuse Cosmos bech32 logic, change prefix only

---

### E) Other Standards (20+ networks)

#### Solana-Like (SPL Standard)
| Blockchain | Ticker | Derivation | Status | Effort |
|-----------|--------|-----------|--------|--------|
| Solana | SOL | m/44'/501'/0'/0' | ✅ Done | DONE |
| Serum | SRM | m/44'/501'/0'/0' | ⏳ PENDING | LOW |

#### Stellar-Like (StrKey)
| Blockchain | Ticker | Derivation | Status | Effort |
|-----------|--------|-----------|--------|--------|
| Stellar | XLM | Custom Ed25519 + StrKey | ⏳ PENDING | MED |
| StellarCannons | N/A | Stellar fork | ⏳ PENDING | LOW |

#### NEAR-Like
| Blockchain | Ticker | Derivation | Status | Effort |
|-----------|--------|-----------|--------|--------|
| NEAR | NEAR | m/44'/397'/0'/0' + Ed25519 hex | ⏳ PENDING | LOW |
| Aurora | AURORA | EVM (also on NEAR) | ⏳ PENDING | LOW |

#### Monero-Like (Privacy Coins)
| Blockchain | Ticker | Derivation | Status | Effort |
|-----------|--------|-----------|--------|--------|
| Monero | XMR | Custom derivation | ✅ Done | DONE |
| Zcash | ZEC | See Bitcoin-like section | ⏳ PENDING | MED |

#### UTXO Extensions
| Blockchain | Ticker | Derivation | Status | Effort |
|-----------|--------|-----------|--------|--------|
| Ordinals/Bitcoin | ORD | m/44'/0'/0'/0/[index] with taproot | ⏳ PENDING | HIGH |
| Stacks | STX | Bitcoin-based | ⏳ PENDING | MED |

#### Multi-Sig / Special Cases
| Blockchain | Ticker | Notes | Status | Effort |
|-----------|--------|-------|--------|--------|
| Tezos | XTZ | m/44'/1729'/0'/0/[index] + Base58Check | ⏳ PENDING | MED |
| Algorand | ALGO | m/44'/283'/0'/0/[index] + Base32 | ⏳ PENDING | LOW |
| Waves | WAVES | Custom | ⏳ PENDING | MED |
| EOS | EOS | Requires account creation | ❌ SKIP | HIGH |

---

## Implementation Strategy: Optimal Grouping

### Phase 1: Quick Wins (Low-Hanging Fruit) - 15-20 hours
These require minimal new code - mostly dispatcher entries.

**1. Generic Bitcoin-Like Loop (8-10 hours)**
```rust
fn derive_bitcoin_like_address(
    seed_phrase: &str,
    coin_type: u32,        // e.g., 5 for Dash, 133 for Zcash
    prefix_byte: u8,       // e.g., 0x4C for Dash
    index: u32,
) -> Result<String, String> {
    // Reuse existing Bitcoin logic
    // Only change coin_type and prefix_byte
}
```
Covers: Dash, Zcash, Monacoin, Vertcoin, Digibyte, Ravencoin, Groestlcoin, Namecoin, Syscoin, Viacoin, Pivx (~30+ networks)

**2. Generic Cosmos-Like Loop (5-8 hours)**
```rust
fn derive_cosmos_like_address(
    seed_phrase: &str,
    coin_type: u32,        // e.g., 118 for Osmosis, 529 for Secret
    hrp_prefix: &str,      // e.g., "osmo" for Osmosis
    index: u32,
) -> Result<String, String> {
    // Reuse existing Cosmos logic
    // Only change coin_type and prefix
}
```
Covers: Osmosis, Juno, Akash, Regen, Stargaze, Cronos, Injective, Kava, Sei, Band, ION, Gravity Bridge (~50+ networks)

**3. Generic Substrate-Like Loop (3-4 hours)**
```rust
fn derive_substrate_like_address(
    seed_phrase: &str,
    ss58_prefix: u8,       // e.g., 2 for Kusama
    index: u32,
) -> Result<String, String> {
    // Reuse existing Polkadot logic
    // Only change SS58 prefix
}
```
Covers: Kusama, Acala, Astar, Shiden, Parallel (~20+ networks)

**Phase 1 Effort:** 15-20 hours  
**Networks Covered:** ~100+ blockchains  
**Additional Volume Coverage:** ~4-5%

---

### Phase 2: Mid-Effort Chains (10-15 hours)
Special address formats requiring custom logic but still standardized.

| Blockchain | Ticker | Effort | Why Special | Implementation |
|-----------|--------|--------|-------------|-----------------|
| Tezos | XTZ | MED (3-4 hrs) | tz1/tz2/tz3 prefixes, custom encoding | Custom Base58Check with Tezos alphabet |
| Algorand | ALGO | LOW (2-3 hrs) | Base32 encoding | Reuse bech32 logic, Base32 alphabet |
| Stellar | XLM | MED (3-4 hrs) | StrKey encoding, custom format | Custom encoder for S/G/etc prefixes |
| Waves | WAVES | MED (3-4 hrs) | Custom address format | SHA256 + Base58Check + version 1 |
| Stacks | STX | MED (3-4 hrs) | Bitcoin-derived + version byte | Extend Bitcoin derivation |

**Phase 2 Effort:** 10-15 hours  
**Networks Covered:** ~10 additional blockchains  
**Additional Volume Coverage:** ~1-2%

---

### Phase 3: High-Complexity Chains (15-25 hours)
These require novel implementations.

| Blockchain | Ticker | Effort | Why Complex | Implementation |
|-----------|--------|--------|------------|-----------------|
| Ordinals | ORD | HIGH (5-8 hrs) | Taproot addresses, BIP340 | Requires Schnorr signatures + taproot |
| Zcash | ZEC | HIGH (4-6 hrs) | Shielded + transparent, multiple formats | Support transparent only, skip shielded |
| EOS | EOS | SKIP | Account names, not key-based | Not suitable for auto-generation |
| TON | TON | HIGH (4-5 hrs) | Workchain + address encoding | Custom workchain derivation |
| ICP | ICP | HIGH (4-6 hrs) | Principal IDs, complex derivation | Principal ID encoding |
| Aptos | APT | MED (3-4 hrs) | Custom single address from seed | Use first public key directly |
| Sui | SUI | ✅ Done | Already implemented | DONE |

**Phase 3 Effort:** 15-25 hours  
**Networks Covered:** ~10 additional blockchains  
**Additional Volume Coverage:** ~0.5-1%

---

## Dependency Analysis

### Required Crates (Already Have)
- ✅ `bip39` - Seed phrase handling
- ✅ `coins_bip32` - BIP32 derivation
- ✅ `secp256k1` - ECDSA keys
- ✅ `ed25519-dalek` - EdDSA keys
- ✅ `sha2` - SHA256/SHA512
- ✅ `ripemd` - RIPEMD160
- ✅ `blake2` - BLAKE2 hash
- ✅ `bech32` - Bech32 encoding
- ✅ `bs58` - Base58 encoding

### New Crates Needed
- `blake3` - BLAKE3 hash (for NEAR/Aptos)
- `hex` - Hex encoding/decoding
- `base64` - Base64 encoding (some chains)
- `starknet-crypto` - For Starknet if added

---

## BIP44 Coin Types Reference

| Coin | Coin Type | Path |
|------|-----------|------|
| Bitcoin | 0 | m/44'/0'/0'/0/[index] |
| Bitcoin Testnet | 1 | m/44'/1'/0'/0/[index] |
| Litecoin | 2 | m/44'/2'/0'/0/[index] |
| Dogecoin | 3 | m/44'/3'/0'/0/[index] |
| Dash | 5 | m/44'/5'/0'/0/[index] |
| Namecoin | 7 | m/44'/7'/0'/0/[index] |
| Viacoin | 14 | m/44'/14'/0'/0/[index] |
| Digibyte | 20 | m/44'/20'/0'/0/[index] |
| Monacoin | 22 | m/44'/22'/0'/0/[index] |
| Vertcoin | 28 | m/44'/28'/0'/0/[index] |
| Groestlcoin | 17 | m/44'/17'/0'/0/[index] |
| Syscoin | 57 | m/44'/57'/0'/0/[index] |
| Bitcoin Cash | 145 | m/44'/145'/0'/0/[index] |
| Zcash | 133 | m/44'/133'/0'/0/[index] |
| Ravencoin | 175 | m/44'/175'/0'/0/[index] |
| Pivx | 119 | m/44'/119'/0'/0/[index] |
| Tezos | 1729 | m/44'/1729'/0'/0/[index] |
| Algorand | 283 | m/44'/283'/0'/0/[index] |
| Solana | 501 | m/44'/501'/0'/0' (no index) |
| Cosmos | 118 | m/44'/118'/0'/0/[index] |
| Polkadot | 354 | m/44'/354'/0'/0/[index] |
| Kusama | 2 | Substrate (not BIP44) |
| Avalanche | 60 | m/44'/60'/0'/0/[index] (EVM) |
| Near | 397 | m/44'/397'/0'/0/[index] |
| Aptos | 637 | m/44'/637'/0'/0/[index] |
| Secret Network | 529 | m/44'/529'/0'/0/[index] |

---

## Testing Strategy

### Unit Tests Required
1. **Format validation** - Address starts with correct prefix
2. **Determinism** - Same seed + index = same address
3. **Uniqueness** - Different indices = different addresses
4. **Invalid input** - Bad seeds are rejected
5. **Performance** - 100 addresses in <5 seconds
6. **Dispatcher** - All aliases route correctly

### Integration Tests
1. **Swap integration** - Generated address can receive swap proceeds
2. **Cross-blockchain** - No duplicate addresses across chains
3. **High volume** - Stress test with 10,000 address generations

### Compatibility Tests
1. **Official test vectors** - Verify against blockchain specification examples
2. **Third-party wallets** - Import generated addresses into MetaMask, Ledger, etc.
3. **Block explorers** - Verify addresses are recognized by explorers

---

## Known Limitations & Workarounds

### Cannot Implement
- **EOS** - Requires account name registration, not suitable for auto-generation
- **Filecoin** - Complex address format with custom encoding, low volume
- **Cosmos (IBC)** - Cross-chain addresses, not applicable

### Fallback Strategy
For any chain that cannot be auto-generated:
1. Accept it as swap source (user sends FROM it)
2. Convert destination swaps to USDC on Polygon
3. Show user: "Destination not directly supported, converting to USDC"

---

## Implementation Phases Timeline

| Phase | Networks | Time | Completion | Volume |
|-------|----------|------|------------|--------|
| Tier 1 | 5 | ✅ Done | March 2026 | 85% |
| Tier 2 Phase 1 | 3 | ✅ Done | March 2026 | 87% |
| Tier 3 Phase 1 | 100+ | 15-20 hrs | ~3-4 days | 91-92% |
| Tier 3 Phase 2 | 10 | 10-15 hrs | ~2-3 days | 92-93% |
| Tier 3 Phase 3 | 10 | 15-25 hrs | ~3-5 days | 93-95% |
| **Total** | **128+** | **40-60 hrs** | **2 weeks** | **93-95%** |

---

## Files to Create/Modify

### New Dispatcher Functions
```rust
// Generic Bitcoin-like (covers 30+ networks)
async fn derive_bitcoin_like_address(seed, coin_type, prefix, index) -> Result<String>

// Generic Cosmos-like (covers 50+ networks)
async fn derive_cosmos_like_address(seed, coin_type, hrp, index) -> Result<String>

// Generic Substrate-like (covers 20+ networks)
async fn derive_substrate_like_address(seed, ss58_prefix, index) -> Result<String>

// Special implementations
async fn derive_tezos_address(seed, index) -> Result<String>
async fn derive_algorand_address(seed, index) -> Result<String>
async fn derive_stellar_address(seed, index) -> Result<String>
async fn derive_waves_address(seed, index) -> Result<String>
```

### Updated Files
- `src/services/wallet/derivation.rs` - Add ~1500 lines of new code
- `src/services/wallet/dispatcher.rs` - Update dispatcher for 100+ networks
- `tests/wallet/tier3_phase{1,2,3}_test.rs` - Add test suites

---

## Success Criteria

- ✅ Support 128+ blockchains (99% of Trocador's offerings)
- ✅ All tests passing (format, determinism, uniqueness, integration)
- ✅ Performance <5 seconds for 100 addresses
- ✅ No duplicate addresses across chains
- ✅ Verified against official blockchain documentation
- ✅ Integration tests with real swap flows
- ✅ Production deployment verified

---

**Last Updated:** March 1, 2026  
**Research Status:** ✅ COMPLETE  
**Ready for Implementation:** ✅ YES (Phase 1 immediate, Phase 2-3 after Phase 1 verification)

---

## Quick Reference: Generic Implementations

### Bitcoin-Like Template
```rust
pub async fn derive_bitcoin_like_address(
    seed_phrase: &str,
    coin_type: u32,
    prefix_byte: u8,
    index: u32,
) -> Result<String, String> {
    let path = format!("m/44'/{coin_type}'/0'/0/{index}");
    let key = derive_with_path(seed_phrase, &path)?;
    let pub_key = extract_public_key(key)?;
    let hash = ripemd160(sha256(pub_key));
    let mut payload = vec![prefix_byte];
    payload.extend_from_slice(&hash);
    let checksum = double_sha256(&payload)[..4].to_vec();
    payload.extend_from_slice(&checksum);
    Ok(bs58::encode(&payload).into_string())
}
```

### Cosmos-Like Template
```rust
pub async fn derive_cosmos_like_address(
    seed_phrase: &str,
    coin_type: u32,
    hrp_prefix: &str,
    index: u32,
) -> Result<String, String> {
    let path = format!("m/44'/{coin_type}'/0'/0/{index}");
    let key = derive_with_path(seed_phrase, &path)?;
    let pub_key = extract_public_key(key)?;
    let hash = ripemd160(sha256(pub_key));
    let hrp = Hrp::parse(hrp_prefix)?;
    bech32::encode(hrp, &hash)
}
```

### Substrate-Like Template
```rust
pub async fn derive_substrate_like_address(
    seed_phrase: &str,
    ss58_prefix: u8,
    index: u32,
) -> Result<String, String> {
    let path = format!("m/44'/354'/0'/0/{index}"); // Polkadot base
    let key = derive_ed25519_with_path(seed_phrase, &path)?;
    let pub_key = extract_ed25519_public_key(key)?;
    encode_ss58(&pub_key, ss58_prefix)
}
```


  The 80 remaining break down as:

   - ~20 Bitcoin-like (framework ready)
   - ~35 Cosmos-like (framework ready)
   - ~15 Substrate-like (framework ready)
   - ~10 EVM-compatible (framework ready)
   - ~5 Other/special (framework ready)