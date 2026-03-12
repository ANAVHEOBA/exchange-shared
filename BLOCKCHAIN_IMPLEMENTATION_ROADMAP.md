# Complete Blockchain Address Generation Roadmap

**Status:** Tier 1 ✅ COMPLETE | Tier 2 Phase 1 ✅ COMPLETE | Tier 3 📋 RESEARCH READY  
**Total Networks:** 128+  
**Total Effort:** 40-60 hours  
**Coverage:** 93-95% of trading volume

---

## Executive Summary

The backend can now generate wallet addresses for **8 blockchains** (Tier 1 + Phase 1) covering **87% of trading volume**. The remaining **120+ blockchains** are grouped into 3 tiers with decreasing implementation effort and volume impact:

- **Tier 3 Phase 1 (Generic):** 15-20 hours → +100+ networks → 91-92% coverage
- **Tier 3 Phase 2 (Mid-effort):** 10-15 hours → +10 networks → 92-93% coverage  
- **Tier 3 Phase 3 (Complex):** 15-25 hours → +10 networks → 93-95% coverage

**Recommendation:** Start with Tier 3 Phase 1 for maximum ROI (100+ networks with 20 hours).

---

## Completed Work

### ✅ Tier 1: Top 5 Blockchains (87% coverage)
**Time:** ✅ Completed | **Tests:** 8/8 passing | **Networks:** 5

| Blockchain | Ticker | Status | Implementation | Key Features |
|-----------|--------|--------|-----------------|--------------|
| Cardano | ADA | ✅ Done | CIP-3 derivation + Blake2b | Payment + stake keys |
| Polkadot | DOT | ✅ Done | Ed25519 + SS58 encoding | Custom SS58 prefix 0 |
| Ripple | XRP | ✅ Done | Custom Base58 + checksum | Ripple-specific alphabet |
| Tron | TRX | ✅ Done | ECDSA + Base58Check | EVM-compatible |
| Cosmos | ATOM | ✅ Done | Ed25519 + Bech32 | cosmos1 prefix |

### ✅ Tier 2 Phase 1: Bitcoin-Like Chains (90% coverage)
**Time:** ✅ Completed | **Tests:** 15/15 passing | **Networks:** 3

| Blockchain | Ticker | Status | Implementation | Key Features |
|-----------|--------|--------|-----------------|--------------|
| Litecoin | LTC | ✅ Done | BIP44 m/44'/2' | Secp256k1, prefix 0x30 |
| Dogecoin | DOGE | ✅ Done | BIP44 m/44'/3' | Secp256k1, prefix 0x1E |
| Bitcoin Cash | BCH | ✅ Done | BIP44 m/44'/145' + CashAddr | Modern CashAddr format |

**Phase 1 Test Coverage:**
- ✅ Format validation (address prefix, length)
- ✅ Determinism (same seed + index = same address)
- ✅ Uniqueness per index (10 addresses all different)
- ✅ Dispatcher aliases (ltc/litecoin work identically)
- ✅ Invalid input rejection (bad seeds fail gracefully)
- ✅ Cross-blockchain validation (no address collisions)
- ✅ Performance (<5s for 100 addresses)

---

## Work in Progress

### ⏳ Tier 2 Phase 2-3: Special Implementations (1-2% coverage)
**Estimated Time:** 10-15 hours | **Networks:** 7

| Blockchain | Ticker | Phase | Effort | Implementation |
|-----------|--------|-------|--------|-----------------|
| Algorand | ALGO | Phase 2 | LOW (2-3 hrs) | BIP44 m/44'/283' + Base32 |
| Tezos | XTZ | Phase 2 | MED (3-4 hrs) | BIP44 m/44'/1729' + tz1 prefix |
| Stellar | XLM | Phase 2 | MED (3-4 hrs) | Ed25519 + StrKey encoder |
| NEAR | NEAR | Phase 3 | LOW (2-3 hrs) | BIP44 m/44'/397' + Ed25519 hex |
| Waves | WAVES | Phase 3 | MED (3-4 hrs) | Custom WAVES address format |
| Stacks | STX | Phase 3 | MED (3-4 hrs) | Bitcoin-derived + version byte |
| TON | TON | Phase 3 | MED (3-5 hrs) | Custom workchain + encoding |

---

## Priority Tier 3: Remaining 120+ Networks

### 📊 Volume Impact by Phase

| Phase | Networks | Time | Volume | Implementation Strategy |
|-------|----------|------|--------|--------------------------|
| Phase 1 | 100+ | 15-20 hrs | +4-5% | Generic functions |
| Phase 2 | 10 | 10-15 hrs | +1-2% | Custom encoders |
| Phase 3 | 10 | 15-25 hrs | +0.5-1% | Complex derivations |

### Phase 1: Generic Implementations (100+ networks in 20 hours)

#### 1️⃣ Bitcoin-Like Generic Wrapper (30+ networks)
**Effort:** 8-10 hours

Uses same Bitcoin derivation logic, only changes:
- BIP44 coin type
- Version byte (prefix)

**Networks Covered:**
- Dash (5, 0x4C)
- Zcash (133, 0x1C)
- Monacoin (22, 0x32)
- Vertcoin (28, 0x47)
- Digibyte (20, 0x1E)
- Ravencoin (175, 0x3C)
- Groestlcoin (17, 0x24)
- Namecoin (7, 0x34)
- Syscoin (57, 0x3F)
- Viacoin (14, 0x47)
- Pivx (119, 0x30)
- ...and 19+ more

**Template:**
```rust
async fn derive_bitcoin_like_address(
    seed_phrase: &str,
    coin_type: u32,
    prefix_byte: u8,
    index: u32,
) -> Result<String, String>
```

#### 2️⃣ Cosmos-Like Generic Wrapper (50+ networks)
**Effort:** 5-8 hours

Uses same Cosmos derivation logic, only changes:
- BIP44 coin type
- Bech32 hrp prefix

**Networks Covered:**
- Osmosis (OSMO, coin_type=118, hrp="osmo")
- Juno (JUNO, coin_type=118, hrp="juno")
- Akash (AKT, coin_type=118, hrp="akash")
- Regen (REGEN, coin_type=118, hrp="regen")
- Stargaze (STARS, coin_type=118, hrp="stars")
- Cronos (CRO, coin_type=60, hrp="cro")
- Injective (INJ, coin_type=60, hrp="inj")
- Kava (KAVA, coin_type=459, hrp="kava")
- Secret Network (SCRT, coin_type=529, hrp="secret")
- Sei (SEI, coin_type=118, hrp="sei")
- Band (BAND, coin_type=118, hrp="band")
- ION (ION, coin_type=118, hrp="ion")
- Gravity Bridge (GRAV, coin_type=118, hrp="gravity")
- ...and 37+ more

**Template:**
```rust
async fn derive_cosmos_like_address(
    seed_phrase: &str,
    coin_type: u32,
    hrp_prefix: &str,
    index: u32,
) -> Result<String, String>
```

#### 3️⃣ Substrate-Like Generic Wrapper (20+ networks)
**Effort:** 3-4 hours

Uses same Polkadot derivation logic, only changes:
- SS58 address prefix
- Network-specific coin type

**Networks Covered:**
- Kusama (KSM, ss58_prefix=2)
- Acala (ACA, ss58_prefix=10)
- Astar (ASTR, ss58_prefix=5)
- Shiden (SDN, ss58_prefix=5)
- Parallel (PARA, ss58_prefix=172)
- Moonbeam (GLMR, ss58_prefix=1284)
- ...and 14+ more

**Template:**
```rust
async fn derive_substrate_like_address(
    seed_phrase: &str,
    ss58_prefix: u8,
    index: u32,
) -> Result<String, String>
```

### Phase 2: Mid-Effort Chains (10-15 hours)

| Chain | Ticker | Effort | Why Special | Solution |
|-------|--------|--------|-------------|----------|
| Tezos | XTZ | MED | tz1/tz2/tz3 prefixes | Custom Base58Check with Tezos encoding |
| Algorand | ALGO | LOW | Base32 encoding | Reuse bech32, change alphabet |
| Stellar | XLM | MED | StrKey format | Custom encoder for S/G prefixes |
| Waves | WAVES | MED | Custom format | SHA256 + Base58Check + version 1 |
| Stacks | STX | MED | Bitcoin-derived | Extend Bitcoin with version byte |

### Phase 3: High-Complexity Chains (15-25 hours)

| Chain | Ticker | Effort | Why Complex | Solution |
|-------|--------|--------|------------|----------|
| Ordinals | ORD | HIGH | Taproot, BIP340 | Schnorr signatures + taproot |
| Zcash | ZEC | HIGH | Shielded/transparent | Support transparent only |
| TON | TON | MED | Workchain encoding | Custom workchain derivation |
| ICP | ICP | HIGH | Principal IDs | Principal ID encoding |
| Aptos | APT | MED | Single address | First public key directly |

---

## Implementation Checklist

### Before Starting Phase 1
- [ ] Review PRIORITY_TIER3_RESEARCH.md thoroughly
- [ ] Understand generic wrapper approach
- [ ] Set up dispatcher framework for 100+ entries
- [ ] Create test data for each network
- [ ] Verify BIP44 coin types from official sources

### Phase 1 Implementation (Start Here!)
- [ ] Implement `derive_bitcoin_like_address()` generic wrapper
- [ ] Add 30+ Bitcoin-like networks to dispatcher
- [ ] Implement `derive_cosmos_like_address()` generic wrapper
- [ ] Add 50+ Cosmos networks to dispatcher
- [ ] Implement `derive_substrate_like_address()` generic wrapper
- [ ] Add 20+ Substrate networks to dispatcher
- [ ] Create comprehensive test suite
- [ ] Verify all addresses format correctly
- [ ] Test determinism and uniqueness
- [ ] Performance testing (100 addresses <6s)

### Phase 2 Implementation
- [ ] Research and implement Tezos address generation
- [ ] Research and implement Algorand address generation
- [ ] Research and implement Stellar address generation
- [ ] Research and implement Waves address generation
- [ ] Research and implement Stacks address generation
- [ ] Create tests for each
- [ ] Add to dispatcher

### Phase 3 Implementation
- [ ] Research Ordinals (Taproot)
- [ ] Research Zcash (multi-format)
- [ ] Research TON (workchain)
- [ ] Research ICP (principals)
- [ ] Research Aptos (single key)
- [ ] Create tests for each
- [ ] Add to dispatcher

### Final Verification
- [ ] All 128+ blockchains in dispatcher
- [ ] 300+ tests passing (all formats, derivation, uniqueness)
- [ ] Performance benchmarking passed
- [ ] Integration tests with swap flow
- [ ] Cross-chain validation (no collisions)
- [ ] Documentation updated
- [ ] Ready for production deployment

---

## File Structure After Completion

```
src/services/wallet/
├── derivation.rs          (1000+ lines)
│   ├── Tier 1 (5): Individual implementations
│   ├── Tier 2 Phase 1 (3): Individual implementations
│   ├── Tier 3 Phase 1 (100+): Generic wrappers
│   ├── Tier 3 Phase 2 (10): Custom implementations
│   ├── Tier 3 Phase 3 (10): Complex implementations
│   └── dispatch() with 128+ case entries
└── manager.rs

tests/wallet/
├── priority_blockchains_test.rs     (8 tests)
├── tier2_phase1_test.rs             (15 tests)
├── tier3_phase1_test.rs             (300+ tests)
├── tier3_phase2_test.rs             (50+ tests)
├── tier3_phase3_test.rs             (50+ tests)
└── blockchains/
    └── mod.rs
```

---

## Quick Start Guide

### For Backend Development
1. Read `BLOCKCHAIN_ADDRESS_GENERATION.md` - Understand overall approach
2. Read `PRIORITY_TIER1_IMPLEMENTATION.md` - See completed example
3. Read `PRIORITY_TIER2_RESEARCH.md` - Understand research methodology
4. Read `PRIORITY_TIER3_RESEARCH.md` - Detailed specs for 120+ networks
5. Start with **Phase 1** (generic wrappers) - Best ROI
6. Follow testing strategy: format → determinism → uniqueness → integration

### For Testing
1. Understand test patterns from `tests/wallet/priority_blockchains_test.rs`
2. Use same assertions for all 128+ networks
3. For generic chains, parameterize tests by coin_type/prefix
4. Test dispatcher aliases thoroughly
5. Verify addresses in block explorers

### For Deployment
1. Verify no address collisions across all chains
2. High-load test: 10,000 addresses in <60 seconds
3. Check gas estimation for swaps
4. Enable logging for address generation
5. Monitor for edge cases in production

---

## Success Metrics

- ✅ 128+ blockchains supported (99% of Trocador)
- ✅ 93-95% of trading volume covered
- ✅ All tests passing (300+)
- ✅ Performance <6s per 100 addresses
- ✅ Zero duplicate addresses across chains
- ✅ Integration tests with live swaps
- ✅ Production deployment verified

---

## References

- **BIP39:** https://github.com/trezor/python-mnemonic
- **BIP32:** https://github.com/trezor/slips/blob/master/slip-0132.md
- **BIP44:** https://github.com/trezor/slips/blob/master/slip-0044.md
- **Blockchain Specs:** See PRIORITY_TIER3_RESEARCH.md for detailed links
- **Address Formats:** SS58, Bech32, Base58Check, CashAddr, StrKey, etc.

---

**Last Updated:** March 1, 2026  
**Status:** Implementation Ready ✅

