# Tier 3 Implementation Quick Reference

**Use this as a checklist while implementing.**

---

## Phase 1: Generic Implementations (Start Here!)

### 1. Bitcoin-Like Wrapper (30+ networks)

**File Location:** `src/services/wallet/derivation.rs` (after Tier 2)

**Function Signature:**
```rust
pub async fn derive_bitcoin_like_address(
    seed_phrase: &str,
    coin_type: u32,
    prefix_byte: u8,
    index: u32,
) -> Result<String, String>
```

**Implementation Steps:**
1. Validate seed phrase
2. Parse mnemonic (BIP39)
3. Generate seed
4. Derive key using path: `m/44'/{coin_type}'/0'/0/{index}`
5. Extract compressed public key
6. Hash with SHA256 → RIPEMD160
7. Create payload: `[prefix_byte] + [hash] + [checksum]`
8. Encode to Base58
9. Return address string

**Networks to Add to Dispatcher:**
```rust
// Copy this pattern for each:
"dash" | "dashcoin" => {
    derive_bitcoin_like_address(seed_phrase, 5, 0x4Cu8, index).await
}
"zcash" | "zec" => {
    derive_bitcoin_like_address(seed_phrase, 133, 0x1Cu8, index).await
}
// ... repeat for Monacoin, Vertcoin, Digibyte, etc.
```

**Test Template:**
```rust
#[tokio::test]
async fn test_[network]_address_generation() {
    let addr = derive_address(TEST_MNEMONIC, "[ticker]", "[network]", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();
    assert!(address.starts_with('[expected_prefix]'));
    assert!(address.len() >= 26 && address.len() <= 34);
}
```

**Key Reference:**
| Network | Coin Type | Prefix | Example Prefix Char |
|---------|-----------|--------|---------------------|
| Dash | 5 | 0x4C | X (testnet: y) |
| Zcash | 133 | 0x1C | t (transparent) |
| Monacoin | 22 | 0x32 | M |
| Vertcoin | 28 | 0x47 | V |
| Digibyte | 20 | 0x1E | D |
| Ravencoin | 175 | 0x3C | R |

---

### 2. Cosmos-Like Wrapper (50+ networks)

**File Location:** `src/services/wallet/derivation.rs` (after Bitcoin-like)

**Function Signature:**
```rust
pub async fn derive_cosmos_like_address(
    seed_phrase: &str,
    coin_type: u32,
    hrp_prefix: &str,
    index: u32,
) -> Result<String, String>
```

**Implementation Steps:**
1. Validate seed phrase
2. Parse mnemonic (BIP39)
3. Generate seed
4. Derive key using path: `m/44'/{coin_type}'/0'/0/{index}`
5. Extract compressed public key
6. Hash with SHA256 → RIPEMD160
7. Encode with Bech32 using HRP: `hrp_prefix`
8. Return address string

**Networks to Add to Dispatcher:**
```rust
"osmosis" | "osmo" => {
    derive_cosmos_like_address(seed_phrase, 118, "osmo", index).await
}
"juno" => {
    derive_cosmos_like_address(seed_phrase, 118, "juno", index).await
}
// ... repeat for Akash, Regen, Stargaze, etc.
```

**Test Template:**
```rust
#[tokio::test]
async fn test_[network]_address_generation() {
    let addr = derive_address(TEST_MNEMONIC, "[ticker]", "[network]", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();
    assert!(address.starts_with("[hrp]1"));
    assert!(address.len() > 40);
}
```

**Key Reference:**
| Network | Coin Type | HRP Prefix |
|---------|-----------|-----------|
| Osmosis | 118 | osmo |
| Juno | 118 | juno |
| Akash | 118 | akash |
| Regen | 118 | regen |
| Stargaze | 118 | stars |
| Cronos | 60 | cro |
| Injective | 60 | inj |
| Secret | 529 | secret |
| Kava | 459 | kava |

---

### 3. Substrate-Like Wrapper (20+ networks)

**File Location:** `src/services/wallet/derivation.rs` (after Cosmos-like)

**Function Signature:**
```rust
pub async fn derive_substrate_like_address(
    seed_phrase: &str,
    ss58_prefix: u8,
    index: u32,
) -> Result<String, String>
```

**Implementation Steps:**
1. Validate seed phrase
2. Parse mnemonic (BIP39)
3. Generate seed
4. Derive key using path: `m/44'/354'/0'/0/{index}` (Polkadot base)
5. Extract Ed25519 public key
6. Encode to SS58 format with `ss58_prefix`
7. Return address string

**Networks to Add to Dispatcher:**
```rust
"kusama" | "ksm" => {
    derive_substrate_like_address(seed_phrase, 2, index).await
}
"acala" | "aca" => {
    derive_substrate_like_address(seed_phrase, 10, index).await
}
// ... repeat for Astar, Shiden, Parallel, etc.
```

**Test Template:**
```rust
#[tokio::test]
async fn test_[network]_address_generation() {
    let addr = derive_address(TEST_MNEMONIC, "[ticker]", "[network]", 0).await;
    assert!(addr.is_ok());
    let address = addr.unwrap();
    // Substrate addresses don't have fixed prefix
    assert!(address.len() > 45);
}
```

**Key Reference:**
| Network | SS58 Prefix |
|---------|-------------|
| Kusama | 2 |
| Acala | 10 |
| Astar | 5 |
| Shiden | 5 |
| Parallel | 172 |

---

## Phase 2: Special Implementations (10-15 hours)

### Tezos (XTZ)
**Path:** m/44'/1729'/0'/0/[index]
**Encoding:** Base58Check with Tezos alphabet
**Address Prefix:** tz1 (tz2, tz3 for other key types)
**Effort:** MED (3-4 hours)

### Algorand (ALGO)
**Path:** m/44'/283'/0'/0/[index]
**Encoding:** Base32 using special alphabet
**Address Format:** 58-character Base32 + 4-byte checksum
**Effort:** LOW (2-3 hours)

### Stellar (XLM)
**Derivation:** Ed25519 + custom StrKey encoder
**Address Prefix:** S (signing key), G (verification key)
**Effort:** MED (3-4 hours)

### Waves (WAVES)
**Encoding:** Custom WAVES address format
**Schema:** Version (1) + Chain ID (87) + Public Key Hash + Checksum
**Effort:** MED (3-4 hours)

### Stacks (STX)
**Base:** Bitcoin derivation extended
**Encoding:** Bitcoin format with Stacks version byte
**Effort:** MED (3-4 hours)

---

## Phase 3: Complex Implementations (15-25 hours)

### Ordinals (ORD)
**Type:** Taproot addresses (P2TR)
**Standard:** BIP341, BIP340 Schnorr signatures
**Effort:** HIGH (5-8 hours)
**Status:** Skip for Phase 1, needs research

### Zcash (ZEC)
**Type:** Multiple address types
**Transparent:** z-address (like Bitcoin, but different version)
**Shielded:** Use transparent only for now
**Effort:** HIGH (4-6 hours)

### TON (TON)
**Encoding:** Custom workchain-based address format
**Schema:** Workchain ID + Account ID
**Effort:** MED-HIGH (4-5 hours)

### Aptos (APT)
**Derivation:** Single address per account
**Format:** Hex-encoded account address
**Effort:** MED (3-4 hours)

### ICP (ICP)
**Type:** Principal IDs
**Format:** Bech32-like encoding of principal
**Effort:** HIGH (4-6 hours)

---

## Testing Checklist for Each Network

- [ ] Address generation succeeds
- [ ] Address starts with correct prefix/format
- [ ] Address has correct length
- [ ] Determinism: same seed + index = same address
- [ ] Uniqueness: different indices = different addresses
- [ ] Invalid seed rejected
- [ ] Dispatcher aliases work
- [ ] Performance: 100 addresses in <5 seconds
- [ ] Address validates in block explorer

---

## Dispatcher Entry Template

```rust
"[network_name]" | "[ticker]" => {
    derive_[function_name](seed_phrase, [params], index).await
}
```

**Examples:**
```rust
// Bitcoin-like
"dash" | "dashcoin" => {
    derive_bitcoin_like_address(seed_phrase, 5, 0x4Cu8, index).await
}

// Cosmos-like
"osmosis" | "osmo" => {
    derive_cosmos_like_address(seed_phrase, 118, "osmo", index).await
}

// Substrate-like
"kusama" | "ksm" => {
    derive_substrate_like_address(seed_phrase, 2, index).await
}
```

---

## Common Errors & Solutions

**Error:** "Invalid derivation path"
- **Solution:** Verify coin_type is correct for network
- **Check:** BIP44 specification for that blockchain

**Error:** "Invalid seed phrase"
- **Solution:** Test mnemonic uses valid BIP39 wordlist
- **Check:** Use TEST_MNEMONIC from test constants

**Error:** "Address doesn't start with expected prefix"
- **Solution:** Check version byte / prefix byte / HRP
- **Check:** Blockchain specification documentation

**Error:** "Performance test fails (>6 seconds)"
- **Solution:** Check for inefficient hash loops
- **Fix:** Cache common computations, avoid re-derives

**Error:** "Address validates in explorer but fails in swap"
- **Solution:** Check address format matches explorer
- **Debug:** Log generated address and compare with manual wallet

---

## File Modification Order

1. **Add functions to derivation.rs:**
   - derive_bitcoin_like_address()
   - derive_cosmos_like_address()
   - derive_substrate_like_address()
   - [Phase 2 functions]

2. **Update dispatcher in derive_address():**
   - Add 100+ case entries for Phase 1
   - Add 10 case entries for Phase 2
   - Add 10 case entries for Phase 3

3. **Create test files:**
   - tests/wallet/tier3_phase1_test.rs (300+ tests)
   - tests/wallet/tier3_phase2_test.rs (50+ tests)
   - tests/wallet/tier3_phase3_test.rs (50+ tests)

4. **Register in tests/wallet/mod.rs:**
   ```rust
   pub mod tier3_phase1_test;
   pub mod tier3_phase2_test;
   pub mod tier3_phase3_test;
   ```

---

## Commands to Run While Implementing

```bash
# Check compilation
cargo check -p exchange-shared

# Run specific test file
cargo test --test wallet_tests tier3_phase1

# Run all wallet tests
cargo test --test wallet_tests

# Run with backtrace on error
RUST_BACKTRACE=1 cargo test --test wallet_tests tier3_phase1 -- --nocapture

# Benchmark specific test
cargo test --test wallet_tests performance --release -- --nocapture
```

---

## Progress Tracking

**Phase 1 Implementation:**
- [ ] Bitcoin-like wrapper (8-10 hours)
  - [ ] Function implemented
  - [ ] 30+ networks in dispatcher
  - [ ] Tests created and passing
- [ ] Cosmos-like wrapper (5-8 hours)
  - [ ] Function implemented
  - [ ] 50+ networks in dispatcher
  - [ ] Tests created and passing
- [ ] Substrate-like wrapper (3-4 hours)
  - [ ] Function implemented
  - [ ] 20+ networks in dispatcher
  - [ ] Tests created and passing
- [ ] Integration & verification (2-3 hours)
  - [ ] All 100+ tests passing
  - [ ] Performance verified
  - [ ] Dispatcher verified

**Phase 1 Subtotal:** 15-20 hours → 91-92% coverage

---

**Last Updated:** March 1, 2026  
**Ready for Implementation:** ✅ YES

// - Phase 3 (High-complexity): Ordinals, Zcash shielded, ICP, Aptos, and 90+ remaining networks 