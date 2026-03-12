# Priority Tier 2: Blockchain Address Generation Research 📋

**Status:** 📋 RESEARCH PHASE (Pre-Implementation)  
**Blockchains:** 10 networks, ~10-15% additional trading volume  
**Implementation Time:** 10-15 hours  
**Next Step:** Implement after documentation validation  

---

## Bitcoin-Like Chains (Coin Family: UTXO-based)

### 1. Litecoin (LTC) 🟰 Bitcoin-like

**Official Docs:** https://litecoin.org/en/developer-guide  
**BIP44 Path:** m/44'/2'/0'/0/[index]  
**Coin Type:** 2 (registered in SLIP-0044)  
**Key Algorithm:** Secp256k1 (same as Bitcoin)  
**Address Format:** Base58Check, mainnet prefix 0x30 (addresses start with 'L')  
**Checksum:** Double SHA256 (same as Bitcoin)  

**Differences from Bitcoin:**
- Different version byte (0x30 for mainnet vs 0x00 for Bitcoin)
- Same BIP32/BIP44 derivation process
- Different address prefix (L instead of 1)

**Implementation:** Use same Bitcoin address derivation logic, just change version byte

---

### 2. Dogecoin (DOGE) 🐕 Bitcoin-like

**Official Docs:** https://github.com/dogecoin/dogecoin/blob/master/doc/README.md  
**BIP44 Path:** m/44'/3'/0'/0/[index]  
**Coin Type:** 3 (registered in SLIP-0044)  
**Key Algorithm:** Secp256k1  
**Address Format:** Base58Check, mainnet prefix 0x1E (addresses start with 'D')  
**Checksum:** Double SHA256  

**Differences from Bitcoin:**
- Coin type 3 (vs Bitcoin 0)
- Version byte 0x1E (different prefix)
- Otherwise identical to Bitcoin address derivation

---

### 3. Bitcoin Cash (BCH) ⚠️ Bitcoin-like BUT SPECIAL

**Official Docs:** https://bitcoincash.org/  
**BIP44 Path:** m/44'/145'/0'/0/[index]  
**Coin Type:** 145 (registered in SLIP-0044)  
**Key Algorithm:** Secp256k1  

**Address Format - TWO OPTIONS:**
1. **Legacy (Bitcoin format):** Base58Check, prefix 0x00 (looks like 1...)
   - Used by older wallets
   - Compatible with Bitcoin address parser
   
2. **CashAddr (BCH format):** New standard
   - Format: `bitcoincash:qph2v...` or just `qph2v...`
   - Uses bech32-like encoding with custom alphabet
   - Recommended for new implementations

**DECISION:** Use CashAddr format (newer standard)  
**Implementation:** Custom base32 encoding with BCH alphabet

---

## Account-Based Chains (Unique Derivation)

### 4. Stellar Lumens (XLM)

**Official Docs:** https://developers.stellar.org/docs/  
**Key Derivation:** Custom (NOT standard BIP44)  
**Signing Key:** Ed25519 keypair  
**Address Format:** StrKey encoding (base32 with custom alphabet)  
**Address Prefix:** 'G' for mainnet accounts  
**Checksum:** Custom StrKey checksum (CRC16)  

**Key Differences:**
- Uses ECDSA Ed25519 keys
- Custom derivation path (not BIP44)
- StrKey encoding (NOT Bech32)
- Account addresses start with 'G'

**Implementation:** Requires Ed25519 + StrKey custom encoder

---

### 5. Algorand (ALGO)

**Official Docs:** https://developer.algorand.org/docs/  
**Key Derivation:** BIP44 path m/44'/283'/0'/0'/[index]  
**Coin Type:** 283 (registered in SLIP-0044)  
**Key Algorithm:** Ed25519  
**Address Format:** Base32 encoding with checksum  
**Address Length:** 58 characters  
**Checksum:** 4-byte Blake2b hash  

**Format Details:**
- Public key (32 bytes) + Checksum (4 bytes) = 36 bytes total
- Base32 encoded = 58 characters
- Addresses look like: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY5HVY

---

### 6. NEAR Protocol

**Official Docs:** https://docs.near.org/  
**Account Format:** Named accounts (alice.near) OR Implicit accounts (hex format)  
**Key Algorithm:** Ed25519  

**Account Types:**
1. **Named Accounts:** Top-level accounts like `myaccount.near`
2. **Subaccounts:** Like `subaccount.myaccount.near`
3. **Implicit Accounts:** Derived from public key hash (64-char hex)

**For Our Use Case:** Use implicit accounts (public key derived)  
**Format:** Hex-encoded public key (no prefix)  
**Length:** 64 characters (32-byte public key in hex)

---

### 7. Tezos (XTZ)

**Official Docs:** https://tezos.com/developer/  
**BIP44 Path:** m/44'/1729'/0'/0'/[index]  
**Coin Type:** 1729 (registered in SLIP-0044)  
**Key Algorithm:** Ed25519  
**Address Format:** Base58Check with custom Tezos encoding  
**Address Prefix:** 'tz1', 'tz2', or 'tz3' (depending on key type)  

**Address Types:**
- `tz1...` = Ed25519 public key hash (most common)
- `tz2...` = Secp256k1 public key hash
- `tz3...` = P-256 public key hash

**For Our Use:** Use Ed25519 (tz1 prefix)  
**Implementation:** Base58Check with Tezos alphabet + custom prefix

---

## Additional Research Needed

### Filecoin (FIL) ⚠️ COMPLEX
- **Status:** Different address format than typical crypto
- **Types:** f0, f1 (secp256k1), f2 (Actor), f3 (BLS)
- **Research Required:** Before implementation

### Monero (XMR) ✅ ALREADY IMPLEMENTED
- Already have working `derive_xmr_address()` in code
- Uses Monero-specific key derivation
- Should verify it works with Trocador

### EOS ⚠️ NO STANDARD DERIVATION
- Accounts are named (like alice)
- No standard key derivation from seed
- Requires custom account creation flow
- **Decision:** Skip for now (complex integration)

---

## BIP44 Coin Types Summary (SLIP-0044)

| Blockchain | Coin Type | BIP44 Path | Status |
|-----------|-----------|-----------|--------|
| Bitcoin | 0 | m/44'/0'/0'/0/[idx] | ✅ Done (Tier 1) |
| Litecoin | 2 | m/44'/2'/0'/0/[idx] | ⏳ Tier 2 |
| Dogecoin | 3 | m/44'/3'/0'/0/[idx] | ⏳ Tier 2 |
| Bitcoin Cash | 145 | m/44'/145'/0'/0/[idx] | ⏳ Tier 2 |
| Tezos | 1729 | m/44'/1729'/0'/0/[idx] | ⏳ Tier 2 |
| Algorand | 283 | m/44'/283'/0'/0/[idx] | ⏳ Tier 2 |
| Ripple | 144 | m/44'/144'/0'/0/[idx] | ✅ Done (Tier 1) |
| Tron | 195 | m/44'/195'/0'/0/[idx] | ✅ Done (Tier 1) |
| Cosmos | 118 | m/44'/118'/0'/0/[idx] | ✅ Done (Tier 1) |
| Polkadot | 354 | m/44'/354'/0'/0/[idx] | ✅ Done (Tier 1) |
| Cardano | 1815 | m/1852'/1815'/0'/0/[idx] | ✅ Done (Tier 1) |
| Monero | - | Custom | ✅ Done (Tier 1) |
| Solana | 501 | m/44'/501'/0'/0/[idx] | ✅ Done (Tier 1) |
| Stellar | - | Custom | ⏳ Tier 2 |
| NEAR | - | Custom | ⏳ Tier 2 |

---

## Encoding Methods Needed

| Method | Used By | Status |
|--------|---------|--------|
| Base58Check (Bitcoin) | Bitcoin, Litecoin, Dogecoin, Tezos | ✅ Already have |
| Bech32 | Cosmos, others | ✅ Already have |
| Base58Check (Ripple) | Ripple, Tron | ✅ Custom implemented |
| SS58 | Polkadot | ✅ Custom implemented |
| Bech32 (Cardano) | Cardano | ✅ Custom implemented |
| Base32 (CashAddr) | Bitcoin Cash | ⏳ Need to implement |
| Base32 | Algorand | ⏳ Need to implement |
| StrKey | Stellar | ⏳ Need to implement |
| Base58Check (Tezos) | Tezos | ⏳ Need to implement |
| Hex | NEAR | ✅ Already have |

---

## Implementation Order (Priority 2)

### Phase 1: Bitcoin-Like (3-4 hours)
1. Litecoin - Use Bitcoin code, change version byte
2. Dogecoin - Use Bitcoin code, change version byte
3. Bitcoin Cash - New CashAddr encoder

### Phase 2: Standard BIP44 (3-4 hours)
1. Algorand - Simple BIP44 + Base32
2. Tezos - Simple BIP44 + Base58Check with prefix

### Phase 3: Custom Derivation (3-4 hours)
1. Stellar - Ed25519 + StrKey encoder
2. NEAR - Ed25519 + Hex encoding

---

## Next Steps

1. ✅ Research complete (this document)
2. ⏳ Implement Phase 1 (Bitcoin-like chains)
3. ⏳ Implement Phase 2 (BIP44 chains)
4. ⏳ Implement Phase 3 (Custom derivation)
5. ⏳ Add comprehensive tests
6. ⏳ Update dispatcher
7. ⏳ Verify with test vectors

---

## Known Issues & Decisions

1. **Bitcoin Cash:** Using CashAddr format (not legacy) - this is the modern standard
2. **EOS:** Skipping for now - requires account name registration (not suitable for auto-generation)
3. **Filecoin:** Requires more research - complex address format
4. **Monero:** Already implemented, verify compatibility
5. **Stellar:** Custom StrKey encoding - not standard crypto

---

**Last Updated:** March 1, 2026  
**Research Status:** ✅ COMPLETE  
**Ready for Implementation:** ✅ YES
