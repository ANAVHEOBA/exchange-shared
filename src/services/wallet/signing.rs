use ed25519_dalek::{Signer, SigningKey};
use hex;
use secp256k1::{Message, Secp256k1, SecretKey};
use sha3::{Digest, Keccak256};
use std::str::FromStr;

use crate::modules::wallet::schema::EvmTransaction;

pub struct SigningService;

impl SigningService {
    /// Sign an EVM transaction (Ethereum, Polygon, Arbitrum, etc.)
    /// Implements EIP-155 signing with RLP encoding
    pub fn sign_evm_transaction(
        private_key_hex: &str,
        tx: &EvmTransaction,
    ) -> Result<String, String> {
        let secp = Secp256k1::new();

        let clean_key = private_key_hex.trim_start_matches("0x");
        let secret_key =
            SecretKey::from_str(clean_key).map_err(|e| format!("Invalid private key: {}", e))?;

        // 1. Prepare EIP-155 fields for RLP encoding
        // [nonce, gasPrice, gasLimit, to, value, data, chainId, 0, 0]
        let mut rlp_fields: Vec<Vec<u8>> = Vec::new();
        rlp_fields.push(encode_u64(tx.nonce));
        rlp_fields.push(encode_u64(tx.gas_price));
        rlp_fields.push(encode_u64(21000)); // Default gas limit for transfer
        rlp_fields
            .push(hex::decode(tx.to_address.trim_start_matches("0x")).map_err(|e| e.to_string())?);
        rlp_fields.push(encode_f64_to_wei(tx.amount));
        rlp_fields.push(Vec::new()); // Empty data
        rlp_fields.push(encode_u64(tx.chain_id as u64));
        rlp_fields.push(Vec::new()); // r = 0 for signing hash
        rlp_fields.push(Vec::new()); // s = 0 for signing hash

        // 2. RLP Encode and Hash
        let rlp_encoded = encode_list(&rlp_fields);
        let mut hasher = Keccak256::new();
        hasher.update(&rlp_encoded);
        let hash = hasher.finalize();

        let message = Message::from_digest_slice(&hash)
            .map_err(|e| format!("Invalid message hash: {}", e))?;

        // 3. Sign the message
        let sig = secp.sign_ecdsa_recoverable(&message, &secret_key);
        let (rec_id, sig_bytes) = sig.serialize_compact();

        // 4. Final V calculation (EIP-155)
        let v = (rec_id.to_i32() + 35 + (tx.chain_id as i32 * 2)) as u8;

        let mut final_sig = hex::encode(sig_bytes);
        final_sig.push_str(&format!("{:02x}", v));

        Ok(format!("0x{}", final_sig))
    }

    /// Sign a Solana transaction using Ed25519
    pub fn sign_solana_transaction(
        private_key_hex: &str,
        tx_data_hex: &str,
    ) -> Result<String, String> {
        let clean_key = private_key_hex.trim_start_matches("0x");
        let key_bytes = hex::decode(clean_key).map_err(|e| e.to_string())?;

        let signing_key = SigningKey::from_bytes(
            key_bytes
                .as_slice()
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let message_bytes = hex::decode(tx_data_hex).map_err(|e| e.to_string())?;

        let signature = signing_key.sign(&message_bytes);

        Ok(format!("0x{}", hex::encode(signature.to_bytes())))
    }

    /// Sign a Bitcoin transaction (Foundation for P2WPKH)
    pub fn sign_btc_transaction(
        private_key_hex: &str,
        sighash_hex: &str,
    ) -> Result<String, String> {
        let secp = Secp256k1::new();
        let clean_key = private_key_hex.trim_start_matches("0x");
        let secret_key = SecretKey::from_str(clean_key).map_err(|e| e.to_string())?;

        let hash_bytes = hex::decode(sighash_hex).map_err(|e| e.to_string())?;
        let message = Message::from_digest_slice(&hash_bytes).map_err(|e| e.to_string())?;

        let sig = secp.sign_ecdsa(&message, &secret_key);

        Ok(hex::encode(sig.serialize_der()))
    }

    /// Sign a UTXO transaction (Dash, Dogecoin, Zcash)
    /// Core logic is similar to Bitcoin ECDSA but with different sighash handling
    pub fn sign_utxo_transaction(
        private_key_hex: &str,
        sighash_hex: &str,
    ) -> Result<String, String> {
        // Reuse BTC logic as the fundamental signing (ECDSA on Secp256k1) is identical
        Self::sign_btc_transaction(private_key_hex, sighash_hex)
    }

    /// Sign a Cosmos transaction (Osmosis, Juno, etc.)
    /// Uses Secp256k1 and produces a signature for the Doc object
    pub fn sign_cosmos_transaction(
        private_key_hex: &str,
        sign_doc_hex: &str,
    ) -> Result<String, String> {
        let secp = Secp256k1::new();
        let clean_key = private_key_hex.trim_start_matches("0x");
        let secret_key = SecretKey::from_str(clean_key).map_err(|e| e.to_string())?;

        // Cosmos signs the SHA256 of the sign doc
        let doc_bytes = hex::decode(sign_doc_hex).map_err(|e| e.to_string())?;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&doc_bytes);
        let hash = hasher.finalize();

        let message = Message::from_digest_slice(&hash).map_err(|e| e.to_string())?;
        let sig = secp.sign_ecdsa(&message, &secret_key);

        // Cosmos expects the compact 64-byte signature [r, s]
        let sig_bytes = sig.serialize_compact();
        Ok(hex::encode(sig_bytes))
    }

    /// Sign a Substrate transaction (Polkadot, Kusama)
    /// Uses Ed25519 (or Sr25519, but Ed25519 is standard for many implementations)
    pub fn sign_substrate_transaction(
        private_key_hex: &str,
        message_hex: &str,
    ) -> Result<String, String> {
        // Reuse Solana logic as the fundamental signing (Ed25519) is identical
        Self::sign_solana_transaction(private_key_hex, message_hex)
    }

    /// Sign an Algorand/NEAR/TON transaction
    /// All use Ed25519 signatures
    pub fn sign_ed25519_transaction(
        private_key_hex: &str,
        message_hex: &str,
    ) -> Result<String, String> {
        Self::sign_solana_transaction(private_key_hex, message_hex)
    }
}

// =============================================================================
// SIMPLIFIED RLP ENCODER
// =============================================================================

fn encode_u64(val: u64) -> Vec<u8> {
    if val == 0 {
        return vec![];
    }
    let bytes = val.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(8);
    bytes[start..].to_vec()
}

fn encode_f64_to_wei(amount: f64) -> Vec<u8> {
    // 1 ETH = 10^18 Wei
    let wei = (amount * 1_000_000_000_000_000_000.0) as u128;
    let bytes = wei.to_be_bytes();
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(16);
    bytes[start..].to_vec()
}

fn encode_list(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut payload = Vec::new();
    for el in elements {
        if el.len() == 1 && el[0] < 0x80 {
            payload.push(el[0]);
        } else if el.len() < 56 {
            payload.push(0x80 + el.len() as u8);
            payload.extend_from_slice(el);
        } else {
            let len_bytes = encode_u64(el.len() as u64);
            payload.push(0xb7 + len_bytes.len() as u8);
            payload.extend_from_slice(&len_bytes);
            payload.extend_from_slice(el);
        }
    }

    let mut result = Vec::new();
    if payload.len() < 56 {
        result.push(0xc0 + payload.len() as u8);
    } else {
        let len_bytes = encode_u64(payload.len() as u64);
        result.push(0xf7 + len_bytes.len() as u8);
        result.extend_from_slice(&len_bytes);
    }
    result.extend(payload);
    result
}
