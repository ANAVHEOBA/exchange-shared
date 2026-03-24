use secp256k1::{Message, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha512};

/// XRP Ledger transaction builder
/// Implements proper JSON-RPC format for XRP transactions
#[derive(Debug, Serialize, Deserialize)]
pub struct XrpTransaction {
    #[serde(rename = "TransactionType")]
    pub transaction_type: String,
    #[serde(rename = "Account")]
    pub account: String,
    #[serde(rename = "Destination")]
    pub destination: String,
    #[serde(rename = "Amount")]
    pub amount: String, // In drops (1 XRP = 1,000,000 drops)
    #[serde(rename = "Fee")]
    pub fee: String, // In drops
    #[serde(rename = "Sequence")]
    pub sequence: u64,
    #[serde(rename = "SigningPubKey")]
    pub signing_pub_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "DestinationTag")]
    pub destination_tag: Option<u32>,
}

impl XrpTransaction {
    pub fn new_payment(
        sender: &str,
        receiver: &str,
        amount_drops: u64,
        fee_drops: u64,
        sequence: u64,
        destination_tag: Option<u32>,
    ) -> Self {
        Self {
            transaction_type: "Payment".to_string(),
            account: sender.to_string(),
            destination: receiver.to_string(),
            amount: amount_drops.to_string(),
            fee: fee_drops.to_string(),
            sequence,
            signing_pub_key: String::new(), // Will be filled during signing
            destination_tag,
        }
    }

    /// Sign the transaction with Secp256k1
    pub fn sign(&mut self, private_key_hex: &str) -> Result<String, String> {
        let secp = Secp256k1::new();

        // Parse private key
        let secret_key = SecretKey::from_slice(
            &hex::decode(private_key_hex.trim_start_matches("0x"))
                .map_err(|e| format!("Invalid private key: {}", e))?,
        )
        .map_err(|e| format!("Invalid secret key: {}", e))?;

        // Get public key
        let public_key = secp256k1::PublicKey::from_secret_key(&secp, &secret_key);
        self.signing_pub_key = hex::encode(public_key.serialize());

        // Serialize transaction for signing (canonical JSON)
        let tx_json =
            serde_json::to_string(&self).map_err(|e| format!("Failed to serialize: {}", e))?;

        // Hash with SHA-512 (first half)
        let mut hasher = Sha512::new();
        hasher.update(b"STX\x00"); // Single-signing prefix
        hasher.update(tx_json.as_bytes());
        let hash = hasher.finalize();
        let hash_half = &hash[..32]; // Use first 32 bytes

        // Sign
        let message =
            Message::from_digest_slice(hash_half).map_err(|e| format!("Invalid message: {}", e))?;
        let signature = secp.sign_ecdsa(&message, &secret_key);

        // Return hex-encoded signature
        Ok(hex::encode(signature.serialize_compact()))
    }

    /// Build signed transaction blob for submission
    pub fn to_blob(&self, signature: &str) -> Result<String, String> {
        // XRP uses binary serialization for transaction blobs
        // This is a simplified version - production should use proper binary encoding
        let tx_json = json!({
            "TransactionType": self.transaction_type,
            "Account": self.account,
            "Destination": self.destination,
            "Amount": self.amount,
            "Fee": self.fee,
            "Sequence": self.sequence,
            "SigningPubKey": self.signing_pub_key,
            "TxnSignature": signature,
        });

        Ok(tx_json.to_string())
    }
}

/// Get account info from XRP Ledger
pub async fn get_xrp_account_info(rpc_url: &str, address: &str) -> Result<XrpAccountInfo, String> {
    let client = reqwest::Client::new();
    let response = client
        .post(rpc_url)
        .json(&json!({
            "method": "account_info",
            "params": [{
                "account": address,
                "ledger_index": "current"
            }]
        }))
        .send()
        .await
        .map_err(|e| format!("Failed to get account info: {}", e))?;

    let result: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))?;

    let account_data = result["result"]["account_data"]
        .as_object()
        .ok_or("Missing account_data")?;

    Ok(XrpAccountInfo {
        sequence: account_data["Sequence"]
            .as_u64()
            .ok_or("Missing sequence")?,
        balance: account_data["Balance"]
            .as_str()
            .ok_or("Missing balance")?
            .parse()
            .map_err(|e| format!("Invalid balance: {}", e))?,
    })
}

#[derive(Debug)]
pub struct XrpAccountInfo {
    pub sequence: u64,
    pub balance: u64, // In drops
}
