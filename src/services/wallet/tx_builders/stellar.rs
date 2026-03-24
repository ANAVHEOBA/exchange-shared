use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stellar (XLM) transaction builder
#[derive(Debug, Serialize, Deserialize)]
pub struct StellarTransaction {
    pub source_account: String,
    pub fee: u32,
    pub sequence_number: String,
    pub operations: Vec<StellarOperation>,
    pub memo: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StellarOperation {
    #[serde(rename = "type")]
    pub op_type: String,
    pub destination: String,
    pub asset: StellarAsset,
    pub amount: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StellarAsset {
    #[serde(rename = "type")]
    pub asset_type: String,
}

impl StellarTransaction {
    pub fn new_payment(
        source: &str,
        destination: &str,
        amount_stroops: u64, // 1 XLM = 10,000,000 stroops
        sequence: u64,
        fee: u32,
    ) -> Self {
        Self {
            source_account: source.to_string(),
            fee,
            sequence_number: sequence.to_string(),
            operations: vec![StellarOperation {
                op_type: "payment".to_string(),
                destination: destination.to_string(),
                asset: StellarAsset {
                    asset_type: "native".to_string(),
                },
                amount: format!("{:.7}", amount_stroops as f64 / 10_000_000.0),
            }],
            memo: None,
        }
    }

    /// Sign with Ed25519
    pub fn sign(&self, private_key: &[u8], network_passphrase: &str) -> Result<String, String> {
        // Build transaction envelope
        let tx_json =
            serde_json::to_string(self).map_err(|e| format!("Failed to serialize: {}", e))?;

        // Hash with network passphrase
        let mut hasher = Sha256::new();
        hasher.update(network_passphrase.as_bytes());
        hasher.update(tx_json.as_bytes());
        let hash = hasher.finalize();

        // Sign
        let signing_key = SigningKey::from_bytes(
            private_key[..32]
                .try_into()
                .map_err(|_| "Invalid key length")?,
        );
        let signature = signing_key.sign(&hash);

        Ok(hex::encode(signature.to_bytes()))
    }
}

pub const STELLAR_MAINNET_PASSPHRASE: &str = "Public Global Stellar Network ; September 2015";
