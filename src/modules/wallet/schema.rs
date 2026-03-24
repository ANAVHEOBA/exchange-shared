use super::model::PayoutStatus;
use serde::{Deserialize, Serialize};

// =============================================================================
// REQUESTS
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateAddressRequest {
    pub swap_id: String,
    pub ticker: String,
    pub network: String,
    pub user_recipient_address: String,
    pub user_recipient_extra_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayoutRequest {
    pub swap_id: String,
}

// =============================================================================
// RESPONSES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletAddressResponse {
    pub address: String,
    pub address_index: u32,
    pub swap_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayoutResponse {
    pub tx_hash: String,
    pub amount: f64,
    pub status: PayoutStatus,
}

// =============================================================================
// EXECUTION & DTO TYPES
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapExecution {
    pub swap_id: String,
    pub user_recipient_address: String,
    pub amount_to_send: f64,
    pub chain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    pub to: String,
    pub amount: f64,
    pub token: String,
    pub chain: String,
    #[serde(default)]
    pub extra_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvmTransaction {
    pub to_address: String,
    pub amount: f64,
    pub token: String,
    pub chain_id: u32,
    pub nonce: u64,
    pub gas_price: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcTransaction {
    pub inputs: Vec<BtcUTXO>,
    pub outputs: Vec<BtcOutput>,
    pub fee: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcUTXO {
    pub input: String,
    pub output_index: u32,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcOutput {
    pub address: String,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaTransaction {
    pub from_pubkey: String,
    pub to_pubkey: String,
    pub amount: u64,
    pub recent_blockhash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayoutRecord {
    pub received_amount: f64,
    pub tier: String,
}
