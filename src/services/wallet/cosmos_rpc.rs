use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

use super::rpc::{BlockchainProvider, CosmosAccountState, RpcError};

#[derive(Debug, Clone, Copy)]
pub struct CosmosChainConfig {
    pub denom: &'static str,
    pub decimals: u8,
    // Cosmos gas prices are often fractional base units per gas, so we keep
    // an exact rational and derive the tx fee from gas_limit at runtime.
    pub gas_price_numerator: u64,
    pub gas_price_denominator: u64,
    pub gas_limit: u64,
}

impl CosmosChainConfig {
    pub fn fee_amount_base_units(&self) -> u64 {
        let numerator = (self.gas_limit as u128) * (self.gas_price_numerator as u128);
        let denominator = self.gas_price_denominator as u128;
        numerator.div_ceil(denominator) as u64
    }

    pub fn network_fee_native(&self) -> f64 {
        self.fee_amount_base_units() as f64 / 10f64.powi(self.decimals as i32)
    }
}

#[derive(Debug, Deserialize)]
struct CosmosBalanceResponse {
    #[serde(default)]
    balance: Option<CosmosCoinBalance>,
}

#[derive(Debug, Deserialize)]
struct CosmosCoinBalance {
    amount: String,
}

pub fn supported_cosmos_chain(chain_key: &str) -> Option<CosmosChainConfig> {
    match chain_key {
        "agoric" => Some(CosmosChainConfig {
            denom: "ubld",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "akash" => Some(CosmosChainConfig {
            denom: "uakt",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "axelar" => Some(CosmosChainConfig {
            denom: "uaxl",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "band" => Some(CosmosChainConfig {
            denom: "uband",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "celestia" => Some(CosmosChainConfig {
            denom: "utia",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "cheqd" => Some(CosmosChainConfig {
            denom: "ncheq",
            decimals: 9,
            gas_price_numerator: 7_500,
            gas_price_denominator: 1,
            gas_limit: 120_000,
        }),
        "coreum" => Some(CosmosChainConfig {
            denom: "ucore",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 16,
            gas_limit: 120_000,
        }),
        "cosmos_hub" => Some(CosmosChainConfig {
            denom: "uatom",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "dydx" => Some(CosmosChainConfig {
            denom: "adydx",
            decimals: 18,
            gas_price_numerator: 12_500_000_000,
            gas_price_denominator: 1,
            gas_limit: 120_000,
        }),
        "dymension" => Some(CosmosChainConfig {
            denom: "adym",
            decimals: 18,
            gas_price_numerator: 5_000_000_000,
            gas_price_denominator: 1,
            gas_limit: 120_000,
        }),
        "fetch" => Some(CosmosChainConfig {
            denom: "afet",
            decimals: 18,
            gas_price_numerator: 25,
            gas_price_denominator: 1_000,
            gas_limit: 120_000,
        }),
        "initia" => Some(CosmosChainConfig {
            denom: "uinit",
            decimals: 6,
            gas_price_numerator: 15,
            gas_price_denominator: 1_000,
            gas_limit: 120_000,
        }),
        "juno" => Some(CosmosChainConfig {
            denom: "ujuno",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "kyve" => Some(CosmosChainConfig {
            denom: "ukyve",
            decimals: 6,
            gas_price_numerator: 80,
            gas_price_denominator: 1,
            gas_limit: 120_000,
        }),
        "neutron" => Some(CosmosChainConfig {
            denom: "untrn",
            decimals: 6,
            gas_price_numerator: 53,
            gas_price_denominator: 10_000,
            gas_limit: 120_000,
        }),
        "oraichain" => Some(CosmosChainConfig {
            denom: "uorai",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "osmosis" => Some(CosmosChainConfig {
            denom: "uosmo",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "persistence" => Some(CosmosChainConfig {
            denom: "uxprt",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "regen" => Some(CosmosChainConfig {
            denom: "uregen",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "secret" => Some(CosmosChainConfig {
            denom: "uscrt",
            decimals: 6,
            gas_price_numerator: 5,
            gas_price_denominator: 28,
            gas_limit: 140_000,
        }),
        "shentu" => Some(CosmosChainConfig {
            denom: "uctk",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "stargaze" => Some(CosmosChainConfig {
            denom: "ustars",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        "terra" => Some(CosmosChainConfig {
            denom: "uluna",
            decimals: 6,
            gas_price_numerator: 1,
            gas_price_denominator: 12,
            gas_limit: 120_000,
        }),
        _ => None,
    }
}

pub fn is_supported_cosmos_chain(chain_key: &str) -> bool {
    supported_cosmos_chain(chain_key).is_some()
}

pub struct CosmosRpcClient {
    client: reqwest::Client,
    base_url: String,
    config: CosmosChainConfig,
}

impl CosmosRpcClient {
    pub fn new(url: String, chain_key: &str) -> Result<Self, String> {
        let config = supported_cosmos_chain(chain_key)
            .ok_or_else(|| format!("No supported Cosmos metadata for {}", chain_key))?;

        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            base_url: normalize_cosmos_base_url(&url),
            config,
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, RpcError> {
        self.client
            .get(self.endpoint(path))
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))
    }

    async fn get_value(&self, path: &str) -> Result<Value, RpcError> {
        self.client
            .get(self.endpoint(path))
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))
    }
}

fn normalize_cosmos_base_url(url: &str) -> String {
    let mut normalized = url.trim_end_matches('/').to_string();

    for suffix in [
        "/status",
        "/api/cosmos/base/tendermint/v1beta1/blocks/latest",
        "/cosmos/base/tendermint/v1beta1/blocks/latest",
    ] {
        if normalized.ends_with(suffix) {
            normalized.truncate(normalized.len() - suffix.len());
            break;
        }
    }

    normalized
}

fn parse_decimal_amount(amount: &str, decimals: u8) -> Result<f64, RpcError> {
    let base_amount = amount
        .parse::<u128>()
        .map_err(|e| RpcError::Parse(format!("Invalid Cosmos amount '{}': {}", amount, e)))?;
    Ok(base_amount as f64 / 10f64.powi(decimals as i32))
}

fn extract_account_numbers(value: &Value) -> Option<(u64, u64)> {
    if let (Some(account_number), Some(sequence)) = (
        value.get("account_number").and_then(|v| v.as_str()),
        value.get("sequence").and_then(|v| v.as_str()),
    ) {
        let account_number = account_number.parse::<u64>().ok()?;
        let sequence = sequence.parse::<u64>().ok()?;
        return Some((account_number, sequence));
    }

    match value {
        Value::Object(map) => map.values().find_map(extract_account_numbers),
        Value::Array(items) => items.iter().find_map(extract_account_numbers),
        _ => None,
    }
}

#[async_trait]
impl BlockchainProvider for CosmosRpcClient {
    async fn get_balance(&self, address: &str) -> Result<f64, RpcError> {
        let path = format!(
            "/cosmos/bank/v1beta1/balances/{address}/by_denom?denom={}",
            self.config.denom
        );

        let response: CosmosBalanceResponse = self.get_json(&path).await?;
        let amount = response
            .balance
            .map(|balance| balance.amount)
            .unwrap_or_else(|| "0".to_string());

        parse_decimal_amount(&amount, self.config.decimals)
    }

    async fn send_raw_transaction(&self, signed_hex: &str) -> Result<String, RpcError> {
        let signed_bytes = hex::decode(signed_hex.trim_start_matches("0x"))
            .map_err(|e| RpcError::Parse(format!("Invalid Cosmos tx hex: {}", e)))?;

        let response = self
            .client
            .post(self.endpoint("/cosmos/tx/v1beta1/txs"))
            .json(&serde_json::json!({
                "tx_bytes": BASE64_STANDARD.encode(signed_bytes),
                "mode": "BROADCAST_MODE_SYNC"
            }))
            .send()
            .await
            .map_err(|e| RpcError::Network(e.to_string()))?;

        let value: Value = response
            .json()
            .await
            .map_err(|e| RpcError::Parse(e.to_string()))?;

        let tx_response = value.get("tx_response").ok_or_else(|| {
            RpcError::Parse("Missing tx_response in Cosmos broadcast response".to_string())
        })?;

        let code = tx_response.get("code").and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|raw| raw.parse::<u64>().ok()))
        });

        if code.unwrap_or(0) != 0 {
            return Err(RpcError::Rpc(
                tx_response
                    .get("raw_log")
                    .and_then(|v| v.as_str())
                    .or_else(|| tx_response.get("codespace").and_then(|v| v.as_str()))
                    .unwrap_or("Cosmos broadcast failed")
                    .to_string(),
            ));
        }

        tx_response
            .get("txhash")
            .and_then(|v| v.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| RpcError::Parse("Missing Cosmos txhash".to_string()))
    }

    async fn cosmos_get_account_state(
        &self,
        address: &str,
    ) -> Result<CosmosAccountState, RpcError> {
        let account_value = self
            .get_value(&format!("/cosmos/auth/v1beta1/accounts/{address}"))
            .await?;

        let (account_number, sequence) =
            extract_account_numbers(&account_value).ok_or_else(|| {
                RpcError::Parse(
                    "Missing account_number/sequence in Cosmos account response".to_string(),
                )
            })?;

        let node_info = self
            .get_value("/cosmos/base/tendermint/v1beta1/node_info")
            .await?;

        let chain_id = node_info
            .get("default_node_info")
            .and_then(|value| value.get("network"))
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| RpcError::Parse("Missing Cosmos chain_id".to_string()))?;

        Ok(CosmosAccountState {
            account_number,
            sequence,
            chain_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::supported_cosmos_chain;

    #[test]
    fn derives_fee_amount_from_gas_price_metadata() {
        let axelar = supported_cosmos_chain("axelar").expect("axelar config");
        assert_eq!(axelar.fee_amount_base_units(), 10_000);
        assert!((axelar.network_fee_native() - 0.01).abs() < f64::EPSILON);

        let cheqd = supported_cosmos_chain("cheqd").expect("cheqd config");
        assert_eq!(cheqd.fee_amount_base_units(), 900_000_000);
        assert!((cheqd.network_fee_native() - 0.9).abs() < f64::EPSILON);

        let coreum = supported_cosmos_chain("coreum").expect("coreum config");
        assert_eq!(coreum.fee_amount_base_units(), 7_500);
        assert!((coreum.network_fee_native() - 0.0075).abs() < f64::EPSILON);

        let dydx = supported_cosmos_chain("dydx").expect("dydx config");
        assert_eq!(dydx.fee_amount_base_units(), 1_500_000_000_000_000);
        assert!((dydx.network_fee_native() - 0.0015).abs() < f64::EPSILON);

        let dymension = supported_cosmos_chain("dymension").expect("dymension config");
        assert_eq!(dymension.fee_amount_base_units(), 600_000_000_000_000);
        assert!((dymension.network_fee_native() - 0.0006).abs() < f64::EPSILON);

        let fetch = supported_cosmos_chain("fetch").expect("fetch config");
        assert_eq!(fetch.fee_amount_base_units(), 3_000);
        assert!((fetch.network_fee_native() - 0.000000000000003).abs() < f64::EPSILON);

        let initia = supported_cosmos_chain("initia").expect("initia config");
        assert_eq!(initia.fee_amount_base_units(), 1_800);
        assert!((initia.network_fee_native() - 0.0018).abs() < f64::EPSILON);

        let kyve = supported_cosmos_chain("kyve").expect("kyve config");
        assert_eq!(kyve.fee_amount_base_units(), 9_600_000);
        assert!((kyve.network_fee_native() - 9.6).abs() < f64::EPSILON);

        let neutron = supported_cosmos_chain("neutron").expect("neutron config");
        assert_eq!(neutron.fee_amount_base_units(), 636);
        assert!((neutron.network_fee_native() - 0.000636).abs() < f64::EPSILON);

        let secret = supported_cosmos_chain("secret").expect("secret config");
        assert_eq!(secret.fee_amount_base_units(), 25_000);
        assert!((secret.network_fee_native() - 0.025).abs() < f64::EPSILON);
    }
}
