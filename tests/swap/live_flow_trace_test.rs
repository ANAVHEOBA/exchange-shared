use crate::common::{timed_get, timed_post, TestContext};
use exchange_shared::modules::wallet::crud::WalletCrud;
use serde_json::{json, Value};
use serial_test::serial;
use tokio::time::{sleep, Duration};

pub(crate) const DEFAULT_FLOW_ROUNDS: usize = 1;

pub(crate) struct FlowScenario {
    pub(crate) label: &'static str,
    pub(crate) from: &'static str,
    pub(crate) network_from: &'static str,
    pub(crate) to: &'static str,
    pub(crate) network_to: &'static str,
    pub(crate) amount: f64,
    pub(crate) recipient_address: &'static str,
    pub(crate) refund_address: &'static str,
    pub(crate) expected_direct_settlement: bool,
}

pub(crate) struct FlowTrace {
    pub(crate) swap_id: String,
    pub(crate) provider_swap_id: String,
    pub(crate) deposit_address: String,
    pub(crate) recipient_address: String,
    pub(crate) internal_payout_address: Option<String>,
    pub(crate) stored_recipient_address: Option<String>,
    pub(crate) trocador_address_user: String,
    pub(crate) trocador_address_provider: String,
    pub(crate) provider: String,
    pub(crate) rate_provider: String,
}

pub(crate) async fn execute_flow_round(
    ctx: &TestContext,
    wallet_crud: &WalletCrud,
    scenario: &FlowScenario,
    round: usize,
) -> FlowTrace {
    println!();
    println!(
        "================ FLOW ROUND {}: {} ================",
        round, scenario.label
    );
    println!("USER");
    println!(
        "  wants to swap {} {} on {} into {} on {}",
        scenario.amount, scenario.from, scenario.network_from, scenario.to, scenario.network_to
    );
    println!("  enters recipient address {}", scenario.recipient_address);
    println!("  enters refund address {}", scenario.refund_address);

    let rates_path = format!(
        "/swap/rates?from={}&to={}&amount={}&network_from={}&network_to={}",
        scenario.from, scenario.to, scenario.amount, scenario.network_from, scenario.network_to
    );

    println!("PLATFORM");
    println!("  asks Trocador for a live quote via {}", rates_path);
    let rate_response = timed_get(&ctx.server, &rates_path).await;
    rate_response.assert_status_ok();

    let rate_json: Value = rate_response.json();
    let trade_id = rate_json["trade_id"]
        .as_str()
        .expect("rate response should include trade_id")
        .to_string();
    let rate_provider = rate_json["rates"][0]["provider"]
        .as_str()
        .expect("rate response should include provider")
        .to_string();
    let quoted_receive = rate_json["rates"][0]["estimated_amount"]
        .as_f64()
        .expect("rate response should include estimated_amount");

    println!("TROCADOR");
    println!("  returned trade_id {}", trade_id);
    println!("  best provider for this quote is {}", rate_provider);
    println!("  estimated payout is {}", quoted_receive);

    let create_payload = json!({
        "trade_id": trade_id,
        "from": scenario.from,
        "network_from": scenario.network_from,
        "to": scenario.to,
        "network_to": scenario.network_to,
        "amount": scenario.amount,
        "provider": rate_provider,
        "recipient_address": scenario.recipient_address,
        "refund_address": scenario.refund_address,
        "rate_type": "floating"
    });

    println!("USER");
    println!("  confirms the swap and submits /swap/create");
    println!("PLATFORM");
    println!("  validates the recipient locally and with Trocador");
    println!("  decides whether payout will be direct-settlement or provider-managed");
    let create_response = timed_post(&ctx.server, "/swap/create", &create_payload).await;
    create_response.assert_status_success();

    let create_json: Value = create_response.json();
    let swap_id = create_json["swap_id"]
        .as_str()
        .expect("create response should include swap_id")
        .to_string();
    let deposit_address = create_json["deposit_address"]
        .as_str()
        .expect("create response should include deposit_address")
        .to_string();
    let recipient_address = create_json["recipient_address"]
        .as_str()
        .expect("create response should include recipient_address")
        .to_string();
    let provider = create_json["provider"]
        .as_str()
        .expect("create response should include provider")
        .to_string();

    println!("PLATFORM");
    println!("  created swap {}", swap_id);
    println!("  returned deposit address {} to the user", deposit_address);
    println!(
        "  still shows the user recipient address as {}",
        recipient_address
    );

    let status_path = format!("/swap/{}", swap_id);
    println!("PLATFORM");
    println!("  fetches live swap status via {}", status_path);
    let status_response = timed_get(&ctx.server, &status_path).await;
    status_response.assert_status_ok();

    let status_json: Value = status_response.json();
    let provider_swap_id = status_json["provider_swap_id"]
        .as_str()
        .expect("status response should include provider_swap_id")
        .to_string();

    println!("PLATFORM");
    println!("  provider swap id stored as {}", provider_swap_id);

    let address_info = wallet_crud
        .get_address_info(&swap_id)
        .await
        .expect("wallet lookup should succeed");

    let trade = fetch_trade_status_payload(&provider_swap_id).await;
    let trade_id = trade_string_field(&trade, "trade_id");
    let address_provider = trade_string_field(&trade, "address_provider");
    let address_user = trade_string_field(&trade, "address_user");

    println!("TROCADOR");
    println!(
        "  trade {} currently points user deposits to {}",
        trade_id, address_provider
    );
    println!("  trade {} will pay out to {}", trade_id, address_user);

    assert_eq!(
        address_provider, deposit_address,
        "deposit address returned to the user must match Trocador's provider deposit address"
    );
    assert_eq!(
        recipient_address, scenario.recipient_address,
        "platform must echo the user's requested recipient address"
    );
    assert_eq!(
        status_json["recipient_address"].as_str().unwrap(),
        scenario.recipient_address,
        "status endpoint must preserve the user's recipient address"
    );

    match (scenario.expected_direct_settlement, address_info) {
        (true, Some(info)) => {
            println!("PLATFORM");
            println!(
                "  stored internal payout address {} for second-hop settlement",
                info.our_address
            );
            println!(
                "  stored user recipient address {} alongside the swap",
                info.recipient_address
            );

            assert_eq!(
                info.recipient_address, scenario.recipient_address,
                "direct-settlement swaps must keep the user's real recipient address in swap_address_info"
            );
            assert_eq!(
                address_user, info.our_address,
                "direct-settlement swaps must send our internal payout address to Trocador"
            );

            FlowTrace {
                swap_id,
                provider_swap_id,
                deposit_address,
                recipient_address,
                internal_payout_address: Some(info.our_address),
                stored_recipient_address: Some(info.recipient_address),
                trocador_address_user: address_user.to_string(),
                trocador_address_provider: address_provider.to_string(),
                provider,
                rate_provider,
            }
        }
        (true, None) => {
            panic!(
                "expected direct settlement to create swap_address_info for {}",
                swap_id
            )
        }
        (false, None) => {
            println!("PLATFORM");
            println!("  did not allocate an internal payout address");
            println!("  delegated the payout destination directly to Trocador");

            assert_eq!(
                address_user, scenario.recipient_address,
                "provider-managed swaps must send the user's recipient address straight to Trocador"
            );

            FlowTrace {
                swap_id,
                provider_swap_id,
                deposit_address,
                recipient_address,
                internal_payout_address: None,
                stored_recipient_address: None,
                trocador_address_user: address_user.to_string(),
                trocador_address_provider: address_provider.to_string(),
                provider,
                rate_provider,
            }
        }
        (false, Some(info)) => {
            panic!(
                "expected provider-managed fallback without internal payout address, but found {} for {}",
                info.our_address, swap_id
            )
        }
    }
}

pub(crate) async fn fetch_trade_status_payload(trade_id: &str) -> Value {
    let api_key = std::env::var("TROCADOR_API_KEY").expect("TROCADOR_API_KEY must be set");
    let response = reqwest::Client::new()
        .get("https://api.trocador.app/trade")
        .header("API-Key", api_key)
        .query(&[("id", trade_id)])
        .send()
        .await
        .expect("trocador trade request should succeed");

    let status = response.status();
    let response_text = response
        .text()
        .await
        .expect("trocador trade response body should be readable");

    println!("TROCADOR raw /trade payload: {}", response_text);

    assert!(
        status.is_success(),
        "expected successful Trocador /trade response, got {} with body {}",
        status,
        response_text
    );

    serde_json::from_str(&response_text).expect("trocador trade payload should be JSON")
}

pub(crate) fn trade_string_field<'a>(payload: &'a Value, key: &str) -> &'a str {
    let payload = match payload {
        Value::Array(items) => items
            .first()
            .unwrap_or_else(|| panic!("trade payload array was empty: {}", payload)),
        other => other,
    };

    payload
        .get(key)
        .or_else(|| payload.get("trade").and_then(|trade| trade.get(key)))
        .or_else(|| payload.get("result").and_then(|trade| trade.get(key)))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            panic!(
                "missing string field '{}' in trade payload: {}",
                key, payload
            )
        })
}

pub(crate) fn read_rounds() -> usize {
    std::env::var("LIVE_SWAP_FLOW_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_FLOW_ROUNDS)
}

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY, WALLET_MNEMONIC, database, Redis, and network access; traces live direct and fallback swap creation flows end to end"]
async fn live_swap_create_flow_trace_provider_managed_fallback() {
    dotenvy::dotenv().ok();

    let rounds = read_rounds();
    let ctx = TestContext::new().await;
    let wallet_crud = WalletCrud::new(ctx.db.clone());

    let fallback_scenario = FlowScenario {
        label: "Provider Managed ADA Payout",
        from: "btc",
        network_from: "Mainnet",
        to: "ada",
        network_to: "Mainnet",
        amount: 0.005,
        recipient_address: "addr1v9dd3gtv6je555fpdjwma8f98qqy492lky2n08c7ftslyeg89jvu8",
        refund_address: "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
        expected_direct_settlement: false,
    };

    for round in 1..=rounds {
        let fallback_trace =
            execute_flow_round(&ctx, &wallet_crud, &fallback_scenario, round).await;

        println!("ASSERTIONS");
        println!("  swap id: {}", fallback_trace.swap_id);
        println!("  provider swap id: {}", fallback_trace.provider_swap_id);
        println!("  quote provider: {}", fallback_trace.rate_provider);
        println!("  created trade provider: {}", fallback_trace.provider);
        println!("  deposit address: {}", fallback_trace.deposit_address);
        println!(
            "  Trocador address_provider: {}",
            fallback_trace.trocador_address_provider
        );
        println!("  user recipient: {}", fallback_trace.recipient_address);
        println!(
            "  internal payout address: {}",
            fallback_trace
                .internal_payout_address
                .as_deref()
                .unwrap_or("<none>")
        );
        println!(
            "  stored recipient in DB: {}",
            fallback_trace
                .stored_recipient_address
                .as_deref()
                .unwrap_or("<none>")
        );
        println!(
            "  Trocador address_user: {}",
            fallback_trace.trocador_address_user
        );

        sleep(Duration::from_secs(2)).await;
    }

    ctx.cleanup().await;
}
