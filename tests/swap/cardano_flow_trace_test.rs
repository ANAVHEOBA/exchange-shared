use crate::common::TestContext;
use exchange_shared::modules::wallet::crud::WalletCrud;
use serial_test::serial;
use tokio::time::{sleep, Duration};

use super::live_flow_trace_test::{execute_flow_round, FlowScenario};

const ADA_RECIPIENT: &str = "addr1v9dd3gtv6je555fpdjwma8f98qqy492lky2n08c7ftslyeg89jvu8";
const BTC_REFUND_ADDRESS: &str = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh";

#[serial]
#[tokio::test]
#[ignore = "Requires TROCADOR_API_KEY, database, Redis, and network access; traces the provider-managed Cardano payout flow end to end"]
async fn cardano_provider_managed_flow_trace_live() {
    dotenvy::dotenv().ok();

    let ctx = TestContext::new().await;
    let wallet_crud = WalletCrud::new(ctx.db.clone());

    let scenario = FlowScenario {
        label: "Cardano Provider Managed Payout",
        from: "btc",
        network_from: "Mainnet",
        to: "ada",
        network_to: "Mainnet",
        amount: 0.005,
        recipient_address: ADA_RECIPIENT,
        refund_address: BTC_REFUND_ADDRESS,
        expected_direct_settlement: false,
    };

    let trace = execute_flow_round(&ctx, &wallet_crud, &scenario, 1).await;

    println!("ASSERTIONS");
    println!("  swap id: {}", trace.swap_id);
    println!("  provider swap id: {}", trace.provider_swap_id);
    println!("  quote provider: {}", trace.rate_provider);
    println!("  created trade provider: {}", trace.provider);
    println!("  deposit address: {}", trace.deposit_address);
    println!("  user recipient: {}", trace.recipient_address);
    println!(
        "  Trocador address_provider: {}",
        trace.trocador_address_provider
    );
    println!("  Trocador address_user: {}", trace.trocador_address_user);
    println!(
        "  internal payout address: {}",
        trace.internal_payout_address.as_deref().unwrap_or("<none>")
    );

    assert!(trace.internal_payout_address.is_none());
    assert!(trace.stored_recipient_address.is_none());
    assert_eq!(trace.trocador_address_user, ADA_RECIPIENT);

    sleep(Duration::from_secs(2)).await;
    ctx.cleanup().await;
}
