#[path = "common/mod.rs"]
mod common;

use exchange_shared::services::wallet::derivation;

#[tokio::test]
async fn test_all_trocador_top_networks() {
    let seed = common::test_wallet_mnemonic();

    // Test top Trocador networks (using their exact case from API)
    let test_networks = vec![
        ("ERC20", "Ethereum tokens"),
        ("BEP20", "BSC tokens"),
        ("MAINNET", "Mainnet"),
        ("Mainnet", "Mainnet (mixed)"),
        ("SOL", "Solana"),
        ("ETH", "Ethereum"),
        ("MATIC", "Polygon"),
        ("BASE", "Base"),
        ("base", "Base (lower)"),
        ("ARBITRUM", "Arbitrum"),
        ("Arbitrum", "Arbitrum (mixed case)"),
        ("BSC", "BSC"),
        ("AVAXC", "Avalanche C-Chain"),
        ("TRC20", "Tron tokens"),
        ("TON", "TON"),
        ("SUI", "Sui"),
        ("OPTIMISM", "Optimism"),
        ("Optimism", "Optimism (mixed case)"),
        ("CHZ", "Chiliz"),
        ("KCC", "KCC"),
        ("BTC", "Bitcoin"),
        ("brc20", "BRC20 ordinals"),
        ("BRC20", "BRC20 ordinals (upper)"),
        ("zano", "Zano"),
        ("ZANO", "Zano (upper)"),
    ];

    println!("\n{}", "=".repeat(80));
    println!("TESTING TROCADOR NETWORKS WITH ACTUAL DERIVATION");
    println!("{}", "=".repeat(80));
    println!();

    let mut passed = 0;
    let mut failed_list = Vec::new();

    for (network, description) in test_networks {
        match derivation::derive_address(&seed, "TEST", network, 0).await {
            Ok(addr) => {
                passed += 1;
                println!(
                    "✅ {:<25} ({:<30}) -> {}",
                    network,
                    description,
                    &addr[..42.min(addr.len())]
                );
            }
            Err(e) => {
                failed_list.push((network, description, e));
                println!("❌ {:<25} ({:<30}) -> ERROR", network, description);
            }
        }
    }

    println!();
    println!("{}", "=".repeat(80));
    if failed_list.is_empty() {
        println!("✅ PERFECT! All {} networks work correctly!", passed);
        println!("{}", "=".repeat(80));
    } else {
        println!("❌ FAILED: {} passed, {} failed", passed, failed_list.len());
        println!("{}", "=".repeat(80));
        for (network, desc, err) in &failed_list {
            println!("  ❌ {} ({}): {}", network, desc, err);
        }
    }
    println!();

    assert!(failed_list.is_empty(), "Some networks failed derivation");
}
