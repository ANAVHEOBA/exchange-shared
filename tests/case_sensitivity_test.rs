// Quick test to verify case-insensitive network matching
#[path = "common/mod.rs"]
mod common;

use exchange_shared::services::wallet::derivation;

#[tokio::test]
async fn test_case_insensitive_network_matching() {
    let seed = common::test_wallet_mnemonic();

    // Test the 3 problematic networks from Trocador
    let test_cases = vec![
        ("Arbitrum", "arbitrum"), // Capital A in Trocador
        ("brc20", "BRC20"),       // lowercase in Trocador
        ("zano", "ZANO"),         // lowercase in Trocador
    ];

    println!("\nTesting case-insensitive network matching:\n");

    for (trocador_case, our_case) in test_cases {
        // Test with Trocador's case
        let result1 = derivation::derive_address(&seed, "TEST", trocador_case, 0).await;

        // Test with our case
        let result2 = derivation::derive_address(&seed, "TEST", our_case, 0).await;

        match (&result1, &result2) {
            (Ok(addr1), Ok(addr2)) => {
                assert_eq!(addr1, addr2, "Addresses should match regardless of case");
                println!(
                    "✅ '{}' and '{}' both work -> {}",
                    trocador_case,
                    our_case,
                    &addr1[..20.min(addr1.len())]
                );
            }
            (Err(e), _) => {
                panic!("❌ '{}' failed: {}", trocador_case, e);
            }
            (_, Err(e)) => {
                panic!("❌ '{}' failed: {}", our_case, e);
            }
        }
    }

    println!("\n✅ All case variations work correctly!\n");
}
