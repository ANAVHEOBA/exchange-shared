// Tests for Priority Tier 1 blockchain address derivation
// Cardano, Polkadot, Ripple, Tron, Cosmos
//
// CRITICAL: Each transaction MUST get a UNIQUE address (increment index)
// Reusing the same address across multiple swaps = fraud flag / AML trigger

use std::collections::HashSet;

#[cfg(test)]
mod priority_tier_1_blockchains {
    use super::*;

    #[tokio::test]
    async fn test_cardano_address_generation() {
        let mnemonic = crate::common::test_wallet_mnemonic();
        let addr = exchange_shared::services::wallet::derivation::derive_cardano_address(&mnemonic, 0)
            .await
            .expect("Cardano derivation failed");
        
        assert!(addr.starts_with("addr1"), "Cardano must start with addr1");
        println!("✓ Cardano: {}", addr);
    }

    #[tokio::test]
    async fn test_polkadot_address_generation() {
        let mnemonic = crate::common::test_wallet_mnemonic();
        let addr = exchange_shared::services::wallet::derivation::derive_polkadot_address(&mnemonic, 0)
            .await
            .expect("Polkadot derivation failed");
        
        assert!(addr.starts_with("1"), "Polkadot SS58 must start with 1");
        println!("✓ Polkadot: {}", addr);
    }

    #[tokio::test]
    async fn test_ripple_address_generation() {
        let mnemonic = crate::common::test_wallet_mnemonic();
        let addr = exchange_shared::services::wallet::derivation::derive_ripple_address(&mnemonic, 0)
            .await
            .expect("Ripple derivation failed");
        
        // Ripple address format: Base58Check encoded with version byte
        // Should be 25-35 characters and contain valid Base58 characters
        assert!(addr.len() >= 25, "Ripple address should be reasonably long");
        assert!(addr.len() <= 35, "Ripple address should not be too long");
        // Verify it's valid Base58 (no 0, O, I, l)
        for c in addr.chars() {
            assert!(!"0OIl".contains(c), "Ripple address contains invalid Base58 character");
        }
        println!("✓ Ripple: {}", addr);
    }

    #[tokio::test]
    async fn test_tron_address_generation() {
        let mnemonic = crate::common::test_wallet_mnemonic();
        let addr = exchange_shared::services::wallet::derivation::derive_tron_address(&mnemonic, 0)
            .await
            .expect("Tron derivation failed");
        
        assert!(addr.starts_with("T"), "Tron must start with T");
        assert_eq!(addr.len(), 34, "Tron address is 34 chars");
        println!("✓ Tron: {}", addr);
    }

    #[tokio::test]
    async fn test_cosmos_address_generation() {
        let mnemonic = crate::common::test_wallet_mnemonic();
        let addr = exchange_shared::services::wallet::derivation::derive_cosmos_address(&mnemonic, 0)
            .await
            .expect("Cosmos derivation failed");
        
        assert!(addr.starts_with("cosmos1"), "Cosmos must start with cosmos1");
        println!("✓ Cosmos: {}", addr);
    }

    #[tokio::test]
    async fn test_deterministic_same_index() {
        // CRITICAL: Same seed + same index = SAME address (deterministic)
        let mnemonic = crate::common::test_wallet_mnemonic();
        let cardano_1 = exchange_shared::services::wallet::derivation::derive_cardano_address(&mnemonic, 42).await.unwrap();
        let cardano_2 = exchange_shared::services::wallet::derivation::derive_cardano_address(&mnemonic, 42).await.unwrap();
        assert_eq!(cardano_1, cardano_2, "Must be deterministic");

        let polkadot_1 = exchange_shared::services::wallet::derivation::derive_polkadot_address(&mnemonic, 42).await.unwrap();
        let polkadot_2 = exchange_shared::services::wallet::derivation::derive_polkadot_address(&mnemonic, 42).await.unwrap();
        assert_eq!(polkadot_1, polkadot_2, "Must be deterministic");

        println!("✓ All addresses are deterministic");
    }

    #[tokio::test]
    async fn test_unique_per_index() {
        // CRITICAL: Each index gets DIFFERENT address
        // This prevents address reuse which triggers AML/fraud flags
        let mnemonic = crate::common::test_wallet_mnemonic();
        
        let mut cardano_set = HashSet::new();
        let mut polkadot_set = HashSet::new();
        let mut ripple_set = HashSet::new();
        let mut tron_set = HashSet::new();
        let mut cosmos_set = HashSet::new();

        for i in 0..10 {
            cardano_set.insert(exchange_shared::services::wallet::derivation::derive_cardano_address(&mnemonic, i).await.unwrap());
            polkadot_set.insert(exchange_shared::services::wallet::derivation::derive_polkadot_address(&mnemonic, i).await.unwrap());
            ripple_set.insert(exchange_shared::services::wallet::derivation::derive_ripple_address(&mnemonic, i).await.unwrap());
            tron_set.insert(exchange_shared::services::wallet::derivation::derive_tron_address(&mnemonic, i).await.unwrap());
            cosmos_set.insert(exchange_shared::services::wallet::derivation::derive_cosmos_address(&mnemonic, i).await.unwrap());
        }

        assert_eq!(cardano_set.len(), 10);
        assert_eq!(polkadot_set.len(), 10);
        assert_eq!(ripple_set.len(), 10);
        assert_eq!(tron_set.len(), 10);
        assert_eq!(cosmos_set.len(), 10);

        println!("✓ Each index produces unique address (prevents AML flagging)");
    }

    #[tokio::test]
    async fn test_dispatcher_aliases() {
        // Network aliases must work
        let mnemonic = crate::common::test_wallet_mnemonic();
        let c1 = exchange_shared::services::wallet::derivation::derive_address(&mnemonic, "ADA", "cardano", 0).await.unwrap();
        let c2 = exchange_shared::services::wallet::derivation::derive_address(&mnemonic, "ADA", "ada", 0).await.unwrap();
        assert_eq!(c1, c2);

        let p1 = exchange_shared::services::wallet::derivation::derive_address(&mnemonic, "DOT", "polkadot", 0).await.unwrap();
        let p2 = exchange_shared::services::wallet::derivation::derive_address(&mnemonic, "DOT", "dot", 0).await.unwrap();
        assert_eq!(p1, p2);

        println!("✓ All dispatcher aliases work");
    }
}
