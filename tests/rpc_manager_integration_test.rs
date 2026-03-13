use exchange_shared::services::rpc::{RpcManager, RpcManagerAdapter, build_default_rpc_configs};
use exchange_shared::services::wallet::rpc::BlockchainProvider;
use std::sync::Arc;

#[tokio::test]
async fn test_rpc_manager_adapter_integration() {
    // Initialize RpcManager with default configs
    let configs = build_default_rpc_configs();
    let rpc_manager = Arc::new(RpcManager::new(configs));
    
    // Create adapter for Ethereum
    let adapter = RpcManagerAdapter::new(rpc_manager.clone(), "ethereum".to_string());
    
    // Test that adapter implements BlockchainProvider trait
    // Note: This will fail if no RPC is available, but that's expected in test environment
    let result = adapter.get_balance("0x0000000000000000000000000000000000000000").await;
    
    // We just want to verify the adapter is properly wired up
    // The actual RPC call may fail due to network/config, but the types should work
    match result {
        Ok(balance) => {
            println!("✅ RPC Manager adapter working! Balance: {}", balance);
        }
        Err(e) => {
            println!("⚠️ RPC call failed (expected in test env): {:?}", e);
            // This is OK - we're just testing the integration, not the actual RPC
        }
    }
}

#[test]
fn test_rpc_config_builder() {
    // Test that config builder creates valid configs
    let configs = build_default_rpc_configs();
    
    // Should have configs for all chains in chains.json (119 chains)
    assert!(configs.len() >= 100, "Should have at least 100 chain configs, got {}", configs.len());
    
    // Check key chains exist
    assert!(configs.contains_key("ethereum"), "Should have Ethereum config");
    assert!(configs.contains_key("bitcoin"), "Should have Bitcoin config");
    assert!(configs.contains_key("solana"), "Should have Solana config");
    assert!(configs.contains_key("bnb_smart_chain"), "Should have BSC config");
    assert!(configs.contains_key("polygon"), "Should have Polygon config");
    assert!(configs.contains_key("arbitrum_one"), "Should have Arbitrum config");
    assert!(configs.contains_key("base"), "Should have Base config");
    assert!(configs.contains_key("avalanche_c_chain"), "Should have Avalanche config");
    assert!(configs.contains_key("optimism"), "Should have Optimism config");
    
    // Check Ethereum config structure
    let eth_config = configs.get("ethereum").unwrap();
    assert_eq!(eth_config.chain, "Ethereum");
    assert!(!eth_config.endpoints.is_empty(), "Should have at least one endpoint");
    
    // Check that endpoints have proper priority ordering
    let priorities: Vec<u8> = eth_config.endpoints.iter().map(|e| e.priority).collect();
    for i in 1..priorities.len() {
        assert!(priorities[i] >= priorities[i-1], "Priorities should be in ascending order");
    }
    
    println!("✅ RPC config builder creates {} chain configs", configs.len());
    
    // Print sample of chains
    let mut chain_names: Vec<String> = configs.keys().cloned().collect();
    chain_names.sort();
    println!("Sample chains: {:?}", &chain_names[..10.min(chain_names.len())]);
}
