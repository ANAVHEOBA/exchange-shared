/// Test to verify Alchemy RPC integration works
/// 
/// This test checks:
/// 1. RPC configuration loads correctly
/// 2. Alchemy API key is used when available
/// 3. Fallback to public endpoints when no API key
/// 4. Can fetch gas price from Ethereum mainnet
/// 
/// To run this test:
/// ```bash
/// # Without Alchemy API key (uses public endpoints)
/// cargo test --test rpc_alchemy_test -- --nocapture
/// 
/// # With Alchemy API key (uses Alchemy)
/// ALCHEMY_API_KEY=your_key_here cargo test --test rpc_alchemy_test -- --nocapture
/// ```

use exchange_shared::config::rpc_config::{load_rpc_config, get_rpc_config, BlockchainProtocol};
use exchange_shared::services::wallet::rpc::{HttpRpcClient, BlockchainProvider};

#[tokio::test]
async fn test_rpc_config_loads() {
    println!("\n🔧 Testing RPC Configuration Loading...");
    
    let config = load_rpc_config();
    
    // Check that major chains are configured
    assert!(config.contains_key("ethereum"), "Ethereum config missing");
    assert!(config.contains_key("polygon"), "Polygon config missing");
    assert!(config.contains_key("arbitrum"), "Arbitrum config missing");
    assert!(config.contains_key("optimism"), "Optimism config missing");
    assert!(config.contains_key("base"), "Base config missing");
    assert!(config.contains_key("solana"), "Solana config missing");
    
    println!("✅ All major chains configured");
    println!("📊 Total chains configured: {}", config.len());
}

#[tokio::test]
async fn test_alchemy_api_key_integration() {
    println!("\n🔑 Testing Alchemy API Key Integration...");
    
    let eth_config = get_rpc_config("ethereum").expect("Ethereum config not found");
    
    // Check if Alchemy API key is set
    if let Ok(api_key) = std::env::var("ALCHEMY_API_KEY") {
        println!("✅ ALCHEMY_API_KEY found in environment");
        println!("🔑 API Key: {}...{}", &api_key[..8], &api_key[api_key.len()-4..]);
        
        // Verify the primary RPC uses Alchemy
        assert!(
            eth_config.primary.contains("alchemy.com"),
            "Primary RPC should use Alchemy when API key is set"
        );
        assert!(
            eth_config.primary.contains(&api_key),
            "Primary RPC should include the API key"
        );
        
        println!("✅ Primary RPC correctly uses Alchemy: {}", eth_config.primary);
    } else {
        println!("⚠️  ALCHEMY_API_KEY not set - using public endpoints");
        println!("💡 To test with Alchemy, run:");
        println!("   ALCHEMY_API_KEY=your_key cargo test --test rpc_alchemy_test");
        
        // Verify fallback to public endpoint
        assert!(
            !eth_config.primary.contains("alchemy.com") || eth_config.primary.contains("YOUR_ALCHEMY_KEY"),
            "Should use public endpoint when no API key"
        );
        
        println!("✅ Primary RPC correctly uses public endpoint: {}", eth_config.primary);
    }
    
    // Check fallbacks are configured
    assert!(!eth_config.fallbacks.is_empty(), "Fallback RPCs should be configured");
    println!("✅ Fallback RPCs configured: {} endpoints", eth_config.fallbacks.len());
    
    // Check protocol type
    assert_eq!(eth_config.protocol, BlockchainProtocol::EVM, "Ethereum should be EVM protocol");
    println!("✅ Protocol type: {:?}", eth_config.protocol);
}

#[tokio::test]
async fn test_fetch_ethereum_gas_price() {
    println!("\n⛽ Testing Real Gas Price Fetch from Ethereum...");
    
    let eth_config = get_rpc_config("ethereum").expect("Ethereum config not found");
    
    println!("🌐 Using RPC endpoint: {}", eth_config.primary);
    
    // Create RPC client
    let client = HttpRpcClient::new(eth_config.primary.clone());
    
    // Try to fetch gas price
    match client.get_gas_price().await {
        Ok(gas_price) => {
            println!("✅ Successfully fetched gas price!");
            println!("⛽ Gas Price: {} wei", gas_price);
            println!("⛽ Gas Price: {} gwei", gas_price as f64 / 1_000_000_000.0);
            
            // Sanity check: gas price should be reasonable (1 gwei to 1000 gwei)
            let gwei = gas_price as f64 / 1_000_000_000.0;
            assert!(gwei >= 0.1 && gwei <= 10000.0, "Gas price seems unreasonable: {} gwei", gwei);
            
            println!("✅ Gas price is within reasonable range");
        }
        Err(e) => {
            println!("❌ Failed to fetch gas price: {}", e);
            println!("💡 This might be due to:");
            println!("   - Network connectivity issues");
            println!("   - RPC endpoint rate limiting");
            println!("   - Invalid API key (if using Alchemy)");
            
            // Don't fail the test if it's a network issue
            if std::env::var("CI").is_ok() {
                println!("⚠️  Running in CI - skipping network test");
            } else {
                panic!("Failed to fetch gas price: {}", e);
            }
        }
    }
}

#[tokio::test]
async fn test_fetch_gas_price_with_fallback() {
    println!("\n🔄 Testing Gas Price Fetch with Fallback...");
    
    let eth_config = get_rpc_config("ethereum").expect("Ethereum config not found");
    
    // Try primary endpoint
    println!("🌐 Trying primary endpoint: {}", eth_config.primary);
    let primary_client = HttpRpcClient::new(eth_config.primary.clone());
    
    match primary_client.get_gas_price().await {
        Ok(gas_price) => {
            println!("✅ Primary endpoint succeeded!");
            println!("⛽ Gas Price: {} gwei", gas_price as f64 / 1_000_000_000.0);
        }
        Err(e) => {
            println!("⚠️  Primary endpoint failed: {}", e);
            println!("🔄 Trying fallback endpoints...");
            
            // Try fallback endpoints
            let mut success = false;
            for (i, fallback) in eth_config.fallbacks.iter().enumerate() {
                println!("🌐 Trying fallback #{}: {}", i + 1, fallback);
                let fallback_client = HttpRpcClient::new(fallback.clone());
                
                match fallback_client.get_gas_price().await {
                    Ok(gas_price) => {
                        println!("✅ Fallback #{} succeeded!", i + 1);
                        println!("⛽ Gas Price: {} gwei", gas_price as f64 / 1_000_000_000.0);
                        success = true;
                        break;
                    }
                    Err(e) => {
                        println!("❌ Fallback #{} failed: {}", i + 1, e);
                    }
                }
            }
            
            if !success && std::env::var("CI").is_err() {
                panic!("All RPC endpoints failed - check network connectivity");
            }
        }
    }
}

#[tokio::test]
async fn test_multiple_chains_gas_price() {
    println!("\n🌍 Testing Gas Price Fetch Across Multiple Chains...");
    
    let chains = vec![
        ("ethereum", "Ethereum"),
        ("polygon", "Polygon"),
        ("arbitrum", "Arbitrum"),
        ("optimism", "Optimism"),
        ("base", "Base"),
    ];
    
    for (chain_id, chain_name) in chains {
        println!("\n📡 Testing {}...", chain_name);
        
        if let Some(config) = get_rpc_config(chain_id) {
            println!("🌐 RPC: {}", config.primary);
            
            let client = HttpRpcClient::new(config.primary.clone());
            
            match client.get_gas_price().await {
                Ok(gas_price) => {
                    let gwei = gas_price as f64 / 1_000_000_000.0;
                    println!("✅ {} gas price: {:.2} gwei", chain_name, gwei);
                }
                Err(e) => {
                    println!("⚠️  {} failed: {}", chain_name, e);
                }
            }
        } else {
            println!("❌ {} config not found", chain_name);
        }
        
        // Small delay to avoid rate limiting
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

#[test]
fn test_alchemy_url_format() {
    println!("\n🔗 Testing Alchemy URL Format...");
    
    // Set a test API key
    std::env::set_var("ALCHEMY_API_KEY", "test_key_12345");
    
    let config = load_rpc_config();
    let eth_config = config.get("ethereum").expect("Ethereum config not found");
    
    // Verify URL format
    assert!(
        eth_config.primary.contains("eth-mainnet.g.alchemy.com/v2/test_key_12345"),
        "Alchemy URL format incorrect: {}",
        eth_config.primary
    );
    
    println!("✅ Alchemy URL format correct: {}", eth_config.primary);
    
    // Clean up
    std::env::remove_var("ALCHEMY_API_KEY");
}

#[test]
fn test_supported_chains() {
    println!("\n📋 Listing All Supported Chains...");
    
    let config = load_rpc_config();
    
    println!("\n🌐 EVM Chains:");
    for (name, endpoint) in config.iter() {
        if endpoint.protocol == BlockchainProtocol::EVM {
            let uses_alchemy = endpoint.primary.contains("alchemy.com");
            let status = if uses_alchemy { "🔑 Alchemy" } else { "🌍 Public" };
            println!("  {} - {} ({})", name, status, endpoint.chain_id.as_ref().unwrap_or(&"N/A".to_string()));
        }
    }
    
    println!("\n🔗 Non-EVM Chains:");
    for (name, endpoint) in config.iter() {
        if endpoint.protocol != BlockchainProtocol::EVM {
            let uses_alchemy = endpoint.primary.contains("alchemy.com");
            let status = if uses_alchemy { "🔑 Alchemy" } else { "🌍 Public" };
            println!("  {} - {} ({:?})", name, status, endpoint.protocol);
        }
    }
    
    println!("\n📊 Total: {} chains configured", config.len());
}
