use exchange_shared::services::wallet::derivation::derive_address;

#[tokio::test]
async fn test_monero_address_generation() {
    dotenvy::dotenv().ok();
    
    let mnemonic = std::env::var("WALLET_MNEMONIC")
        .expect("WALLET_MNEMONIC must be set");
    
    // Generate a Monero address
    let address = derive_address(&mnemonic, "xmr", "Mainnet", 0)
        .await
        .expect("Should generate XMR address");
    
    println!("Generated Monero address: {}", address);
    println!("Address length: {}", address.len());
    
    // Monero mainnet addresses start with '4' and are 95 characters
    assert!(address.starts_with('4'), "Monero mainnet address should start with '4'");
    assert_eq!(address.len(), 95, "Monero address should be 95 characters");
    
    // Validate with Trocador
    let api_key = std::env::var("TROCADOR_API_KEY")
        .expect("TROCADOR_API_KEY must be set");
    
    let client = exchange_shared::services::trocador::TrocadorClient::new(api_key);
    let is_valid = client.validate_address("xmr", "Mainnet", &address)
        .await
        .expect("Should validate address");
    
    println!("Trocador validation result: {}", is_valid);
    assert!(is_valid, "Generated Monero address should be valid according to Trocador");
}
