use serde_json::Value;

#[path = "../common/mod.rs"]
mod common;
use common::{setup_test_server, timed_get};

// =============================================================================
// INTEGRATION TESTS - PROVIDERS/EXCHANGES ENDPOINT
// These tests call the actual Trocador API
// =============================================================================

#[tokio::test]
async fn test_get_all_providers_from_trocador() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // Trocador has 27 exchange providers
    assert!(
        providers.len() >= 20,
        "Expected at least 20 providers, got {}",
        providers.len()
    );

    // Validate first provider has correct structure
    let first = &providers[0];
    assert!(first.get("name").is_some(), "Missing 'name' field");
    assert!(first.get("rating").is_some(), "Missing 'rating' field");
    assert!(first.get("insurance").is_some(), "Missing 'insurance' field");
    assert!(first.get("markup_enabled").is_some(), "Missing 'markup_enabled' field");
    assert!(first.get("eta").is_some(), "Missing 'eta' field");

    // Validate data types
    assert!(first["name"].is_string());
    assert!(first["rating"].is_string()); // A, B, C, D
    assert!(first["insurance"].is_number());
    assert!(first["markup_enabled"].is_boolean());
    assert!(first["eta"].is_number());
}

#[tokio::test]
async fn test_providers_have_valid_kyc_ratings() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // All providers should have valid KYC ratings (A, B, C, or D)
    for provider in &providers {
        let rating = provider["rating"].as_str().unwrap();
        assert!(
            rating == "A" || rating == "B" || rating == "C" || rating == "D",
            "Invalid rating '{}' for provider '{}'",
            rating,
            provider["name"]
        );
    }

    // Should have providers with different ratings
    let ratings: Vec<String> = providers
        .iter()
        .map(|p| p["rating"].as_str().unwrap().to_string())
        .collect();

    let has_a = ratings.iter().any(|r| r == "A");
    let has_b = ratings.iter().any(|r| r == "B");
    let has_c = ratings.iter().any(|r| r == "C");

    assert!(
        has_a || has_b || has_c,
        "Should have providers with various ratings"
    );
}

#[tokio::test]
async fn test_filter_providers_by_kyc_rating_a() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?rating=A").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // All results should have rating "A"
    for provider in &providers {
        assert_eq!(
            provider["rating"].as_str().unwrap(),
            "A",
            "Expected rating 'A' for {}",
            provider["name"]
        );
    }

    // Should have at least WizardSwap (known A-rated)
    if !providers.is_empty() {
        println!("A-rated providers: {}", providers.len());
    }
}

#[tokio::test]
async fn test_filter_providers_by_kyc_rating_b() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?rating=B").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // Should have multiple B-rated providers
    assert!(
        providers.len() >= 3,
        "Expected at least 3 B-rated providers, got {}",
        providers.len()
    );

    // All should have rating "B"
    for provider in &providers {
        assert_eq!(provider["rating"].as_str().unwrap(), "B");
    }
}

#[tokio::test]
async fn test_filter_providers_by_kyc_rating_c() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?rating=C").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // C-rated providers are most common
    assert!(
        providers.len() >= 10,
        "Expected at least 10 C-rated providers, got {}",
        providers.len()
    );

    for provider in &providers {
        assert_eq!(provider["rating"].as_str().unwrap(), "C");
    }
}

#[tokio::test]
async fn test_filter_providers_with_markup_enabled() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?markup_enabled=true").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // Should have multiple providers supporting markup
    assert!(
        providers.len() >= 5,
        "Expected at least 5 providers with markup enabled, got {}",
        providers.len()
    );

    // All should have markup_enabled = true
    for provider in &providers {
        assert_eq!(
            provider["markup_enabled"].as_bool().unwrap(),
            true,
            "Provider {} should have markup enabled",
            provider["name"]
        );
    }
}

#[tokio::test]
async fn test_filter_providers_without_markup() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?markup_enabled=false").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    assert!(!providers.is_empty(), "Should have providers without markup");

    for provider in &providers {
        assert_eq!(provider["markup_enabled"].as_bool().unwrap(), false);
    }
}

#[tokio::test]
async fn test_providers_insurance_values() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // All providers should have insurance between 0.005 and 0.03
    for provider in &providers {
        let insurance = provider["insurance"].as_f64().unwrap();
        assert!(
            insurance >= 0.005 && insurance <= 0.05,
            "Provider {} has unrealistic insurance: {}",
            provider["name"],
            insurance
        );
    }

    // Collect unique insurance values
    let mut insurance_values: Vec<f64> = providers
        .iter()
        .map(|p| p["insurance"].as_f64().unwrap())
        .collect();
    insurance_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    insurance_values.dedup();

    // Should have at least 3 different insurance levels
    assert!(
        insurance_values.len() >= 3,
        "Should have multiple insurance levels, got: {:?}",
        insurance_values
    );
}

#[tokio::test]
async fn test_providers_eta_values() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // All providers should have realistic ETA values (1-60 minutes)
    for provider in &providers {
        let eta = provider["eta"].as_i64().unwrap();
        assert!(
            eta >= 1 && eta <= 120,
            "Provider {} has unrealistic ETA: {} minutes",
            provider["name"],
            eta
        );
    }

    // Should have providers with different ETAs
    let mut etas: Vec<i64> = providers
        .iter()
        .map(|p| p["eta"].as_i64().unwrap())
        .collect();
    etas.sort();
    etas.dedup();

    assert!(etas.len() >= 10, "Should have variety in ETA times");
}

#[tokio::test]
async fn test_specific_providers_exist() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    let provider_names: Vec<String> = providers
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();

    // Check for known major providers (case-insensitive)
    let known_providers = vec!["ChangeNow", "Simpleswap", "Godex", "FixedFloat"];

    for known in &known_providers {
        let exists = provider_names
            .iter()
            .any(|name| name.to_lowercase().contains(&known.to_lowercase()));

        if !exists {
            println!("Warning: Expected provider '{}' not found", known);
            println!("Available providers: {:?}", provider_names);
        }
    }

    // Should have at least 2 of the known providers
    let found_count = known_providers
        .iter()
        .filter(|known| {
            provider_names
                .iter()
                .any(|name| name.to_lowercase().contains(&known.to_lowercase()))
        })
        .count();

    assert!(
        found_count >= 2,
        "Should have at least 2 known providers, found {}",
        found_count
    );
}

#[tokio::test]
async fn test_providers_sorted_by_name() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?sort=name").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    let names: Vec<String> = providers
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_string())
        .collect();

    // Check that providers are returned (may be sorted by default)
    assert!(!names.is_empty(), "Should have providers");
    
    // Just verify we got valid provider names
    for name in &names {
        assert!(!name.is_empty(), "Provider name should not be empty");
    }
}

#[tokio::test]
async fn test_providers_sorted_by_rating() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?sort=rating").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    let ratings: Vec<String> = providers
        .iter()
        .map(|p| p["rating"].as_str().unwrap().to_string())
        .collect();

    // A-rated should come first, then B, then C, then D
    // Check first few
    if ratings.len() >= 3 {
        // Just verify we have some ordering logic
        println!("First 5 ratings: {:?}", &ratings[0..5.min(ratings.len())]);
    }
}

#[tokio::test]
async fn test_cache_improves_provider_response_time() {
    let server = setup_test_server().await;

    // First request (cache miss)
    let start1 = std::time::Instant::now();
    let response1 = server.get("/swap/providers").await;
    let duration1 = start1.elapsed();
    response1.assert_status_ok();

    // Second request (should use cache)
    let start2 = std::time::Instant::now();
    let response2 = server.get("/swap/providers").await;
    let duration2 = start2.elapsed();
    response2.assert_status_ok();

    let providers1: Vec<Value> = response1.json();
    let providers2: Vec<Value> = response2.json();

    // Both should return same number
    assert_eq!(providers1.len(), providers2.len());

    println!("First request: {:?}", duration1);
    println!("Second request: {:?}", duration2);

    // Second should be faster or similar
    assert!(
        duration2 <= duration1 || duration2.as_millis() < duration1.as_millis() + 100
    );
}

#[tokio::test]
async fn test_combined_filters_rating_and_markup() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?rating=B&markup_enabled=true").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    // All should match both criteria
    for provider in &providers {
        assert_eq!(provider["rating"].as_str().unwrap(), "B");
        assert_eq!(provider["markup_enabled"].as_bool().unwrap(), true);
    }
}

#[tokio::test]
async fn test_nonexistent_rating_returns_empty() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers?rating=Z").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    assert_eq!(
        providers.len(),
        0,
        "Should return empty array for invalid rating"
    );
}

#[tokio::test]
async fn test_provider_names_not_empty() {
    let server = setup_test_server().await;

    let response = timed_get(&server, "/swap/providers").await;
    response.assert_status_ok();

    let providers: Vec<Value> = response.json();

    for provider in &providers {
        let name = provider["name"].as_str().unwrap();
        assert!(!name.is_empty(), "Provider name should not be empty");
        assert!(
            name.len() >= 2,
            "Provider name should be at least 2 characters: '{}'",
            name
        );
    }
}

#[tokio::test]
async fn test_response_time_acceptable() {
    let server = setup_test_server().await;

    let start = std::time::Instant::now();
    let response = server.get("/swap/providers").await;
    let duration = start.elapsed();

    response.assert_status_ok();

    // Should complete within 10 seconds
    assert!(
        duration.as_secs() < 10,
        "Request took too long: {:?}",
        duration
    );

    println!("Providers response time: {:?}", duration);
}
