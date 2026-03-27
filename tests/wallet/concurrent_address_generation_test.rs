use exchange_shared::config::database::init_db;
use exchange_shared::modules::wallet::crud::WalletCrud;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_concurrent_address_index_generation() {
    // This test verifies that concurrent calls to get_next_index()
    // return unique indices without race conditions

    dotenvy::dotenv().ok();

    let pool = init_db().await;
    let wallet_crud = Arc::new(WalletCrud::new(pool.clone()));

    // Create 100 concurrent tasks that each request an address index
    let mut join_set = JoinSet::new();
    let num_concurrent = 100;

    for _ in 0..num_concurrent {
        let crud = Arc::clone(&wallet_crud);
        join_set.spawn(async move { crud.get_next_index().await.expect("Failed to get index") });
    }

    // Collect all indices
    let mut indices = HashSet::new();
    while let Some(result) = join_set.join_next().await {
        let index = result.expect("Task panicked");
        indices.insert(index);
    }

    // Verify all indices are unique
    assert_eq!(
        indices.len(),
        num_concurrent,
        "Expected {} unique indices, got {}. Duplicate indices detected!",
        num_concurrent,
        indices.len()
    );

    println!(
        "✓ All {} concurrent address index generations returned unique values",
        num_concurrent
    );
}
