// =============================================================================
// INTEGRATION TESTS - FAILURE RECOVERY
// Verifies retry logic and distributed locking
// =============================================================================

#[path = "../common/mod.rs"]
mod common;

use common::TestContext;
use uuid::Uuid;
use chrono::Utc;
use exchange_shared::services::monitor::MonitorEngine;
use exchange_shared::modules::monitor::model::PollingState;

#[tokio::test]
async fn test_worker_lock_prevents_concurrency() {
    let ctx = TestContext::new().await;
    let swap_id = Uuid::new_v4().to_string();
    let master_seed = "abandon ".repeat(11) + "about";
    
    // 1. Try to manually acquire the Redis lock for this swap
    let lock_key = format!("lock:monitor:{}", swap_id);
    
    // Check if Redis is available
    let lock_acquired = match ctx.redis.try_lock(&lock_key, 60).await {
        Ok(acquired) => acquired,
        Err(e) => {
            println!("⚠️  Redis not available: {}. Skipping lock test.", e);
            println!("   Start Redis with: redis-server");
            return; // Skip test if Redis is not running
        }
    };
    
    if !lock_acquired {
        println!("⚠️  Could not acquire lock. Skipping test.");
        return;
    }
    
    // 2. Try to process the poll via engine
    // Since we already hold the lock, the engine should skip it (return Ok)
    let engine = MonitorEngine::new(ctx.db.clone(), ctx.redis.clone(), master_seed);
    let state = PollingState {
        swap_id: swap_id.clone(),
        last_polled_at: None,
        next_poll_at: Utc::now(),
        poll_count: 0,
        last_status: "waiting".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };

    let result = engine.process_poll(state).await;
    
    // If the engine correctly respected the lock, it would have skipped the DB/API 
    // calls. We can verify this by checking that NO poll was recorded in DB.
    let poll_exists = sqlx::query("SELECT swap_id FROM polling_states WHERE swap_id = ?")
        .bind(&swap_id)
        .fetch_optional(&ctx.db)
        .await
        .unwrap();
    
    assert!(result.is_ok());
    assert!(poll_exists.is_none(), "Engine should have skipped processing because of lock");
    
    println!("✅ Distributed lock correctly prevents concurrent processing");
}