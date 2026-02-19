#!/bin/bash
# Script to run swap tests sequentially to avoid rate limiting

echo "🧪 Running swap tests sequentially to avoid Trocador API rate limits..."
echo "This will take ~5-10 minutes but ensures all tests pass."
echo ""

# Clear Redis cache before running
echo "🗑️  Clearing Redis cache..."
redis-cli FLUSHDB > /dev/null

echo "▶️  Running tests..."
echo ""

# Run tests with single thread (sequential execution)
cargo test --test swap_tests -- --test-threads=1

echo ""
echo "✅ Tests complete!"
