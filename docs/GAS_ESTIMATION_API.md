# Gas Estimation API Documentation

## Overview

The Gas Estimation service provides real-time, multi-chain gas price estimation with optimal mathematical strategies including EMA smoothing and multi-tier caching.

## Quick Start

```rust
use exchange_shared::services::gas::{GasEstimator, TxType};

// Initialize estimator
let estimator = GasEstimator::new(Some(redis_service));

// Get gas cost (simple)
let cost = estimator.get_gas_cost_for_network("ethereum").await;
println!("Gas cost: {} ETH", cost);

// Get detailed estimate
let estimate = estimator.estimate_gas("ethereum", TxType::TokenTransfer).await?;
println!("Gas price: {} wei", estimate.gas_price_wei);
println!("Gas limit: {}", estimate.gas_limit);
println!("Total cost: {} ETH", estimate.total_cost_native);
```

## API Reference

### `GasEstimator`

Main service for gas price estimation.

#### Constructor

```rust
pub fn new(redis_service: Option<RedisService>) -> Self
```

**Parameters:**
- `redis_service`: Optional Redis service for caching. If `None`, caching is disabled but estimation still works.

**Example:**
```rust
// With caching
let estimator = GasEstimator::new(Some(redis_service));

// Without caching (fallback mode)
let estimator = GasEstimator::new(None);
```

#### Methods

##### `estimate_gas`

Get detailed gas estimate for a specific network and transaction type.

```rust
pub async fn estimate_gas(
    &self,
    network: &str,
    tx_type: TxType,
) -> Result<GasEstimate, GasError>
```

**Parameters:**
- `network`: Network name (e.g., "ethereum", "polygon", "bitcoin")
- `tx_type`: Transaction type (see `TxType` enum)

**Returns:**
- `Ok(GasEstimate)`: Detailed gas estimate
- `Err(GasError)`: Error if estimation fails

**Example:**
```rust
let estimate = estimator.estimate_gas("ethereum", TxType::TokenTransfer).await?;

println!("Network: {}", estimate.network);
println!("Gas Price: {} wei", estimate.gas_price_wei);
println!("Gas Limit: {}", estimate.gas_limit);
println!("Total Cost: {} ETH", estimate.total_cost_native);
println!("Cached: {}", estimate.cached);
println!("Timestamp: {}", estimate.timestamp);
```

##### `get_gas_cost_for_network`

Simple method to get gas cost in native tokens (backward compatible).

```rust
pub async fn get_gas_cost_for_network(&self, network: &str) -> f64
```

**Parameters:**
- `network`: Network name

**Returns:**
- Gas cost in native tokens (ETH, BTC, SOL, etc.)

**Example:**
```rust
let eth_cost = estimator.get_gas_cost_for_network("ethereum").await;
let btc_cost = estimator.get_gas_cost_for_network("bitcoin").await;
let sol_cost = estimator.get_gas_cost_for_network("solana").await;
```

### `TxType`

Transaction types with different gas requirements.

```rust
pub enum TxType {
    NativeTransfer,      // Native token transfer
    TokenTransfer,       // ERC20/Token transfer
    TokenApprove,        // ERC20 approve operation
    ComplexContract,     // Complex contract interaction
}
```

#### Gas Limits (EVM Chains)

| Transaction Type | Gas Limit | Description |
|-----------------|-----------|-------------|
| `NativeTransfer` | 21,000 | ETH/native token transfer |
| `TokenTransfer` | 65,000 | ERC20 token transfer |
| `TokenApprove` | 45,000 | ERC20 approve operation |
| `ComplexContract` | 150,000 | Complex contract calls |

**Example:**
```rust
// Get gas limit for transaction type
let gas_limit = TxType::NativeTransfer.evm_gas_limit();
assert_eq!(gas_limit, 21_000);
```

### `GasEstimate`

Detailed gas estimate result.

```rust
pub struct GasEstimate {
    pub network: String,              // Network name
    pub tx_type: TxType,              // Transaction type
    pub gas_price_wei: u64,           // Gas price in wei (EVM) or smallest unit
    pub gas_limit: u64,               // Gas limit (EVM chains)
    pub total_cost_native: f64,       // Total cost in native token
    pub cached: bool,                 // Whether from cache
    pub timestamp: DateTime<Utc>,     // Timestamp of estimate
}
```

**Example:**
```rust
let estimate = estimator.estimate_gas("ethereum", TxType::NativeTransfer).await?;

// Access fields
println!("Network: {}", estimate.network);
println!("Gas Price: {} gwei", estimate.gas_price_wei / 1_000_000_000);
println!("Gas Limit: {}", estimate.gas_limit);
println!("Total Cost: {:.6} ETH", estimate.total_cost_native);

// Check if cached
if estimate.cached {
    println!("Served from cache (fast!)");
} else {
    println!("Fetched from RPC (slower)");
}
```

### `GasError`

Error types for gas estimation.

```rust
pub enum GasError {
    Rpc(String),                    // RPC error
    UnsupportedNetwork(String),     // Network not supported
    Cache(String),                  // Cache error
    Parse(String),                  // Parse error
}
```

## Supported Networks

### EVM Chains (20+)

| Network | Name | Gas Token | Formula |
|---------|------|-----------|---------|
| `ethereum` | Ethereum Mainnet | ETH | gasLimit × gasPrice / 1e18 |
| `polygon` | Polygon | MATIC | gasLimit × gasPrice / 1e18 |
| `bsc` | Binance Smart Chain | BNB | gasLimit × gasPrice / 1e18 |
| `arbitrum` | Arbitrum One | ETH | gasLimit × gasPrice / 1e18 |
| `optimism` | Optimism | ETH | gasLimit × gasPrice / 1e18 |
| `base` | Base | ETH | gasLimit × gasPrice / 1e18 |
| `avalanche` | Avalanche C-Chain | AVAX | gasLimit × gasPrice / 1e18 |
| `fantom` | Fantom | FTM | gasLimit × gasPrice / 1e18 |
| `gnosis` | Gnosis Chain | xDAI | gasLimit × gasPrice / 1e18 |
| `cronos` | Cronos | CRO | gasLimit × gasPrice / 1e18 |
| `zksync` | zkSync Era | ETH | gasLimit × gasPrice / 1e18 |
| `linea` | Linea | ETH | gasLimit × gasPrice / 1e18 |
| `scroll` | Scroll | ETH | gasLimit × gasPrice / 1e18 |
| `mantle` | Mantle | MNT | gasLimit × gasPrice / 1e18 |
| `blast` | Blast | ETH | gasLimit × gasPrice / 1e18 |

### Non-EVM Chains

| Network | Name | Fee Token | Formula |
|---------|------|-----------|---------|
| `bitcoin` | Bitcoin | BTC | feeRate × txSize |
| `solana` | Solana | SOL | 5,000 lamports + priority |

## Performance Characteristics

### Latency

| Scenario | Latency | Description |
|----------|---------|-------------|
| Cache Hit | <5ms | Served from Redis |
| Cache Miss | ~300ms | RPC call + EMA calculation |
| RPC Failure | <1ms | Fallback to hardcoded values |

### Cache Strategy

```
┌─────────────────────────────────────────┐
│  Tier 1: Exact Cache (10s TTL)         │
│  Key: gas:{network}:{tx_type}:estimate │
│  Hit Rate: 90%+                         │
├─────────────────────────────────────────┤
│  Tier 2: EMA Cache (60s TTL)           │
│  Key: gas:{network}:ema                │
│  Purpose: Smoothing volatility          │
└─────────────────────────────────────────┘
```

### RPC Load Reduction

- **Without caching**: 1 RPC call per request
- **With caching**: ~6 RPC calls per minute per network
- **Reduction**: 90%+ fewer RPC calls

## Mathematical Optimizations

### EMA Smoothing (EIP-1559)

Exponential Moving Average reduces gas price volatility:

```
α = 0.125 (EIP-1559 standard)
EMA_new = α × current_price + (1 - α) × EMA_old
```

**Benefits:**
- Reduces impact of sudden spikes
- Provides more stable estimates
- Follows Ethereum protocol standards

**Example:**
```rust
// Current gas price: 50 gwei
// Previous EMA: 40 gwei
// New EMA = 0.125 × 50 + 0.875 × 40 = 41.25 gwei
```

### Gas Limit Calculation

**EVM Chains:**
```
Total Cost = Gas Limit × Gas Price / 1e18
```

**Bitcoin:**
```
Total Fee = Fee Rate (sat/vB) × Transaction Size (vBytes)
Transaction Size ≈ (inputs × 148) + (outputs × 34) + 10
```

**Solana:**
```
Total Fee = Base Fee (5,000 lamports) + Priority Fee
```

## Error Handling

### Graceful Degradation

The service never fails - it always returns an estimate:

1. **Try RPC**: Fetch real-time gas price
2. **Try Cache**: Use cached value if RPC fails
3. **Fallback**: Use hardcoded conservative estimate

**Example:**
```rust
// This will NEVER panic or return error
let cost = estimator.get_gas_cost_for_network("ethereum").await;

// Even if RPC is down, you get a conservative estimate
// Logs warning: "RPC gas price fetch failed, using fallback"
```

### Error Logging

```rust
// Automatic logging on failures
tracing::warn!(
    "RPC gas price fetch failed for {}: {}, using fallback",
    network,
    error
);
```

## Integration Examples

### With SwapCrud

```rust
use exchange_shared::modules::swap::crud::SwapCrud;

let crud = SwapCrud::new(pool, redis_service, wallet_mnemonic);

// Automatically uses real-time gas estimation
let gas_cost = crud.get_gas_cost_for_network("ethereum").await;

// Used in platform fee calculation
let gas_floor = gas_cost * 1.5; // 50% safety buffer
let platform_fee = f64::max(rate * amount_to, gas_floor);
```

### With Pricing Engine

```rust
use exchange_shared::services::pricing::PricingEngine;
use exchange_shared::services::gas::GasEstimator;

let estimator = GasEstimator::new(Some(redis_service));
let pricing_engine = PricingEngine::new();

// Get real-time gas cost
let gas_cost = estimator.get_gas_cost_for_network("ethereum").await;

// Apply optimal markup with real gas costs
let rates = pricing_engine.apply_optimal_markup(
    &quotes,
    amount_from,
    ticker_from,
    gas_cost, // Real-time gas cost
);
```

## Testing

### Unit Tests

```bash
# Run gas estimator tests
cargo test gas::estimator::tests

# Run specific test
cargo test test_fallback_estimates
```

### Integration Tests

```bash
# Run example demo
cargo run --example gas_estimation_demo

# With Redis
REDIS_URL=redis://localhost:6379 cargo run --example gas_estimation_demo
```

### Manual Testing

```rust
#[tokio::test]
async fn test_ethereum_gas_estimation() {
    let estimator = GasEstimator::new(None);
    
    let estimate = estimator
        .estimate_gas("ethereum", TxType::NativeTransfer)
        .await
        .unwrap();
    
    assert!(estimate.total_cost_native > 0.0);
    assert_eq!(estimate.gas_limit, 21_000);
}
```

## Configuration

### Environment Variables

```bash
# RPC endpoints (already configured in .env.example)
ETH_PRIMARY_RPC=https://eth.llamarpc.com
POLYGON_PRIMARY_RPC=https://polygon-rpc.com

# Optional: Alchemy API key for premium performance
ALCHEMY_API_KEY=your_api_key_here

# Redis for caching
REDIS_URL=redis://localhost:6379
```

### Custom Configuration

```rust
// Custom EMA alpha (default: 0.125)
let mut estimator = GasEstimator::new(Some(redis_service));
// Note: EMA alpha is currently fixed to EIP-1559 standard
// Future: Make configurable via constructor
```

## Best Practices

### 1. Always Use Caching

```rust
// ✅ GOOD: With Redis caching
let estimator = GasEstimator::new(Some(redis_service));

// ⚠️ OK: Without caching (slower, more RPC calls)
let estimator = GasEstimator::new(None);
```

### 2. Handle Errors Gracefully

```rust
// ✅ GOOD: Use simple method for backward compatibility
let cost = estimator.get_gas_cost_for_network("ethereum").await;

// ✅ GOOD: Handle detailed errors
match estimator.estimate_gas("ethereum", TxType::NativeTransfer).await {
    Ok(estimate) => println!("Cost: {}", estimate.total_cost_native),
    Err(e) => eprintln!("Error: {}", e),
}
```

### 3. Choose Appropriate Transaction Type

```rust
// ✅ GOOD: Specific transaction type
let estimate = estimator.estimate_gas("ethereum", TxType::TokenTransfer).await?;

// ⚠️ OK: Default to NativeTransfer
let estimate = estimator.estimate_gas("ethereum", TxType::NativeTransfer).await?;
```

### 4. Monitor Cache Performance

```rust
// Check if estimate was cached
if estimate.cached {
    println!("Fast path: served from cache");
} else {
    println!("Slow path: fetched from RPC");
}
```

## Troubleshooting

### Issue: High RPC Latency

**Solution:** Enable Redis caching
```rust
let redis_service = RedisService::new("redis://localhost:6379").await?;
let estimator = GasEstimator::new(Some(redis_service));
```

### Issue: RPC Failures

**Solution:** Service automatically falls back to conservative estimates
```rust
// Logs warning but never fails
let cost = estimator.get_gas_cost_for_network("ethereum").await;
```

### Issue: Inaccurate Estimates

**Solution:** Check RPC endpoint configuration
```bash
# Verify RPC endpoints in .env
ETH_PRIMARY_RPC=https://eth.llamarpc.com

# Or use Alchemy for premium accuracy
ALCHEMY_API_KEY=your_api_key_here
```

## Future Enhancements

### Planned Features

1. **Mempool Monitoring** - Real-time pending transaction analysis
2. **Multi-Speed Estimates** - Slow/Standard/Fast options
3. **Gaussian Process Models** - ML-based predictions
4. **Historical Analytics** - Gas price trend analysis

### Contribution

See `REAL_GAS_PRICE_IMPLEMENTATION.md` for implementation details and research references.

## Support

For issues or questions:
1. Check logs for RPC failures
2. Verify Redis connection
3. Test with example: `cargo run --example gas_estimation_demo`
4. Review `REAL_GAS_PRICE_IMPLEMENTATION.md`
