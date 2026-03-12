# Payout Retry Logic Implementation

## Overview
Implementing robust error handling and retry logic for blockchain payouts to handle transient failures (network errors, gas price spikes, RPC timeouts).

## Implementation Plan

### 1. Add Retry Wrapper Method
**File:** `src/services/wallet/manager.rs`

```rust
pub async fn process_payout_with_retry(
    &self,
    req: PayoutRequest,
    max_attempts: usize,
) -> Result<PayoutResponse, String>
```

Features:
- Exponential backoff (1s, 2s, 4s, 8s...)
- Configurable max attempts (default: 3)
- Detailed error logging
- Idempotency (won't double-pay)

### 2. Update Swap Status on Failure
**File:** `src/modules/swap/model.rs`

Add new status: `"failed"` (already exists in DB schema)

### 3. Update BlockchainListener Error Handling
**File:** `src/services/blockchain/listener.rs`

- Call `process_payout_with_retry` instead of `process_payout`
- Update swap status to 'failed' on permanent failure
- Log detailed error information

### 4. Add Payout Audit Logging
**File:** `src/modules/wallet/crud.rs`

Add method to log payout attempts:
```rust
pub async fn log_payout_attempt(
    &self,
    swap_id: &str,
    status: &str,
    message: Option<&str>,
) -> Result<(), sqlx::Error>
```

## Error Categories

### Retryable Errors
- Network timeouts
- RPC endpoint unavailable
- Temporary gas price spikes
- Nonce conflicts

### Non-Retryable Errors
- Insufficient balance
- Invalid address
- Invalid signature
- Amount too small for fees

## Testing Strategy

1. ✅ Test current behavior (no retry)
2. Test successful retry after N failures
3. Test failure after max retries
4. Test exponential backoff timing
5. Test error logging

## Benefits

- Handles transient network issues automatically
- Reduces manual intervention
- Better user experience
- Detailed error tracking for debugging
- Prevents funds from being stuck
