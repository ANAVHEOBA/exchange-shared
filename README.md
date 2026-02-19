# Exchange Platform

A privacy-focused cryptocurrency swap aggregator built with Rust and Axum, similar to [Trocador.app](https://trocador.app). Aggregates rates from multiple exchange providers via the Trocador API to offer users the best swap rates without requiring account creation.

## Features

### Core Features
- **Anonymous Swaps** - No account required for basic swaps
- **Multi-Provider Aggregation** - Fetches rates from multiple exchanges (ChangeNOW, Changelly, etc.)
- **Best Rate Selection** - Automatically sorts by best rates
- **Fixed & Floating Rates** - Support for both rate types
- **Swap Tracking** - Track swap status via unique swap ID

### Optimization Architecture
- **Distributed Singleflight** - coalesces concurrent requests for the same currency pair into a single upstream API call, preventing "thundering herd" issues and protecting API rate limits.
- **Probabilistic Early Recomputation (PER)** - randomizes cache expiration for slowly changing data (like providers and currencies) to recompute values *before* they fully expire, ensuring users always see fresh data with zero latency.
- **Raw JSON Caching** - stores pre-serialized JSON in Redis for heavy endpoints (like `/currencies`), bypassing serialization overhead for ultra-fast response times (<10ms).
- **Background Warming** - intelligently keeps popular trading pairs "warm" in the cache.

### User Features
- **Optional Accounts** - Create account to track swap history
- **Swap History** - View all past swaps (authenticated users)
- **Sandbox Mode** - Test swaps without real funds
- **Volume-Based Commission** - Flexible commission tiers based on swap size
- **Multi-Chain Support** - USDT on Ethereum, Solana, Polygon (token-chain aware routing)

### Commission System
- **Configurable Tiers** - Volume-based commission structure (e.g., 1.0% for small, 0.3% for large swaps)
- **Transparent Fees** - Platform commission clearly displayed in rate quotes
- **Per-Swap Tracking** - Commission earned tracked for each swap transaction
- **Chain-Aware Deduction** - Commission applied correctly across different blockchains
- **Future-Ready** - Extensible architecture for volatility-based and loyalty discounts

### Security
- **Rate Limiting** - Protection against abuse
- **Security Headers** - X-Content-Type-Options, X-Frame-Options
- **Input Validation** - Address validation, XSS/SQL injection protection
- **Argon2 Password Hashing** - Secure password storage
- **JWT Authentication** - Stateless auth with access/refresh tokens

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust |
| Framework | Axum |
| Database | MySQL (SQLx) |
| Caching | Redis |
| Auth | JWT (jsonwebtoken) |
| Password Hashing | Argon2id |
| Rate Limiting | Governor |
| HTTP Client | Reqwest |

## Getting Started

### Prerequisites

- Rust 1.70+
- MySQL 8.0+
- Redis 6.0+
- cargo

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd exchange-shared

# Install dependencies
cargo build
```

### Environment Setup

Create a `.env` file in the project root:

```env
# Database
DATABASE_URL=mysql://user:password@localhost:3306/exchange_db
TEST_DATABASE_URL=mysql://user:password@localhost:3306/exchange_test_db
REDIS_URL=redis://localhost:6379

# JWT
JWT_SECRET=your-secret-key-min-32-characters-long

# Server
HOST=0.0.0.0
PORT=3000

# Trocador API Key
TROCADOR_API_KEY=your-api-key

# Environment
RUST_LOG=exchange_shared=debug,tower_http=debug
```

### Database Setup

```bash
# Run migrations
sqlx migrate run
```

### Running the Server

```bash
# Development
cargo run

# Production
cargo build --release
./target/release/exchange-shared
```

## API Documentation

### Authentication Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/auth/register` | No | Create new account |
| POST | `/auth/login` | No | Login, get tokens |
| POST | `/auth/logout` | Yes | Invalidate refresh token |
| POST | `/auth/refresh` | No | Refresh access token |
| GET | `/auth/me` | Yes | Get current user |

### Swap Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/swap/currencies` | No | List supported currencies |
| GET | `/swap/pairs` | No | List available trading pairs |
| GET | `/swap/rates` | No | Get rates from all providers |
| POST | `/swap/estimate` | No | Get estimated swap amount |
| POST | `/swap/create` | No* | Create a new swap |
| GET | `/swap/{id}` | No | Get swap status |
| GET | `/swap/history` | Yes | Get user's swap history |
| GET | `/swap/providers` | No | List exchange providers |

*Auth optional - if provided, swap is linked to user account

### Example: Create a Swap

```bash
curl -X POST http://localhost:3000/swap/create \
  -H "Content-Type: application/json" \
  -d '{
    "from": "btc",
    "to": "eth",
    "amount": 0.1,
    "provider": "changenow",
    "recipient_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5bE12",
    "refund_address": "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
  }'
```

Response:
```json
{
  "swap_id": "abc123",
  "deposit_address": "bc1q...",
  "deposit_amount": 0.1,
  "estimated_receive": 1.45,
  "status": "waiting",
  "expires_at": "2024-01-01T12:00:00Z"
}
```

### Example: Get Rates

```bash
curl "http://localhost:3000/swap/rates?from=btc&to=eth&amount=0.1"
```

Response:
```json
{
  "rates": [
    {
      "provider": "changenow",
      "rate": 14.5,
      "estimated_amount": 1.45,
      "min_amount": 0.001,
      "max_amount": 10,
      "network_fee": 0.001,
      "platform_fee": 0.01,
      "rate_type": "floating"
    }
  ]
}
```

## Project Structure

```
exchange-shared/
├── src/
│   ├── main.rs              # Application entry point
│   ├── lib.rs               # App setup, router, state
│   ├── config/              # Database config
│   │   └── mod.rs
│   ├── modules/
│   │   ├── auth/            # Authentication module
│   │   │   ├── mod.rs
│   │   │   ├── controller.rs
│   │   │   ├── crud.rs
│   │   │   ├── model.rs
│   │   │   ├── routes.rs
│   │   │   └── schema.rs
│   │   └── swap/            # Swap module (to implement)
│   │       ├── mod.rs
│   │       ├── controller.rs
│   │       ├── crud.rs
│   │       ├── model.rs
│   │       ├── routes.rs
│   │       └── schema.rs
│   └── services/
│       ├── mod.rs
│       ├── hashing.rs           # Argon2 password hashing
│       ├── jwt.rs               # JWT token management
│       ├── rate_limit.rs        # Rate limiting middleware
│       ├── redis_cache.rs       # Redis caching service
│       ├── trocador.rs          # Trocador API client
│       └── security.rs          # Security headers middleware
├── migrations/                  # SQL migrations
├── tests/
│   ├── common/                  # Test utilities
│   │   └── mod.rs
│   ├── auth/                    # Auth endpoint tests
│   │   ├── mod.rs
│   │   ├── register_test.rs
│   │   ├── login_test.rs
│   │   └── ...
│   └── swap/                    # Swap endpoint tests
│       ├── mod.rs
│       ├── currencies_test.rs
│       ├── rates_test.rs
│       ├── create_test.rs
│       ├── status_test.rs
│       ├── multi_chain_test.rs     # 🆕 Multi-token, multi-chain edge cases
│       ├── commission_test.rs      # 🆕 Commission system integration tests
│       └── wallet_validation_test.rs # 🆕 Address validation edge cases
│   │   ├── rates_test.rs
│   │   ├── create_test.rs
│   │   └── ...
│   ├── auth_tests.rs
│   └── swap_tests.rs
├── Cargo.toml
├── .env.example
└── README.md
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run auth tests only
cargo test --test auth_tests

# Run wallet tests only
cargo test --test wallet_tests

# Run worker tests only
cargo test --test worker_tests
```

### Running Swap Tests (Important!)

Swap tests hit the real Trocador API and must run sequentially to avoid rate limiting:

```bash
# Run ALL swap tests sequentially (recommended)
cargo test --test swap_tests -- --test-threads=1

# Or use the convenience script
./run_swap_tests.sh

# Run specific swap test module
cargo test --test swap_tests currencies_test -- --test-threads=1
cargo test --test swap_tests providers_test -- --test-threads=1
cargo test --test swap_tests create_test -- --test-threads=1
```

**Why `--test-threads=1`?**
- Swap tests call the real Trocador API
- Running tests in parallel overwhelms the API with concurrent requests
- This causes 429 rate limit errors and test failures
- Sequential execution (1 test at a time) ensures all tests pass
- Takes ~5-10 minutes but guarantees reliability

### Test Coverage

| Module | Tests | Status |
|--------|-------|--------|
| Auth - Register | 13 | Passing |
| Auth - Login | 10 | Passing |
| Swap - Currencies | 11 | Passing |
| Swap - Pairs | 13 | Passing |
| Swap - Rates | 26 | Passing |
| Swap - Estimate | 22 | Passing |
| Swap - Create | 30 | Passing |
| Swap - Status | 18 | Passing |
| Swap - History | 21 | Passing |
| Swap - Providers | 20 | Passing |
| **Swap - Multi-Chain (NEW)** | **15** | **Passing** |
| **Swap - Commission (NEW)** | **12** | **Passing** |
| **Swap - Wallet Validation (NEW)** | **13** | **Passing** |

### Advanced Integration Tests (Multi-Chain & Commission System)

These integration tests cover complex edge cases for production-grade swap operations:

#### Multi-Chain Edge Cases (`tests/swap/multi_chain_test.rs`)

Tests for scenarios where the same token exists on multiple blockchains:

- **USDT/USDC across chains**: Verify correct chain routing (Ethereum, Solana, Polygon)
- **Chain mismatch detection**: Reject Ethereum addresses for Solana tokens
- **Token on unsupported chain**: USDT doesn't exist on Bitcoin network
- **Cross-chain address validation**: Ensure address format matches network
- **Rate variation by chain**: Same pair, different networks = different fees
- **Memo/tag requirements**: Handle coins requiring destination tags (XRP, Stellar)
- **Decimal precision**: Different tokens use different decimals (6 vs 18)
- **Amount limits**: Min/max amounts vary per token-chain combo
- **Swap history tracking**: Correctly track which chains involved
- **Provider support variance**: Not all providers support all chains
- **Rate expiration**: Handle quote expiration on slow chains

**Run these tests:**
```bash
cargo test --test swap_tests multi_chain_test -- --nocapture
```

#### Commission System Edge Cases (`tests/swap/commission_test.rs`)

Tests for platform commission calculation, deduction, and tracking:

- **Commission deduction math**: Verify `user_receives = trocador_amount - commission`
- **Volume-based tiers**: Different amounts → different commission percentages
- **Fixed vs Floating rates**: Commission applies to both rate types
- **Provider consistency**: All providers have commission applied
- **Negative commission rejection**: Commission never goes negative (no rebates)
- **User receives less logic**: Confirm user gets less than raw Trocador amount
- **Commission in swap records**: Stored and retrievable from swap history
- **Minimum amount commissions**: Correct math even at minimum swap size
- **Provider variance**: Different providers may have different commission structures
- **Memo-required coins**: Commission applies even to coins needing tags
- **High-volume discounts**: Large swaps should have lower % commission (if tiering enabled)
- **Historical tracking**: Past swaps show commission taken

**Run these tests:**
```bash
cargo test --test swap_tests commission_test -- --nocapture
```

#### Wallet & Address Validation Edge Cases (`tests/swap/wallet_validation_test.rs`)

Tests for address format validation across different cryptocurrencies:

- **Invalid Bitcoin addresses**: Reject wrong format/checksum
- **Invalid Ethereum addresses**: Must be 0x + 40 hex characters
- **Solana Base58 format**: Validate correct encoding
- **Memo/tag handling**: XRP destination tags, Stellar memos
- **Case sensitivity**: Address case handling per network
- **Empty address rejection**: No null/empty values allowed
- **Whitespace handling**: Trim or reject spaces in addresses
- **Special characters**: Handle unique address formats (Monero, Zcash)
- **Very long addresses**: Reject excessively long strings
- **Recipient = Refund logic**: Same address for both is allowed/disallowed
- **Extra ID validation**: XRP, Stellar require extra ID fields
- **Cross-chain confusion**: Solana address for Ethereum token → rejected
- **Batch validation**: Validate multiple addresses of different types

**Run these tests:**
```bash
cargo test --test swap_tests wallet_validation_test -- --nocapture
```

### Running All Swap Tests

```bash
# Run all swap tests (including new advanced tests)
cargo test --test swap_tests -- --nocapture --test-threads=1

# Run only the new advanced integration tests
cargo test --test swap_tests multi_chain_test commission_test wallet_validation_test -- --nocapture

# Run with detailed output
cargo test --test swap_tests -- --nocapture --test-threads=1 2>&1 | grep -E "(test_|passed|FAILED)"
```

## Performance

| Operation | Time |
|-----------|------|
| Register | ~850ms (Argon2 hashing) |
| Login | ~510ms (Argon2 verify) |
| JWT Verify | <1ms |
| Get Rates (Cached) | <10ms |
| Get Rates (API) | ~5-10s (dependent on upstream) |

## Revenue Model

Two revenue streams when integrating with exchange providers:

1. **Revenue Share** - Providers give ~0.4% of their fee
2. **Platform Markup** - Add your own fee on top (100% yours)

```
User pays: Exchange fee + Your markup
You receive: Revenue share + Your markup
```

## Security Considerations

- Never commit `.env` files
- Use strong JWT secrets (32+ characters)
- Enable HTTPS in production
- Monitor rate limit logs
- Validate all wallet addresses
- Sanitize all user input

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/name`)
3. Write tests for new features
4. Ensure all tests pass (`cargo test`)
5. Submit pull request

## License

MIT License - see LICENSE file for details
