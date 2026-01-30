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
│       ├── hashing.rs       # Argon2 password hashing
│       ├── jwt.rs           # JWT token management
│       ├── rate_limit.rs    # Rate limiting middleware
│       ├── redis_cache.rs   # Redis caching service
│       ├── trocador.rs      # Trocador API client
│       └── security.rs      # Security headers middleware
├── migrations/              # SQL migrations
├── tests/
│   ├── common/              # Test utilities
│   │   └── mod.rs
│   ├── auth/                # Auth endpoint tests
│   │   ├── mod.rs
│   │   ├── register_test.rs
│   │   ├── login_test.rs
│   │   └── ...
│   ├── swap/                # Swap endpoint tests
│   │   ├── mod.rs
│   │   ├── currencies_test.rs
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

```bash
# Run all tests
cargo test

# Run auth tests only
cargo test --test auth_tests

# Run swap tests only
cargo test --test swap_tests

# Run specific test with output
cargo test test_name -- --nocapture

# Run tests sequentially (for DB tests)
cargo test -- --test-threads=1
```

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
