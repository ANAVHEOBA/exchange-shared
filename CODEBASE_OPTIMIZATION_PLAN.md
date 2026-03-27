# Codebase Optimization Plan

## Core Flow

This codebase should be optimized around one business flow:

1. User asks for a quote.
2. User chooses a rate/provider.
3. We generate our own destination address for the asset Trocador will send.
4. We create the trade with Trocador using our generated address as the recipient.
5. User sends funds to the provider deposit address returned by Trocador.
6. Trocador completes the exchange and sends the payout to our generated address.
7. We detect incoming funds on-chain.
8. We remove our commission.
9. We send the remaining amount to the user's final address.
10. We mark the swap completed and store the full audit trail.

That needs to be the center of the architecture. Everything else should support that flow or be removed.

## Current Problems

- `SwapCrud` is doing too much: controller-facing logic, external API calls, caching, pricing, validation, swap creation, and persistence.
- The codebase mixes HTTP modules, application services, repositories, and infrastructure in a way that makes ownership unclear.
- The primary flow is real, but some surrounding modules are partially wired or dead.
- Auth and metrics contain scaffold that is not fully connected to the live app.
- Background settlement is split across multiple places with overlapping responsibility.
- Status handling is harder than it needs to be.

## Optimization Goal

Restructure the codebase so the swap pipeline is explicit:

- HTTP layer receives requests.
- Application layer orchestrates business flow.
- Domain services calculate commission, validate transitions, and decide payout amounts.
- Infrastructure adapters talk to Trocador, RPC endpoints, Redis, and MySQL.
- Repositories only read/write database state.

## Target Architecture

Suggested ownership:

- `src/http/`
  - Routes and request/response mapping only.
- `src/application/`
  - `quote_service.rs`
  - `swap_service.rs`
  - `settlement_service.rs`
  - `payout_service.rs`
- `src/domain/`
  - `commission.rs`
  - `swap_status.rs`
  - `swap_flow.rs`
  - `amounts.rs`
- `src/infrastructure/`
  - `trocador_gateway.rs`
  - `rpc_gateway.rs`
  - `redis_cache.rs`
  - `repositories/`
- `src/workers/`
  - `blockchain_listener.rs`
  - `settlement_reconciler.rs`

You do not need to physically move everything at once. The first step is to separate responsibilities while keeping the public behavior stable.

## Step-by-Step Plan

### Step 1: Freeze the Canonical Swap State Machine

Create one source of truth for swap lifecycle states.

Recommended states:

- `quote_requested`
- `trade_created`
- `waiting_for_user_deposit`
- `provider_confirming`
- `provider_exchanging`
- `funds_received_internal`
- `payout_pending`
- `payout_broadcast`
- `completed`
- `failed`
- `refunded`
- `expired`

Actions:

- Define the status enum in one place.
- Define allowed transitions in one place.
- Stop mapping statuses ad hoc in multiple files.
- Add a small helper for `can_transition(from, to)`.

Why first:

- Every service and worker depends on status correctness.

### Step 2: Extract a Real `SwapService`

Move the business flow out of `SwapCrud`.

`SwapService` should own:

- create quote
- create swap
- get swap status
- compute user payout amount
- start settlement when funds arrive

`SwapCrud` should become a repository or be split into:

- `SwapRepository`
- `ProviderRepository`
- `CurrencyRepository`

Why:

- Right now the core flow is hidden inside a giant file.

### Step 3: Extract a `TrocadorGateway`

Wrap all Trocador calls behind one interface.

Responsibilities:

- get currencies
- get providers
- get rates
- validate address
- create trade
- get trade status

Rules:

- retry logic lives here
- request/response normalization lives here
- raw Trocador DTOs stop leaking across the codebase

Why:

- External integration code should not be mixed with swap orchestration.

### Step 4: Separate Address Generation from Swap Creation

Create an `AddressService`.

Responsibilities:

- get next derivation index
- derive internal address
- persist internal address metadata
- return a typed `InternalReceivingAddress`

Why:

- The internal receiving address is a first-class part of your business flow.
- It is not just a side effect inside swap creation.

### Step 5: Centralize Commission Logic

Create a `CommissionService`.

Responsibilities:

- calculate platform commission
- calculate minimum gas floor
- calculate final user payout amount
- store both gross received and net sent amounts

Rules:

- The same commission logic must be used in quote, estimate, and settlement.
- Do not duplicate commission math in quote flow and create flow.

Why:

- Commission is core business logic, not presentation logic.

### Step 6: Split Settlement into Two Explicit Stages

Separate:

1. detection of funds on our internal address
2. payout to the user

Suggested services:

- `SettlementDetector`
- `PayoutExecutor`

Worker behavior:

- blockchain listener detects funds
- detector marks swap as `funds_received_internal`
- payout executor computes commission and sends user payout
- final state becomes `completed`

Why:

- This makes failures easier to retry without replaying unrelated work.

### Step 7: Make Background Workers Non-Overlapping

Current behavior should be simplified:

- one worker is responsible for on-chain detection
- one worker is responsible for fallback reconciliation
- neither should contain duplicated payout decision logic

Recommended rule:

- Listener = primary source for funds detection
- Reconciler = fallback status sync and missed-event recovery
- Only one service actually performs payout execution

Why:

- Overlapping worker logic creates race conditions and mental overhead.

### Step 8: Move Caching Behind Service Boundaries

Keep Redis, but stop letting controllers or mixed CRUD code decide caching strategy.

Suggested ownership:

- `QuoteCache`
- `ProviderCache`
- `CurrencyCache`
- `SettlementLock`

Rules:

- cache keys and TTL strategy live in dedicated modules
- singleflight locking lives next to quote caching
- stale-while-revalidate logic lives next to list endpoints only

Why:

- It becomes easier to reason about data freshness and failures.

### Step 9: Remove or Finish Dead Scaffolding

Choose one:

- wire missing auth/metrics features properly
- or delete unused code until needed

Immediate cleanup candidates:

- unmounted metrics routes
- unused `AppState.http_client`
- auth DTOs and traits that have no live implementation
- legacy compatibility branches that are never reached

Rule:

- if it is not mounted, not called, and not part of the near-term roadmap, remove it

Why:

- Dead scaffolding makes the architecture look bigger than it is.

### Step 10: Add a Real Test Strategy Around the Core Flow

Test layers:

- unit tests for commission, status transitions, and payout math
- integration tests for `SwapService`
- gateway tests for Trocador adapters
- worker tests for funds detection and payout retries

Golden path test:

1. create quote
2. create swap with internal receiving address
3. simulate provider sending funds to internal address
4. apply commission
5. send payout to user
6. verify swap status and persisted amounts

Why:

- Your actual business flow should be testable without relying on accidental coverage.

## Practical Refactor Order

Do the work in this order:

1. Define status model and transition rules.
2. Extract `CommissionService`.
3. Extract `TrocadorGateway`.
4. Extract `AddressService`.
5. Extract `SwapService` from `SwapCrud`.
6. Extract `PayoutExecutor`.
7. Simplify workers so only one place executes payouts.
8. Move cache logic into dedicated modules.
9. Remove dead auth/metrics scaffolding or finish wiring it.
10. Rename modules to match actual ownership.

## Non-Negotiable Rules During Refactor

- Do not break the current swap creation and payout path while reorganizing code.
- Preserve database compatibility first; improve schema only after service boundaries are clear.
- Keep one canonical calculation for commission and payout amounts.
- Keep one canonical mapping for external provider statuses to internal statuses.
- Make every worker idempotent.

## Success Criteria

The codebase is optimized when:

- the core swap flow is readable from top to bottom without jumping across unrelated modules
- controllers are thin
- repositories only persist data
- external integrations are isolated
- commission logic is defined once
- payout execution happens in one place
- dead scaffold is removed or fully wired
- the main business flow has stable tests

 1. Extract TrocadorGateway first from src/services/trocador.rs and make crud.rs call it.
  2. Extract CommissionService and make both quote flow and create flow use the same fee calculation.
  3. Extract QuoteService for rates and estimate.
  4. Extract SwapRepository from the raw SQL in src/modules/swap/crud.rs.
  5. Extract SwapService for create/status orchestration.
  6. Extract SettlementService and make both the blockchain listener and fallback monitor call the same settlement entrypoint.
  7. Only after behavior is stable, rename/move folders.
