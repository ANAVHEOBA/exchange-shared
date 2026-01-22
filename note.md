  ---
  Option B: Grouped Microservices (4 repos) ✅ RECOMMENDED

  ├── exchange-shared           ← Shared library
  ├── exchange-auth-service     ← Auth only
  ├── exchange-trading-service  ← Swap + Bridge (both are trading)
  ├── exchange-cards-service    ← Gift Cards + Prepaid (both are purchases)
  └── exchange-payments-service ← AnonPay (merchant-facing)

Based on the Trocador API documentation you provided and the current code, here is exactly what is left to implement to complete the Swap functionality.

  You have the "Menu" (Coins & Providers), now you need the "Kitchen" (Ordering & Tracking).

  1. Rate Estimation (The "Shopping" Phase)
  Missing Endpoint: GET /swap/rates
   * Trocador Method: new_rate
   * What it needs to do:
       1. Accept query params: from (btc), to (xmr), amount (0.1), network_from, network_to.
       2. Call Trocador new_rate.
       3. Return a list of quotes from different providers (ChangeNOW, CoinCraddle, etc.) sorted by best price.
       4. Crucial: Capture the trade_id returned by Trocador. This ID is required to create the trade in the next step.

  2. Create Swap (The "Checkout" Phase)
  Missing Endpoint: POST /swap/create
   * Trocador Method: new_trade
   * What it needs to do:
       1. Accept body: trade_id (from step 1), address (user's wallet), refund_address, provider.
       2. Call Trocador new_trade.
       3. Database Action: Save this swap to your local MySQL database (swaps table) linked to the user (if authenticated) or anonymous.
       4. Return the Deposit Address to the user so they can send the funds.

  3. Swap Status (The "Tracking" Phase)
  Missing Endpoint: GET /swap/{id}
   * Trocador Method: trade
   * What it needs to do:
       1. Accept the swap_id (your local ID).
       2. Look up the trocador_id in your database.
       3. Call Trocador trade with that ID.
       4. Update the status in your local database (e.g., from waiting -> confirming -> finished).
       5. Return the status to the user.

  4. Address Validation (Safety Feature)
  Missing Utility: (Internal or helper endpoint)
   * Trocador Method: validateaddress
   * Why needed: Before you call create_swap, you should verify the user's address is valid for that coin. If Trocador says it's invalid, block the request instantly to save API calls and user
     frustration.
