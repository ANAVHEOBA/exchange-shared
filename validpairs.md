set -a && source .env && \
    export TROCADOR_VALIDATE_MAX_PAIRS=2506 && \
    export TROCADOR_VALIDATE_SAMPLE_LIMIT=5000 && \
    export TROCADOR_VALIDATE_DELAY_MS=500 && \
    set +a && \
    cargo test test_live_trocador_validation_for_current_live_coin_list --test wallet_tests -- --ignored --nocapture
   Compiling exchange-shared v0.1.0 (/home/a/exchange-shared)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 11.66s
     Running tests/wallet_tests.rs (target/debug/deps/wallet_tests-ed52c122692a1ed9)

running 1 test
Validated 25/2466 live /coins pairs so far
test wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list has been running for over 60 seconds
Validated 50/2466 live /coins pairs so far
Validated 75/2466 live /coins pairs so far
Validated 100/2466 live /coins pairs so far
Validated 125/2466 live /coins pairs so far
Validated 150/2466 live /coins pairs so far
Validated 175/2466 live /coins pairs so far
Validated 200/2466 live /coins pairs so far
Validated 225/2466 live /coins pairs so far
Validated 250/2466 live /coins pairs so far
Validated 275/2466 live /coins pairs so far
Validated 300/2466 live /coins pairs so far
Validated 325/2466 live /coins pairs so far
Validated 350/2466 live /coins pairs so far
Validated 375/2466 live /coins pairs so far
Validated 400/2466 live /coins pairs so far
Validated 425/2466 live /coins pairs so far
Validated 450/2466 live /coins pairs so far
Validated 475/2466 live /coins pairs so far
Validated 500/2466 live /coins pairs so far
Validated 525/2466 live /coins pairs so far
Validated 550/2466 live /coins pairs so far
Validated 575/2466 live /coins pairs so far
Validated 600/2466 live /coins pairs so far
Validated 625/2466 live /coins pairs so far
Validated 650/2466 live /coins pairs so far
Validated 675/2466 live /coins pairs so far
Validated 700/2466 live /coins pairs so far
Validated 725/2466 live /coins pairs so far
Validated 750/2466 live /coins pairs so far
Validated 775/2466 live /coins pairs so far
Validated 800/2466 live /coins pairs so far
Validated 825/2466 live /coins pairs so far
Validated 850/2466 live /coins pairs so far
Validated 875/2466 live /coins pairs so far
Validated 900/2466 live /coins pairs so far
Validated 925/2466 live /coins pairs so far
Validated 950/2466 live /coins pairs so far
Validated 975/2466 live /coins pairs so far
Validated 1000/2466 live /coins pairs so far
Validated 1025/2466 live /coins pairs so far
Validated 1050/2466 live /coins pairs so far
Validated 1075/2466 live /coins pairs so far
Validated 1100/2466 live /coins pairs so far
Validated 1125/2466 live /coins pairs so far
Validated 1150/2466 live /coins pairs so far
Validated 1175/2466 live /coins pairs so far
Validated 1200/2466 live /coins pairs so far
Validated 1225/2466 live /coins pairs so far
Validated 1250/2466 live /coins pairs so far
Validated 1275/2466 live /coins pairs so far
Validated 1300/2466 live /coins pairs so far
Validated 1325/2466 live /coins pairs so far
Validated 1350/2466 live /coins pairs so far
Validated 1375/2466 live /coins pairs so far
Validated 1400/2466 live /coins pairs so far
Validated 1425/2466 live /coins pairs so far
Validated 1450/2466 live /coins pairs so far
Validated 1475/2466 live /coins pairs so far
Validated 1500/2466 live /coins pairs so far
Validated 1525/2466 live /coins pairs so far
Validated 1550/2466 live /coins pairs so far
Validated 1575/2466 live /coins pairs so far
Validated 1600/2466 live /coins pairs so far
Validated 1625/2466 live /coins pairs so far
Validated 1650/2466 live /coins pairs so far
Validated 1675/2466 live /coins pairs so far
Validated 1700/2466 live /coins pairs so far
Validated 1725/2466 live /coins pairs so far
Validated 1750/2466 live /coins pairs so far


Validated 1775/2466 live /coins pairs so far
Validated 1800/2466 live /coins pairs so far
Validated 1825/2466 live /coins pairs so far
Validated 1850/2466 live /coins pairs so far
Validated 1875/2466 live /coins pairs so far
Validated 1900/2466 live /coins pairs so far
Validated 1925/2466 live /coins pairs so far
Validated 1950/2466 live /coins pairs so far
Validated 1975/2466 live /coins pairs so far
Validated 2000/2466 live /coins pairs so far
Validated 2025/2466 live /coins pairs so far
Validated 2050/2466 live /coins pairs so far
Validated 2075/2466 live /coins pairs so far
Validated 2100/2466 live /coins pairs so far
Validated 2125/2466 live /coins pairs so far
Validated 2150/2466 live /coins pairs so far
Validated 2175/2466 live /coins pairs so far
Validated 2200/2466 live /coins pairs so far
Validated 2225/2466 live /coins pairs so far


Validated 2250/2466 live /coins pairs so far
Validated 2275/2466 live /coins pairs so far
Validated 2300/2466 live /coins pairs so far
Validated 2325/2466 live /coins pairs so far
Validated 2350/2466 live /coins pairs so far
Validated 2375/2466 live /coins pairs so far
Validated 2400/2466 live /coins pairs so far
Validated 2425/2466 live /coins pairs so far
Validated 2450/2466 live /coins pairs so far
live_pairs_fetched: 2466
checked_pairs: 2466
valid_pairs: 2444
local_invalid_pairs: 0
local_unsupported_pairs: 0
derivation_errors: 0
rejected_pairs: 0
live_catalog_mismatches: 0
http_errors: 12
parse_errors: 0
rate_limit_errors: 10
api_errors: 0
http_error_samples:
ceek/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=ceek&network=BEP20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
neo/N3 -> NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=neo&network=N3&address=NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z)
TREMP/SOL -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=TREMP&network=SOL&address=E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb)
snm/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=snm&network=BEP20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
xna/MAINNET -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=xna&network=MAINNET&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
far/MATIC -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=far&network=MATIC&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
id/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=id&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
coval/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=coval&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
dose/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=dose&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
m87/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=m87&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
GAS/NEO -> NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=GAS&network=NEO&address=NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z)
abds/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=abds&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
rate_limit_error_samples:
fofar/TRC20 -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error":"Rate limit exceeded"}

fuel/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

sai/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

tava/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

tet/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

TOKO/BSC -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

dia/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

moca/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

MPLX/SOL -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error":"Rate limit exceeded"}

wwy/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}


thread 'wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list' (6522) panicked at tests/wallet/trocador_live_current_support_test.rs:218:5:
Live Trocador /coins validation found rejects or mismatches:
live_pairs_fetched: 2466
checked_pairs: 2466
valid_pairs: 2444
local_invalid_pairs: 0
local_unsupported_pairs: 0
derivation_errors: 0
rejected_pairs: 0
live_catalog_mismatches: 0
http_errors: 12
parse_errors: 0
rate_limit_errors: 10
api_errors: 0
http_error_samples:
ceek/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=ceek&network=BEP20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
neo/N3 -> NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=neo&network=N3&address=NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z)
TREMP/SOL -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=TREMP&network=SOL&address=E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb)
snm/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=snm&network=BEP20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
xna/MAINNET -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=xna&network=MAINNET&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
far/MATIC -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=far&network=MATIC&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
id/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=id&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
coval/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=coval&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
dose/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=dose&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
m87/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=m87&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
GAS/NEO -> NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=GAS&network=NEO&address=NcMYdzEbqY8CcoiZ9Q3zn8KteWiZRJ8J4z)
abds/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: HTTP error: error sending request for url (https://api.trocador.app/validateaddress?ticker=abds&network=ERC20&address=0x9858effd232b4033e47d90003d41ec34ecaeda94)
rate_limit_error_samples:
fofar/TRC20 -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error":"Rate limit exceeded"}

fuel/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

sai/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

tava/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

tet/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

TOKO/BSC -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

dia/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

moca/ERC20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

MPLX/SOL -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error":"Rate limit exceeded"}

wwy/BEP20 -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error":"Rate limit exceeded"}

note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list ... FAILED

failures:

failures:
    wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 141 filtered out; finished in 6224.92s

error: test failed, to rerun pass `--test wallet_tests`









a@a:~/exchange-shared$ set -a && source .env &&     export TROCADOR_VALIDATE_MAX_PAIRS=2506 &&     export TROCADOR_VALIDATE_SAMPLE_LIMIT=5000 &&     export TROCADOR_VALIDATE_DELAY_MS=500 &&     set +a &&     cargo test test_live_trocador_validation_for_current_live_coin_list --test wallet_tests -- --ignored --nocapture
   Compiling exchange-shared v0.1.0 (/home/a/exchange-shared)
warning: associated function `chain_native_builder_required_message` is never used
    --> src/services/wallet/manager.rs:1994:8
     |
 232 | impl WalletManager {
     | ------------------ associated function in this implementation
...
1994 |     fn chain_native_builder_required_message(
     |        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
     |
     = note: `#[warn(dead_code)]` (part of `#[warn(unused)]`) on by default

warning: `exchange-shared` (lib) generated 1 warning
    Finished `test` profile [unoptimized + debuginfo] target(s) in 6.47s
     Running tests/wallet_tests.rs (target/debug/deps/wallet_tests-ed52c122692a1ed9)

running 1 test
Validated 25/2466 live /coins pairs so far
test wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list has been running for over 60 seconds
Validated 50/2466 live /coins pairs so far
Validated 75/2466 live /coins pairs so far
Validated 100/2466 live /coins pairs so far
Validated 125/2466 live /coins pairs so far
Validated 150/2466 live /coins pairs so far
Validated 175/2466 live /coins pairs so far
Validated 200/2466 live /coins pairs so far
Validated 225/2466 live /coins pairs so far
Validated 250/2466 live /coins pairs so far
Validated 275/2466 live /coins pairs so far
Validated 300/2466 live /coins pairs so far
Validated 325/2466 live /coins pairs so far
Validated 350/2466 live /coins pairs so far
Validated 375/2466 live /coins pairs so far
Validated 400/2466 live /coins pairs so far
Validated 425/2466 live /coins pairs so far
Validated 450/2466 live /coins pairs so far
Validated 475/2466 live /coins pairs so far
Validated 500/2466 live /coins pairs so far
Validated 525/2466 live /coins pairs so far
Validated 550/2466 live /coins pairs so far
Validated 575/2466 live /coins pairs so far
Validated 600/2466 live /coins pairs so far
Validated 625/2466 live /coins pairs so far
Validated 650/2466 live /coins pairs so far
Validated 675/2466 live /coins pairs so far
Validated 700/2466 live /coins pairs so far
Validated 725/2466 live /coins pairs so far
Validated 750/2466 live /coins pairs so far
Validated 775/2466 live /coins pairs so far
Validated 800/2466 live /coins pairs so far
Validated 825/2466 live /coins pairs so far
Validated 850/2466 live /coins pairs so far
Validated 875/2466 live /coins pairs so far
Validated 900/2466 live /coins pairs so far
Validated 925/2466 live /coins pairs so far
Validated 950/2466 live /coins pairs so far
Validated 975/2466 live /coins pairs so far
Validated 1000/2466 live /coins pairs so far
Validated 1025/2466 live /coins pairs so far
Validated 1050/2466 live /coins pairs so far
Validated 1075/2466 live /coins pairs so far
Validated 1100/2466 live /coins pairs so far
Validated 1125/2466 live /coins pairs so far
Validated 1150/2466 live /coins pairs so far
Validated 1175/2466 live /coins pairs so far
Validated 1200/2466 live /coins pairs so far
Validated 1225/2466 live /coins pairs so far
Validated 1250/2466 live /coins pairs so far
Validated 1275/2466 live /coins pairs so far
Validated 1300/2466 live /coins pairs so far
Validated 1325/2466 live /coins pairs so far
Validated 1350/2466 live /coins pairs so far
Validated 1375/2466 live /coins pairs so far
Validated 1400/2466 live /coins pairs so far
Validated 1425/2466 live /coins pairs so far
Validated 1450/2466 live /coins pairs so far
Validated 1475/2466 live /coins pairs so far
Validated 1500/2466 live /coins pairs so far
Validated 1525/2466 live /coins pairs so far
Validated 1550/2466 live /coins pairs so far
Validated 1575/2466 live /coins pairs so far
Validated 1600/2466 live /coins pairs so far
Validated 1625/2466 live /coins pairs so far
Validated 1650/2466 live /coins pairs so far
Validated 1675/2466 live /coins pairs so far
Validated 1700/2466 live /coins pairs so far
Validated 1725/2466 live /coins pairs so far
Validated 1750/2466 live /coins pairs so far
Validated 1775/2466 live /coins pairs so far
Validated 1800/2466 live /coins pairs so far
Validated 1825/2466 live /coins pairs so far
Validated 1850/2466 live /coins pairs so far
Validated 1875/2466 live /coins pairs so far
Validated 1900/2466 live /coins pairs so far
Validated 1925/2466 live /coins pairs so far
Validated 1950/2466 live /coins pairs so far
Validated 1975/2466 live /coins pairs so far
Validated 2000/2466 live /coins pairs so far
Validated 2025/2466 live /coins pairs so far
Validated 2050/2466 live /coins pairs so far
Validated 2075/2466 live /coins pairs so far
Validated 2100/2466 live /coins pairs so far
Validated 2125/2466 live /coins pairs so far
Validated 2150/2466 live /coins pairs so far
Validated 2175/2466 live /coins pairs so far
Validated 2200/2466 live /coins pairs so far
Validated 2225/2466 live /coins pairs so far
Validated 2250/2466 live /coins pairs so far
Validated 2275/2466 live /coins pairs so far
Validated 2300/2466 live /coins pairs so far
Validated 2325/2466 live /coins pairs so far
Validated 2350/2466 live /coins pairs so far
Validated 2375/2466 live /coins pairs so far
Validated 2400/2466 live /coins pairs so far
Validated 2425/2466 live /coins pairs so far
Validated 2450/2466 live /coins pairs so far
live_pairs_fetched: 2466
checked_pairs: 2466
valid_pairs: 2466
local_invalid_pairs: 0
local_unsupported_pairs: 0
derivation_errors: 0
rejected_pairs: 0
live_catalog_mismatches: 0
http_errors: 0
parse_errors: 0
rate_limit_errors: 0
api_errors: 0
test wallet::trocador_live_current_support_test::test_live_trocador_validation_for_current_live_coin_list ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 144 filtered out; finished in 4639.52s

a@a:~/exchange-shared$ 


  1. more EVM
  2. more Cosmos
  3. Substrate
  4. NEAR/XRP/Stellar
  5. Antelope
  6. one-offs last


  1. More EVM coverage

  - same EVM family, but many configured EVM chains are not yet in the exact native-route map

  2. More Cosmos coverage

  - same Cosmos family, but only the curated subset is direct-local so far

  3. Substrate / Polkadot

  - still a major missing family

  4. Account-based quick wins

  - NEAR
  - XRP
  - Stellar

  5. Antelope-style chains

  - EOS
  - FIO
  - similar API pattern

  6. True one-off families

  - Cardano
  - TON
  - Stacks
  - Tezos
  - Waves
  - CKB
  - Hedera
  - Starknet
  - Zilliqa
  - ICON
  - Nano
  - NEO
  - Radix
  - Quai
  - Everscale/Venom
  - Steem
  - privacy-style chains like Monero/Dero/Zano
//codex resume 019d2832-113a-7f12-80dd-1181093fb3c5


  1. EVM Family (119 chains): 
       * This includes Ethereum, BNB Smart Chain, Polygon, Arbitrum, Base, Avalanche C-Chain, and 113 other Layer 2s and EVM-compatible chains.
   2. UTXO Family (18 chains):
       * This includes Bitcoin, Dash, Dogecoin, Bitcoin Cash, Litecoin, and 13 other Bitcoin-forks.
   3. Cosmos Family (16 chains):
       * Includes Cosmos Hub, Osmosis, Neutron, Injective, Celestia, and 11 others.
   4. Special Implementations (2 chains):
       * Solana (and its tokens).
       * Tron (and TRC20 tokens).

  ---


   AbeyChain, Acala, ApeChain, Arena-Z, Aurora, B2 Network, Bahamut, Blast, Boba Network, Botanix, BounceBit, Canto, Chiliz, Conflux, Conflux eSpace, COTI, Cyber, Dione,
  Electroneum, Endurance, Energi, Ethereum Classic, EthereumPoW, Evmos, Findora, Fraxtal, Fuse, Gravity, Haqq, Humanode, HyperEVM, IOST, IOTA EVM, Japan Open Chain, Kaia,
  Kaia Legacy, KaiChain, Karura, Katana, Lisk, MAP Protocol, Merlin, Metal L2, Meter, Mode, Peaq, Pocket, PulseChain, Redbelly, REI Network, Rootstock, Shibarium, Step
  Network, Stratis EVM, Ternoa zkEVM, ThunderCore, TomoChain, U2U, Vanar, Viction, WhiteChain, ZetaChain.