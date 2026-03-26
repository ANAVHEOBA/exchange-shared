set -a && source .env && \
  export TROCADOR_VALIDATE_FORCE_FULL_SNAPSHOT=1 && \
  export TROCADOR_VALIDATE_MAX_PAIRS=2506 && \
  export TROCADOR_VALIDATE_SAMPLE_LIMIT=5000 && \
  export TROCADOR_VALIDATE_DELAY_MS=500 && \
  set +a && \
  cargo test test_live_trocador_validation_for_locally_suspicious_snapshot_pairs --test wallet_tests -- --ignored --nocapture | tee trocador_full_snapshot_validation.log
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.27s
     Running tests/wallet_tests.rs (target/debug/deps/wallet_tests-ed52c122692a1ed9)

running 1 test
test wallet::trocador_live_validation_test::test_live_trocador_validation_for_locally_suspicious_snapshot_pairs has been running for over 60 seconds
Validated 25/2506 suspicious snapshot pairs so far
Validated 50/2506 suspicious snapshot pairs so far
Validated 75/2506 suspicious snapshot pairs so far
Validated 100/2506 suspicious snapshot pairs so far
Validated 125/2506 suspicious snapshot pairs so far
Validated 150/2506 suspicious snapshot pairs so far


Validated 175/2506 suspicious snapshot pairs so far
Validated 200/2506 suspicious snapshot pairs so far
Validated 225/2506 suspicious snapshot pairs so far
Validated 250/2506 suspicious snapshot pairs so far
Validated 275/2506 suspicious snapshot pairs so far
Validated 300/2506 suspicious snapshot pairs so far
Validated 325/2506 suspicious snapshot pairs so far
Validated 350/2506 suspicious snapshot pairs so far
Validated 375/2506 suspicious snapshot pairs so far
Validated 400/2506 suspicious snapshot pairs so far
Validated 425/2506 suspicious snapshot pairs so far
Validated 450/2506 suspicious snapshot pairs so far
Validated 475/2506 suspicious snapshot pairs so far
Validated 500/2506 suspicious snapshot pairs so far
Validated 525/2506 suspicious snapshot pairs so far
Validated 550/2506 suspicious snapshot pairs so far
Validated 575/2506 suspicious snapshot pairs so far
Validated 600/2506 suspicious snapshot pairs so far
Validated 625/2506 suspicious snapshot pairs so far
Validated 650/2506 suspicious snapshot pairs so far
Validated 675/2506 suspicious snapshot pairs so far
Validated 700/2506 suspicious snapshot pairs so far
Validated 725/2506 suspicious snapshot pairs so far
Validated 750/2506 suspicious snapshot pairs so far
Validated 775/2506 suspicious snapshot pairs so far
Validated 800/2506 suspicious snapshot pairs so far
Validated 825/2506 suspicious snapshot pairs so far
Validated 850/2506 suspicious snapshot pairs so far
Validated 875/2506 suspicious snapshot pairs so far
Validated 900/2506 suspicious snapshot pairs so far
Validated 925/2506 suspicious snapshot pairs so far
Validated 950/2506 suspicious snapshot pairs so far
Validated 975/2506 suspicious snapshot pairs so far
Validated 1000/2506 suspicious snapshot pairs so far
Validated 1025/2506 suspicious snapshot pairs so far
Validated 1050/2506 suspicious snapshot pairs so far
Validated 1075/2506 suspicious snapshot pairs so far
Validated 1100/2506 suspicious snapshot pairs so far
Validated 1125/2506 suspicious snapshot pairs so far
Validated 1150/2506 suspicious snapshot pairs so far
Validated 1175/2506 suspicious snapshot pairs so far
Validated 1200/2506 suspicious snapshot pairs so far
Validated 1225/2506 suspicious snapshot pairs so far
Validated 1250/2506 suspicious snapshot pairs so far
Validated 1275/2506 suspicious snapshot pairs so far
Validated 1300/2506 suspicious snapshot pairs so far
Validated 1325/2506 suspicious snapshot pairs so far
Validated 1350/2506 suspicious snapshot pairs so far
Validated 1375/2506 suspicious snapshot pairs so far
Validated 1400/2506 suspicious snapshot pairs so far
Validated 1425/2506 suspicious snapshot pairs so far
Validated 1450/2506 suspicious snapshot pairs so far
Validated 1475/2506 suspicious snapshot pairs so far
Validated 1500/2506 suspicious snapshot pairs so far
Validated 1525/2506 suspicious snapshot pairs so far
Validated 1550/2506 suspicious snapshot pairs so far
Validated 1575/2506 suspicious snapshot pairs so far
Validated 1600/2506 suspicious snapshot pairs so far
Validated 1625/2506 suspicious snapshot pairs so far
Validated 1650/2506 suspicious snapshot pairs so far
Validated 1675/2506 suspicious snapshot pairs so far
Validated 1700/2506 suspicious snapshot pairs so far
Validated 1725/2506 suspicious snapshot pairs so far
Validated 1750/2506 suspicious snapshot pairs so far
Validated 1775/2506 suspicious snapshot pairs so far
Validated 1800/2506 suspicious snapshot pairs so far
Validated 1825/2506 suspicious snapshot pairs so far
Validated 1850/2506 suspicious snapshot pairs so far
Validated 1875/2506 suspicious snapshot pairs so far
Validated 1900/2506 suspicious snapshot pairs so far
Validated 1925/2506 suspicious snapshot pairs so far
Validated 1950/2506 suspicious snapshot pairs so far
Validated 1975/2506 suspicious snapshot pairs so far
Validated 2000/2506 suspicious snapshot pairs so far
Validated 2025/2506 suspicious snapshot pairs so far
Validated 2050/2506 suspicious snapshot pairs so far
Validated 2075/2506 suspicious snapshot pairs so far
Validated 2100/2506 suspicious snapshot pairs so far
Validated 2125/2506 suspicious snapshot pairs so far
Validated 2150/2506 suspicious snapshot pairs so far
Validated 2175/2506 suspicious snapshot pairs so far
Validated 2200/2506 suspicious snapshot pairs so far
Validated 2225/2506 suspicious snapshot pairs so far
Validated 2250/2506 suspicious snapshot pairs so far
Validated 2275/2506 suspicious snapshot pairs so far
Validated 2300/2506 suspicious snapshot pairs so far
Validated 2325/2506 suspicious snapshot pairs so far
Validated 2350/2506 suspicious snapshot pairs so far
Validated 2375/2506 suspicious snapshot pairs so far
Validated 2400/2506 suspicious snapshot pairs so far
Validated 2425/2506 suspicious snapshot pairs so far
Validated 2450/2506 suspicious snapshot pairs so far
Validated 2475/2506 suspicious snapshot pairs so far
Validated 2500/2506 suspicious snapshot pairs so far
suspicious_pairs_found: 2506
smoke_pairs_used: 0
local_invalid_pairs: 0
local_unsupported_pairs: 0
checked_pairs: 2506
valid_pairs: 2417
derivation_errors: 0
rejected_pairs: 0
http_errors: 0
parse_errors: 0
rate_limit_errors: 0
unsupported_pairs: 89
api_errors: 0
unsupported_pair_samples:
AA/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
coreum/XRP [valid xrp] -> rHS2TNSLUyZmGqKxiwQfn7uUH8hZ642pJg: API error: API returned error: {"error": "coin not found"}
FERC/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mph/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
labs/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
aeg/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
combo/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
eqx/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
alita/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
aptr/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
xar/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
astros/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
atem/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
urus/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
axel/MAINNET [valid axelar] -> axelar17pd0kc6r2lfruutg7lqnna5q2n2jags9ndmvzs: API error: API returned error: {"error": "coin not found"}
BROCCOLIF3B/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
BEER/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
ben/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
polx/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mblk/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
smile/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
flz/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bvt/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bonus/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bun/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tsugt/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
CHIPPY/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
combo/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
oik/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
coreum/MAINNET [valid core] -> core108dlz2a6fgc2gzmsl03ct6ktmjf9hdq34dtgs5: API error: API returned error: {"error": "coin not found"}
cec/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
cros/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
DOAI/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
cstar/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
deusd/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
digimon/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
fmc/TRC20 [valid tron] -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error": "coin not found"}
GOME/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
klee/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memetoon/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
nvg/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
EQX/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
micro/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
free/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
rainbow/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
stnd/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
ngc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
ofn/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
OFN/BSC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
zbc/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
neat/NEAR [valid near] -> c1ea807a357b5c8c121bf405c80384f48a628547328fafe0b794c47e6405ece4: API error: API returned error: {"error": "coin not found"}
LPOOL/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memeai/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
cros/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memhash/TON [valid ton] -> EQCTMdAX6yaZ-sVPKzvLrHO_FvdZGAb_ZVcFaepdnjOIhggR: API error: API returned error: {"error": "coin not found"}
vela/Arbitrum [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tenet/MAINNET [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mirai/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
redx/TON [valid ton] -> EQCTMdAX6yaZ-sVPKzvLrHO_FvdZGAb_ZVcFaepdnjOIhggR: API error: API returned error: {"error": "coin not found"}
urus/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
snc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NGC/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SLN/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NAO/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
sln/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SORA/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
solve/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SYNT/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tama/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
sunwukong/TRC20 [valid tron] -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error": "coin not found"}
saitama/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
moz/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
rwa/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
roost/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
prx/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
lsd/APTOS [valid hex-32] -> 0xa4c3845b1f1031ee99adde7991ff9980079edafea0b5b993b2dcf33f7cf50572: API error: API returned error: {"error": "coin not found"}
pmg/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
nwc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NWC/XLM [valid stellar] -> GDA6VAD2GV5VZDASDP2ALSADQT2IUYUFI4ZI7L7AW6KMI7TEAXWOITAL: API error: API returned error: {"error": "coin not found"}
plx/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
uuu/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
naym/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
lead/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
MAX/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
indy/ADA [valid addr] -> addr1v9dd3gtv6je555fpdjwma8f98qqy492lky2n08c7ftslyeg89jvu8: API error: API returned error: {"error": "coin not found"}
hlg/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
GINNAN/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
eths/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
vps/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}

thread 'wallet::trocador_live_validation_test::test_live_trocador_validation_for_locally_suspicious_snapshot_pairs' (312500) panicked at tests/wallet/trocador_live_validation_test.rs:295:5:
Live Trocador snapshot validation found rejects or inconclusive pairs:
suspicious_pairs_found: 2506
smoke_pairs_used: 0
local_invalid_pairs: 0
local_unsupported_pairs: 0
checked_pairs: 2506
valid_pairs: 2417
derivation_errors: 0
rejected_pairs: 0
http_errors: 0
parse_errors: 0
rate_limit_errors: 0
unsupported_pairs: 89
api_errors: 0
unsupported_pair_samples:
AA/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
coreum/XRP [valid xrp] -> rHS2TNSLUyZmGqKxiwQfn7uUH8hZ642pJg: API error: API returned error: {"error": "coin not found"}
FERC/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mph/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
labs/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
aeg/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
combo/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
eqx/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
alita/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
aptr/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
xar/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
astros/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
atem/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
urus/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
axel/MAINNET [valid axelar] -> axelar17pd0kc6r2lfruutg7lqnna5q2n2jags9ndmvzs: API error: API returned error: {"error": "coin not found"}
BROCCOLIF3B/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
BEER/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
ben/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
polx/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mblk/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
smile/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
flz/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bvt/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bonus/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
bun/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tsugt/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
CHIPPY/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
combo/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
oik/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
coreum/MAINNET [valid core] -> core108dlz2a6fgc2gzmsl03ct6ktmjf9hdq34dtgs5: API error: API returned error: {"error": "coin not found"}
cec/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
cros/MATIC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
DOAI/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
cstar/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
deusd/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
digimon/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
fmc/TRC20 [valid tron] -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error": "coin not found"}
GOME/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
klee/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memetoon/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
nvg/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
EQX/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
micro/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
free/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
rainbow/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
stnd/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
ngc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
ofn/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
OFN/BSC [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
zbc/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
neat/NEAR [valid near] -> c1ea807a357b5c8c121bf405c80384f48a628547328fafe0b794c47e6405ece4: API error: API returned error: {"error": "coin not found"}
LPOOL/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memeai/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
cros/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
memhash/TON [valid ton] -> EQCTMdAX6yaZ-sVPKzvLrHO_FvdZGAb_ZVcFaepdnjOIhggR: API error: API returned error: {"error": "coin not found"}
vela/Arbitrum [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tenet/MAINNET [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
mirai/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
redx/TON [valid ton] -> EQCTMdAX6yaZ-sVPKzvLrHO_FvdZGAb_ZVcFaepdnjOIhggR: API error: API returned error: {"error": "coin not found"}
urus/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
snc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NGC/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SLN/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NAO/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
sln/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SORA/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
solve/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
SYNT/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
tama/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
sunwukong/TRC20 [valid tron] -> TU8kwtd2r2ojuyHUYbxu8Vcj9ia2KNzN8f: API error: API returned error: {"error": "coin not found"}
saitama/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
moz/ARBITRUM [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
rwa/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
roost/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
prx/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
lsd/APTOS [valid hex-32] -> 0xa4c3845b1f1031ee99adde7991ff9980079edafea0b5b993b2dcf33f7cf50572: API error: API returned error: {"error": "coin not found"}
pmg/BEP20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
nwc/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
NWC/XLM [valid stellar] -> GDA6VAD2GV5VZDASDP2ALSADQT2IUYUFI4ZI7L7AW6KMI7TEAXWOITAL: API error: API returned error: {"error": "coin not found"}
plx/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
uuu/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
naym/BASE [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
lead/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
MAX/ETH [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
indy/ADA [valid addr] -> addr1v9dd3gtv6je555fpdjwma8f98qqy492lky2n08c7ftslyeg89jvu8: API error: API returned error: {"error": "coin not found"}
hlg/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
GINNAN/SOL [valid solana] -> E3y5f9eKrQ2WPm9kH99H63mqLQnBMFmqR3QneJ29PnUb: API error: API returned error: {"error": "coin not found"}
eths/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
vps/ERC20 [valid evm] -> 0x9858effd232b4033e47d90003d41ec34ecaeda94: API error: API returned error: {"error": "coin not found"}
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test wallet::trocador_live_validation_test::test_live_trocador_validation_for_locally_suspicious_snapshot_pairs ... FAILED

failures:

failures:
    wallet::trocador_live_validation_test::test_live_trocador_validation_for_locally_suspicious_snapshot_pairs

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 137 filtered out; finished in 3369.83s

error: test failed, to rerun pass `--test wallet_tests`
a@a:~/exchange-shared$ 
a@a:~/exchange-shared$ 
a@a:~/exchange-shared$ 




set -a && source .env && \
    export TROCADOR_VALIDATE_MAX_PAIRS=2506 && \
    export TROCADOR_VALIDATE_SAMPLE_LIMIT=5000 && \
    export TROCADOR_VALIDATE_DELAY_MS=500 && \
    set +a && \
    cargo test test_live_trocador_validation_for_current_live_coin_list --test wallet_tests -- --ignored --nocapture