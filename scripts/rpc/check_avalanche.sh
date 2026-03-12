#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Avalanche" "evm" "avalanche" "avax-mainnet" "avalanche-mainnet" "https://api.avax.network/ext/bc/C/rpc"
