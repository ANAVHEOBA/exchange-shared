#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Avalanche C-Chain" "evm" "avalanche" "avax-mainnet" "avalanche-mainnet" "https://api.avax.network/ext/bc/C/rpc"
