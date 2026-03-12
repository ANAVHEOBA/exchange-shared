#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Kaia" "evm" "kaia" "kaia-mainnet" "" "https://public-node-api.klaytnapi.com/v1/cypress"
