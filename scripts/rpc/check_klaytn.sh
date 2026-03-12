#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Kaia (Legacy)" "evm" "kaia" "kaia-mainnet" "" "https://public-node-api.klaytnapi.com/v1/cypress"
