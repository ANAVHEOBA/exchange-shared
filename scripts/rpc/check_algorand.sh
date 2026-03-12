#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Algorand" "special" "algorand" "algorand-mainnet" "" "https://mainnet-api.algonode.cloud/v2/status"
