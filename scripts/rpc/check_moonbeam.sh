#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Moonbeam" "evm" "moonbeam" "moonbeam-mainnet" "" "https://rpc.api.moonbeam.network"
