#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Mode" "evm" "" "mode-mainnet" "" "https://mainnet.mode.network"
