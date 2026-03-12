#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "HyperEVM" "evm" "" "" "" "https://rpc.hyperliquid.xyz/evm"
