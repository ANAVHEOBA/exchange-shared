#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "VeChain" "special" "vechain" "" "" "https://sync-mainnet.vechain.org/blocks/best"
