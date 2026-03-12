#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "DeFiChain" "special" "" "" "" "https://ocean.defichain.com/v0/mainnet/stats"
