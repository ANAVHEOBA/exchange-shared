#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ternoa zkEVM" "evm" "" "" "" "https://rpc-mainnet.zkevm.ternoa.network"
