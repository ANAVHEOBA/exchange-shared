#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Sui" "special" "sui" "" "" "https://fullnode.mainnet.sui.io"
