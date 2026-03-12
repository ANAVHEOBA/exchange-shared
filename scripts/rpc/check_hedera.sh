#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Hedera" "special" "hedera" "" "" "https://mainnet.mirrornode.hedera.com/api/v1/network/nodes"
