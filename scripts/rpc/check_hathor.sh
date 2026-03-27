#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Hathor" "special" "" "" "" "https://node1.mainnet.hathor.network/v1a/health"
