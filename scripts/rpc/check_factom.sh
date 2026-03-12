#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Factom/Accumulate" "special" "" "" "" "https://mainnet.accumulatenetwork.io/v2"
