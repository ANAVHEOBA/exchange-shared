#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Aeternity" "special" "" "" "" "https://mainnet.aeternity.io/v3/status"
