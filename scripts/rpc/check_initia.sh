#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Initia" "special" "" "" "" "https://rpc.mainnet.initia.xyz/status"
