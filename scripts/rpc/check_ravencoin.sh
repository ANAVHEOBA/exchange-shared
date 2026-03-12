#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ravencoin" "utxo" "" "" "" "https://rvn.2miners.com/api/stats"
