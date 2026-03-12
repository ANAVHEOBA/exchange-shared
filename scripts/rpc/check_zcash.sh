#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Zcash" "utxo" "zcash" "" "" "https://api.blockchair.com/zcash/stats"
