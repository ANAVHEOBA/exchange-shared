#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Dash" "utxo" "dash" "" "" "https://api.blockchair.com/dash/stats"
