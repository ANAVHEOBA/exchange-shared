#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Bitcoin Cash" "utxo" "" "" "" "https://api.blockchair.com/bitcoin-cash/stats"
