#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Mina" "utxo" "mina" "" "" "https://api.minaexplorer.com/summary"
