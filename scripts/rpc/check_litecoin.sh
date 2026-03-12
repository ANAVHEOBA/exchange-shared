#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Litecoin" "utxo" "litecoin" "" "" "https://api.blockcypher.com/v1/ltc/main"
