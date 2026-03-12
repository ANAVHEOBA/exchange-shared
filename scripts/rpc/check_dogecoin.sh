#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Dogecoin" "utxo" "dogecoin" "" "" "https://api.blockcypher.com/v1/doge/main"
