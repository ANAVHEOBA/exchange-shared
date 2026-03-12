#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "DigiByte" "utxo" "digibyte" "" "" "https://dgb.blockbook.chain49.com/api/v2"
