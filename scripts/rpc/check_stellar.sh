#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Stellar" "special" "stellar" "stellar-mainnet" "" "https://horizon.stellar.org"
