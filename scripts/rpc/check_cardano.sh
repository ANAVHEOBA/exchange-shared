#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Cardano" "cardano" "cardano" "cardano-mainnet" "" "https://api.koios.rest/api/v1/tip"
