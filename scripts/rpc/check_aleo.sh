#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Aleo" "special" "" "" "" "https://api.explorer.aleo.org/v1/mainnet/latest/block"
