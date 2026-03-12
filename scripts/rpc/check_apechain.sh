#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "ApeChain" "evm" "" "apechain-mainnet" "" "https://apechain.calderachain.xyz/http"
