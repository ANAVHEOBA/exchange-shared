#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "ZetaChain" "evm" "" "zetachain-mainnet" "" "https://zetachain-evm.blockpi.network/v1/rpc/public"
