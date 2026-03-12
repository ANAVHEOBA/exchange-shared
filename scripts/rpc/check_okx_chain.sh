#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "OKX Chain" "evm" "" "" "" "https://exchainrpc.okex.org"
