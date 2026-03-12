#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Pocket" "evm" "" "" "" "https://pokt-rpc.gateway.pokt.network"
