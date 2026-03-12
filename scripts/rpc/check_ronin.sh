#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ronin" "evm" "ronin" "" "" "https://api.roninchain.com/rpc"
