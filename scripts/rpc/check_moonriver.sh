#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Moonriver" "evm" "moonriver" "" "" "https://rpc.api.moonriver.moonbeam.network"
