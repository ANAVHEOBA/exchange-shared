#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Stargaze" "special" "" "" "" "https://stargaze-rpc.polkachu.com/status"
