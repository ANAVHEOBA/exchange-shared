#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "ZelCash (Flux)" "special" "" "" "" "https://explorer.runonflux.io/api/status"
