#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ergo" "special" "" "" "" "https://api.ergoplatform.com/api/v1/networkState"
