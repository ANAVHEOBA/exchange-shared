#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ark" "special" "" "" "" "https://api.ark.io/api/node/status"
