#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "DeSo" "special" "" "" "" "https://node.deso.org/api/v0/get-exchange-rate"
