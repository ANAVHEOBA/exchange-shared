#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "TON" "special" "ton" "" "" "https://toncenter.com/api/v2/jsonRPC"
