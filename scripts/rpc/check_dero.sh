#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Dero" "dero" "" "" "" "https://dero-node.mysrv.cloud/json_rpc"
