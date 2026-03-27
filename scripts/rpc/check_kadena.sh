#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Kadena" "special" "" "" "" "https://api.chainweb.com/chainweb/0.0/mainnet01/cut"
