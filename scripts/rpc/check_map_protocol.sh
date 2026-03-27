#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "MAP Protocol" "evm" "" "" "" "https://rpc.maplabs.io"
