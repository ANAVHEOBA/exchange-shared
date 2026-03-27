#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Beam" "evm" "" "" "" "https://build.onbeam.com/rpc"
