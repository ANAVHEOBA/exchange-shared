#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Syscoin NEVM" "evm" "syscoin" "" "" "https://rpc.syscoin.org"
