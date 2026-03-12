#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Stratis EVM" "evm" "" "" "" "https://rpc.stratisevm.com"
