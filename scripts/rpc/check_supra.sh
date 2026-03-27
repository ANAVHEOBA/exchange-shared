#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Supra" "evm" "" "" "" "https://rpc-multivm.supra.com/rpc/v1/eth"
