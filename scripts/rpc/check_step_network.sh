#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Step Network" "evm" "" "" "" "https://rpc.step.network"
