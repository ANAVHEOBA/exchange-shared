#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "PulseChain" "evm" "" "" "" "https://rpc.pulsechain.com"
