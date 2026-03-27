#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Meter" "evm" "" "" "" "https://rpc.meter.io"
