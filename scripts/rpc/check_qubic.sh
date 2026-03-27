#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Qubic" "special" "" "" "" "https://api.qubic.li/v1/status"
