#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "IOTA EVM" "evm" "iota" "" "" "https://json-rpc.evm.iotaledger.net"
