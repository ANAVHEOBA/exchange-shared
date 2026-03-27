#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "EthereumPoW" "evm" "" "" "" "https://mainnet.ethereumpow.org"
