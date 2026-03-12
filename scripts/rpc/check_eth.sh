#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ethereum" "evm" "eth" "eth-mainnet" "mainnet" "https://eth.llamarpc.com"
