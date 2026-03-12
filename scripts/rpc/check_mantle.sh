#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Mantle" "evm" "mantle" "mantle-mainnet" "" "https://rpc.mantle.xyz"
