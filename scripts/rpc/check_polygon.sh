#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Polygon" "evm" "polygon" "polygon-mainnet" "polygon-mainnet" "https://polygon.llamarpc.com"
