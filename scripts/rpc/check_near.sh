#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "NEAR" "special" "near" "near-mainnet" "" "https://rpc.mainnet.near.org"
