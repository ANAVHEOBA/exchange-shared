#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Stacks" "special" "stacks" "stacks-mainnet" "" "https://api.mainnet.hiro.so/v2/info"
