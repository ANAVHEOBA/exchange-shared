#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Flare" "evm" "flare" "flare-mainnet" "" "https://flare-api.flare.network"
