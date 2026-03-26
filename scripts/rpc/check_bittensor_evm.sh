#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Bittensor EVM" "evm" "" "" "" "https://lite.chain.opentensor.ai"
