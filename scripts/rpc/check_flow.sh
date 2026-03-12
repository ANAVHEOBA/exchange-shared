#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Flow" "special" "flow" "flow-mainnet" "" "https://rest-mainnet.onflow.org/v1/blocks?height=sealed"
