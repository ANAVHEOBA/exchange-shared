#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Sonic" "evm" "sonic" "sonic-mainnet" "" "https://rpc.soniclabs.com"
