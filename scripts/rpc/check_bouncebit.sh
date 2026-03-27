#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "BounceBit" "evm" "" "" "" "https://fullnode-mainnet.bouncebitapi.com"
