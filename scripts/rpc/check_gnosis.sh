#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Gnosis Chain" "evm" "gnosis" "gnosis-mainnet" "" "https://rpc.gnosischain.com"
