#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Gnosis" "evm" "gnosis" "" "" "https://gnosischain-rpc.gateway.pokt.network"
