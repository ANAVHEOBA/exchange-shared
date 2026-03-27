#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Constellation DAG" "special" "" "" "" "https://l1-lb-mainnet.constellationnetwork.io/cluster/info"
