#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Energi" "evm" "" "" "" "https://nodeapi.energi.network"
