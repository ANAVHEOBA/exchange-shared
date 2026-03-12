#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Filecoin" "evm" "filecoin" "filecoin-mainnet" "" "https://rpc.ankr.com/filecoin"
