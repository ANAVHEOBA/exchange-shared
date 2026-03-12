#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Blast" "evm" "blast" "blast-mainnet" "" "https://rpc.blast.io"
