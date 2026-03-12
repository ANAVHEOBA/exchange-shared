#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Celo" "evm" "celo" "celo-mainnet" "" "https://forno.celo.org"
