#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Monad" "evm" "monad" "monad-mainnet" "" "https://rpc.monad.xyz"
