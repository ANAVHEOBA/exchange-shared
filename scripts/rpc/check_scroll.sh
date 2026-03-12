#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Scroll" "evm" "scroll" "scroll-mainnet" "" "https://rpc.scroll.io"
