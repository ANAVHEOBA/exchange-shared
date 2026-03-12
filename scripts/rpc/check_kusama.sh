#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Kusama" "special" "kusama" "" "" "https://kusama-rpc.polkadot.io"
