#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Tezos (Legacy)" "special" "" "" "" "https://mainnet.api.tez.ie"
