#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Bitcoin" "utxo" "bitcoin" "" "" "https://mempool.space/api/blocks/tip/height"
