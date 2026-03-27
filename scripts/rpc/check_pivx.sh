#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "PIVX" "bitcoin_rpc" "" "" "" "https://pivx.publicnode.com"
