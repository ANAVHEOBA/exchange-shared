#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Monero" "monero" "" "" "" "https://xmr-node.cakewallet.com:18081/get_info"
