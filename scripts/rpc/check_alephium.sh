#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Alephium" "special" "" "" "" "https://node.mainnet.alephium.org/infos/self-clique"
