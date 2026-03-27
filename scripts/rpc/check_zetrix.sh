#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Zetrix" "special" "" "" "" "https://node.zetrix.com/getLedger"
