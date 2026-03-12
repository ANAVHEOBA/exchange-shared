#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Mina" "special" "mina" "" "" "https://api.minaexplorer.com/summary"
