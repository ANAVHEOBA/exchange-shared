#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Persistence" "special" "" "" "" "https://persistence-rpc.publicnode.com"
