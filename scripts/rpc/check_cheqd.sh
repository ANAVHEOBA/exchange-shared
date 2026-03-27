#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Cheqd" "special" "" "" "" "https://rpc.cheqd.net:443"
