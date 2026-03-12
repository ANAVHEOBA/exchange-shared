#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Factom" "special" "" "" "" "https://api.factomd.net/v2"
