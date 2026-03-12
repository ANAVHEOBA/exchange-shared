#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Zano" "special" "" "" "" "https://explorer.zano.org/api/get_info"
