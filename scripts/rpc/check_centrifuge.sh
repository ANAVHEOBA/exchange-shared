#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Centrifuge" "special" "" "" "" "https://api.centrifuge.io"
