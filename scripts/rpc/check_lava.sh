#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Lava" "special" "" "" "" "https://lava.tendermintrpc.lava.build:443"
