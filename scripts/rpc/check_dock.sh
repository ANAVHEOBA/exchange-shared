#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Dock" "special" "" "" "" "https://dock.api.onfinality.io/public"
