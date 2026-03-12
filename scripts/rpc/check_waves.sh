#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Waves" "special" "waves" "" "" "https://nodes.wavesnodes.com/blocks/height"
