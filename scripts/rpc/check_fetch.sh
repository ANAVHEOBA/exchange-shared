#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Fetch" "special" "" "" "" "https://rpc-fetchhub.fetch.ai:443"
