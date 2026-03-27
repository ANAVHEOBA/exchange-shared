#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Steem" "steem" "" "" "" "https://api.steemit.com"
