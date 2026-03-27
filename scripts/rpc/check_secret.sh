#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Secret" "special" "" "" "" "https://secretnetwork-rpc.stakely.io"
