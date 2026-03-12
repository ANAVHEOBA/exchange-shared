#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Wanchain" "evm" "" "" "" "https://gwan-ssl.wandevs.org:56891"
