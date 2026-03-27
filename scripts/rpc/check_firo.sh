#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Firo" "bitcoin_rpc" "" "" "" "https://firo-rpc.publicnode.com"
