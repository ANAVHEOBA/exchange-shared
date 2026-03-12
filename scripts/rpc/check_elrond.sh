#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "MultiversX" "special" "elrond" "" "" "https://gateway.multiversx.com/network/config"
