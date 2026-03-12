#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Core DAO" "evm" "core" "" "" "https://rpc.coredao.org"
