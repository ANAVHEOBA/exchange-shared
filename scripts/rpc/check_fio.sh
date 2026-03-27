#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "FIO" "special" "" "" "" "https://fio.greymass.com/v1/chain/get_info"
