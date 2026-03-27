#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Axelar" "special" "" "" "" "https://axelar-rpc.publicnode.com/status"
