#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Band" "special" "" "" "" "https://laozi1.bandchain.org/api/cosmos/base/tendermint/v1beta1/blocks/latest"
