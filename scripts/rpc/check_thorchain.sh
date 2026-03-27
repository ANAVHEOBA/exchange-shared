#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "THORChain" "special" "" "" "" "https://thornode.ninerealms.com/thorchain/inbound_addresses"
