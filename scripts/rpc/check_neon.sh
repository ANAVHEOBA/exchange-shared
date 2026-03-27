#!/bin/bash
source $(dirname "$0")/lib_rpc.sh
test_rpc "Neon" "evm" "" "" "" "https://neon-proxy-mainnet.solana.p2p.org"
