#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Starknet" "special" "starknet" "starknet-mainnet" "starknet-mainnet" "https://starknet-mainnet.public.blastapi.io"
