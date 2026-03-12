#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Ontology" "evm" "" "" "" "https://dappnode1.ont.io:10339"
