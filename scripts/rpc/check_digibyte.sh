#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "DigiByte" "special" "digibyte" "" "" "https://digiexplorer.info/api/status"
