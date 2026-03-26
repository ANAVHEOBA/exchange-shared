#!/bin/bash
source "$(dirname "$0")/lib_rpc.sh"
test_rpc "Neutron" "special" "" "" "" "https://rpc-lb.neutron.org"
