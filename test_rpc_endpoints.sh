#!/bin/bash

# RPC Endpoint Tester for chains.json
# Tests connectivity to all blockchain RPC endpoints

# Don't exit on error - we expect some failures

# API Keys
INFURA_API_KEY="970b0c9fd9c0424ea863ef783a452041"
ALCHEMY_API_KEY="_BbLKZkEIvBAOFWlMTtFe"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL=0
SUCCESS=0
FAILED=0

# Chains JSON file
CHAINS_FILE="src/config/chains.json"

if [ ! -f "$CHAINS_FILE" ]; then
    echo -e "${RED}Error: $CHAINS_FILE not found${NC}"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   RPC Endpoint Connectivity Tester   ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to test EVM RPC endpoint
test_evm_rpc() {
    local rpc_url="$1"
    local name="$2"
    
    # Inject API key for Infura URLs
    if [[ "$rpc_url" == *"infura.io/v3/"* ]]; then
        rpc_url="${rpc_url}${INFURA_API_KEY}"
    fi
    
    # Inject API key for Alchemy URLs
    if [[ "$rpc_url" == *"alchemy.com/"* ]]; then
        rpc_url="${rpc_url}${ALCHEMY_API_KEY}"
    fi
    
    # Test with eth_blockNumber RPC call
    local response
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$rpc_url" \
        --max-time 10 \
        2>/dev/null)
    
    if echo "$response" | grep -q '"result"'; then
        local block_num
        block_num=$(echo "$response" | grep -o '"result":"[^"]*"' | cut -d'"' -f4)
        echo -e "${GREEN}✓${NC} $name (Primary) - Block: $block_num"
        return 0
    else
        echo -e "${RED}✗${NC} $name (Primary) - Failed"
        return 1
    fi
}

# Function to test generic HTTP endpoint
test_generic_rpc() {
    local rpc_url="$1"
    local name="$2"
    
    # Inject API key for Infura URLs
    if [[ "$rpc_url" == *"infura.io/v3/"* ]]; then
        rpc_url="${rpc_url}${INFURA_API_KEY}"
    fi
    
    # Inject API key for Alchemy URLs
    if [[ "$rpc_url" == *"alchemy.com/"* ]]; then
        rpc_url="${rpc_url}${ALCHEMY_API_KEY}"
    fi
    
    # Inject API key for Blockfrost (Cardano)
    if [[ "$rpc_url" == *"blockfrost.io/api"* ]]; then
        rpc_url="${rpc_url}/${ALCHEMY_API_KEY}"
    fi
    
    # Test with simple GET request
    local http_code
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "$rpc_url" --max-time 10 2>/dev/null)
    
    if [[ "$http_code" =~ ^[23] ]]; then
        echo -e "${GREEN}✓${NC} $name (Primary) - HTTP $http_code"
        return 0
    else
        echo -e "${RED}✗${NC} $name (Primary) - HTTP $http_code"
        return 1
    fi
}

# Function to test fallback endpoint
test_fallback() {
    local rpc_url="$1"
    local name="$2"
    local family="$3"
    
    if [ -z "$rpc_url" ] || [ "$rpc_url" == "null" ]; then
        echo -e "${YELLOW}○${NC} $name (Fallback) - No fallback configured"
        return 2
    fi
    
    if [ "$family" == "evm" ]; then
        # Inject API key for Infura URLs
        if [[ "$rpc_url" == *"infura.io/v3/"* ]]; then
            rpc_url="${rpc_url}${INFURA_API_KEY}"
        fi
        
        local response
        response=$(curl -s -X POST \
            -H "Content-Type: application/json" \
            --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
            "$rpc_url" \
            --max-time 10 \
            2>/dev/null)
        
        if echo "$response" | grep -q '"result"'; then
            local block_num
            block_num=$(echo "$response" | grep -o '"result":"[^"]*"' | cut -d'"' -f4)
            echo -e "${GREEN}✓${NC} $name (Fallback) - Block: $block_num"
            return 0
        else
            echo -e "${RED}✗${NC} $name (Fallback) - Failed"
            return 1
        fi
    else
        # Inject API key for Infura URLs
        if [[ "$rpc_url" == *"infura.io/v3/"* ]]; then
            rpc_url="${rpc_url}${INFURA_API_KEY}"
        fi
        
        local http_code
        http_code=$(curl -s -o /dev/null -w "%{http_code}" "$rpc_url" --max-time 10 2>/dev/null)
        
        if [[ "$http_code" =~ ^[23] ]]; then
            echo -e "${GREEN}✓${NC} $name (Fallback) - HTTP $http_code"
            return 0
        else
            echo -e "${RED}✗${NC} $name (Fallback) - HTTP $http_code"
            return 1
        fi
    fi
}

# Parse and test each network
echo -e "${BLUE}Testing Primary RPC Endpoints...${NC}"
echo ""

# Extract networks using jq or python
if command -v jq &> /dev/null; then
    # Use jq
    count=$(jq '.networks | length' "$CHAINS_FILE")
    
    for ((i=0; i<count; i++)); do
        name=$(jq -r ".networks[$i].name" "$CHAINS_FILE")
        family=$(jq -r ".networks[$i].family" "$CHAINS_FILE")
        primary_rpc=$(jq -r ".networks[$i].primary_rpc" "$CHAINS_FILE")
        fallback_rpc=$(jq -r ".networks[$i].fallback_rpc" "$CHAINS_FILE")
        
        if [ "$primary_rpc" == "null" ] || [ -z "$primary_rpc" ]; then
            continue
        fi
        
        TOTAL=$((TOTAL + 1))
        
        if [ "$family" == "evm" ]; then
            if test_evm_rpc "$primary_rpc" "$name"; then
                SUCCESS=$((SUCCESS + 1))
            else
                FAILED=$((FAILED + 1))
                # Try fallback
                test_fallback "$fallback_rpc" "$name" "$family"
            fi
        else
            if test_generic_rpc "$primary_rpc" "$name"; then
                SUCCESS=$((SUCCESS + 1))
            else
                FAILED=$((FAILED + 1))
                # Try fallback
                test_fallback "$fallback_rpc" "$name" "$family"
            fi
        fi
    done
elif command -v python3 &> /dev/null; then
    # Use python as fallback
    python3 -c "
import json
with open('$CHAINS_FILE', 'r') as f:
    data = json.load(f)
    for net in data['networks']:
        print(f\"{net['name']}|{net['family']}|{net['primary_rpc']}|{net.get('fallback_rpc', '')}\")
    " | while IFS='|' read -r name family primary_rpc fallback_rpc; do
        
        if [ "$primary_rpc" == "null" ] || [ -z "$primary_rpc" ]; then
            continue
        fi
        
        TOTAL=$((TOTAL + 1))
        
        if [ "$family" == "evm" ]; then
            if test_evm_rpc "$primary_rpc" "$name"; then
                SUCCESS=$((SUCCESS + 1))
            else
                FAILED=$((FAILED + 1))
                test_fallback "$fallback_rpc" "$name" "$family"
            fi
        else
            if test_generic_rpc "$primary_rpc" "$name"; then
                SUCCESS=$((SUCCESS + 1))
            else
                FAILED=$((FAILED + 1))
                test_fallback "$fallback_rpc" "$name" "$family"
            fi
        fi
    done
else
    echo -e "${RED}Error: Neither jq nor python3 is available${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}              Summary                 ${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Total Tested:  $TOTAL"
echo -e "${GREEN}Successful:    $SUCCESS${NC}"
echo -e "${RED}Failed:        $FAILED${NC}"
echo ""

if [ $FAILED -gt 0 ]; then
    echo -e "${YELLOW}Note: Some endpoints may require specific API keys or have rate limits${NC}"
fi
