#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    set -a
    source <(cat .env | grep -v '^#' | grep -v '^$')
    set +a
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Trocador & RPC Connectivity Test${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Test counter
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Function to test API endpoint
test_endpoint() {
    local name=$1
    local url=$2
    local expected_code=${3:-200}
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -n "Testing $name... "
    
    response=$(curl -s -w "\n%{http_code}" "$url" -H "API-Key: $TROCADOR_API_KEY" 2>/dev/null)
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" = "$expected_code" ]; then
        echo -e "${GREEN}✓ PASSED${NC} (HTTP $http_code)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}✗ FAILED${NC} (HTTP $http_code)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# Function to test RPC endpoint
test_rpc() {
    local name=$1
    local url=$2
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -n "Testing $name RPC... "
    
    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' 2>/dev/null)
    
    if echo "$response" | jq -e '.result' > /dev/null 2>&1; then
        block=$(echo "$response" | jq -r '.result')
        block_decimal=$((16#${block#0x}))
        echo -e "${GREEN}✓ PASSED${NC} (Block: $block_decimal)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

# Function to test Solana RPC
test_solana_rpc() {
    local name=$1
    local url=$2
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    echo -n "Testing $name RPC... "
    
    response=$(curl -s -X POST "$url" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' 2>/dev/null)
    
    if echo "$response" | jq -e '.result' > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return 1
    fi
}

echo -e "${YELLOW}=== TROCADOR API TESTS ===${NC}\n"

# Test Trocador API Key
if [ -z "$TROCADOR_API_KEY" ]; then
    echo -e "${RED}ERROR: TROCADOR_API_KEY not found in .env${NC}\n"
    exit 1
fi

echo "API Key: ${TROCADOR_API_KEY:0:10}..."

# Test Trocador endpoints
test_endpoint "Coins List" "https://api.trocador.app/coins"
test_endpoint "Exchanges List" "https://api.trocador.app/exchanges"
test_endpoint "Bitcoin Info" "https://api.trocador.app/coin?ticker=btc"
test_endpoint "Ethereum Info" "https://api.trocador.app/coin?ticker=eth"

# Get currency count
echo -n "Fetching total currency count... "
currency_count=$(curl -s -H "API-Key: $TROCADOR_API_KEY" https://api.trocador.app/coins 2>/dev/null | jq 'length')
if [ ! -z "$currency_count" ] && [ "$currency_count" -gt 0 ]; then
    echo -e "${GREEN}✓ Found $currency_count currencies${NC}"
    PASSED_TESTS=$((PASSED_TESTS + 1))
else
    echo -e "${RED}✗ FAILED${NC}"
    FAILED_TESTS=$((FAILED_TESTS + 1))
fi
TOTAL_TESTS=$((TOTAL_TESTS + 1))

echo -e "\n${YELLOW}=== ALCHEMY RPC TESTS ===${NC}\n"

# Test Alchemy API Key
if [ -z "$ALCHEMY_API_KEY" ]; then
    echo -e "${RED}WARNING: ALCHEMY_API_KEY not found in .env${NC}"
    echo "Skipping RPC tests..."
else
    echo "Alchemy Key: ${ALCHEMY_API_KEY:0:10}..."
    
    # Test major EVM chains
    test_rpc "Ethereum" "https://eth-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "Polygon" "https://polygon-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "Arbitrum" "https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "Optimism" "https://opt-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "Base" "https://base-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "BSC" "https://bnb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    test_rpc "Avalanche" "https://avax-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
    
    # Test Solana
    test_solana_rpc "Solana" "https://solana-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY"
fi

echo -e "\n${YELLOW}=== PUBLIC RPC FALLBACK TESTS ===${NC}\n"

# Test public fallback endpoints
test_rpc "Ethereum (Public)" "https://eth.llamarpc.com"
test_rpc "Polygon (Public)" "https://polygon-rpc.com"
test_rpc "BSC (Public)" "https://bsc-dataseed.binance.org"

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Total Tests:  $TOTAL_TESTS"
echo -e "${GREEN}Passed:       $PASSED_TESTS${NC}"
echo -e "${RED}Failed:       $FAILED_TESTS${NC}"

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}✓ All tests passed!${NC}\n"
    exit 0
else
    echo -e "\n${RED}✗ Some tests failed${NC}\n"
    exit 1
fi
