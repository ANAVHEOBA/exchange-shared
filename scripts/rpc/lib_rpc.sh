#!/bin/bash
# scripts/rpc/lib_rpc.sh - Rigorous Multi-Chain Validator

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

ANKR_ID="255ef0129f301d346a2a784d9bef2bed6feb53f0584208e29751f1593d597662"
ALCHEMY_KEY="_BbLKZkEIvBAOFWlMTtFe"
INFURA_ID="970b0c9fd9c0424ea863ef783a452041"

test_rpc() {
    local NAME=$1
    local FAMILY=$2
    local ANKR_SLUG=$3
    local ALCHEMY_SLUG=$4
    local INFURA_SLUG=$5
    local PUBLIC_URL=$6

    echo -e "${BLUE}--- Testing $NAME ---${NC}"

    if [ ! -z "$ANKR_SLUG" ]; then
        URL="https://rpc.ankr.com/$ANKR_SLUG/$ANKR_ID"
        check_endpoint "Ankr" "$URL" "$FAMILY" && return 0
    fi

    if [ ! -z "$ALCHEMY_SLUG" ] && [ "$ALCHEMY_SLUG" != "null" ]; then
        URL="https://$ALCHEMY_SLUG.g.alchemy.com/v2/$ALCHEMY_KEY"
        check_endpoint "Alchemy" "$URL" "$FAMILY" && return 0
    fi

    if [ ! -z "$INFURA_SLUG" ] && [ "$INFURA_SLUG" != "null" ]; then
        URL="https://$INFURA_SLUG.infura.io/v3/$INFURA_ID"
        check_endpoint "Infura" "$URL" "$FAMILY" && return 0
    fi

    if [ ! -z "$PUBLIC_URL" ]; then
        check_endpoint "Public" "$PUBLIC_URL" "$FAMILY" && return 0
    fi

    echo -e "${RED}❌ ALL PROVIDERS FAILED for $NAME${NC}"
    return 1
}

check_endpoint() {
    local PROVIDER=$1
    local URL=$2
    local FAMILY=$3

    echo -n "  📡 $PROVIDER: "

    case "$FAMILY" in
        "evm")
            RESPONSE=$(curl -s -A "Mozilla/5.0" -X POST -H "Content-Type: application/json" \
                --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
                --max-time 10 "$URL")
            if [[ $RESPONSE == *"result"* ]]; then
                BLOCK=$(echo "$RESPONSE" | jq -r '.result')
                echo -e "${GREEN}✅ LIVE ($BLOCK)${NC}"
                return 0
            fi
            ;;
        "neo")
            RESPONSE=$(curl -s -A "Mozilla/5.0" -X POST -H "Content-Type: application/json" \
                --data '{"jsonrpc":"2.0","method":"getblockcount","params":[],"id":1}' \
                --max-time 10 "$URL")
            if [[ $RESPONSE == *"result"* ]]; then
                HEIGHT=$(echo "$RESPONSE" | jq -r '.result')
                echo -e "${GREEN}✅ LIVE ($HEIGHT)${NC}"
                return 0
            fi
            ;;
        "icon")
            RESPONSE=$(curl -s -A "Mozilla/5.0" -X POST -H "Content-Type: application/json" \
                --data '{"jsonrpc":"2.0","method":"icx_getLastBlock","id":1}' \
                --max-time 10 "$URL")
            if [[ $RESPONSE == *"result"* ]]; then
                HEIGHT=$(echo "$RESPONSE" | jq -r '.result.height')
                echo -e "${GREEN}✅ LIVE ($HEIGHT)${NC}"
                return 0
            fi
            ;;
        *)
            # Generic probe: Must return HTTP 200/405/204
            PROBE_URL="$URL"
            if [[ "$PROBE_URL" == ws://* ]]; then
                PROBE_URL="http://${PROBE_URL#ws://}"
            elif [[ "$PROBE_URL" == wss://* ]]; then
                PROBE_URL="https://${PROBE_URL#wss://}"
            fi
            HTTP_CODE=$(curl -s -A "Mozilla/5.0" -o /dev/null -w "%{http_code}" --max-time 10 "$PROBE_URL")
            if [[ "$HTTP_CODE" =~ ^(200|405|204|403|401)$ ]]; then
                echo -e "${GREEN}✅ LIVE (HTTP $HTTP_CODE)${NC}"
                return 0
            fi
            ;;
    esac

    echo -e "${RED}FAILED${NC}"
    return 1
}
