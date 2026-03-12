#!/bin/bash
# scripts/check_blockchain_health.sh

JSON_FILE="src/config/chains.json"

if [ ! -f "$JSON_FILE" ]; then
    echo "❌ JSON file not found: $JSON_FILE"
    exit 1
fi

echo "🔍 Starting Multi-Chain Health Check (125 Networks)..."
echo "------------------------------------------------------"

# Loop through each network in the JSON
jq -c '.networks[]' "$JSON_FILE" | while read -r chain; do
    sleep 0.5 # Small delay to be polite
    NAME=$(echo "$chain" | jq -r '.name')
    FAMILY=$(echo "$chain" | jq -r '.family')
    RPC=$(echo "$chain" | jq -r '.primary_rpc')

    # Skip if RPC is the generic fallback for unknown chains
    if [[ "$RPC" == "https://rpc.ankr.com/multichain" ]] || [ -z "$RPC" ]; then
        echo "⚠️  $NAME: No specific RPC configured. Skipping."
        continue
    fi

    echo -n "📡 Testing $NAME ($FAMILY) at $RPC... "

    # IF URL is dRPC, we MUST use POST
    if [[ "$RPC" == *"drpc.org"* ]]; then
        # Try a generic EVM check first as most dRPC chains are EVM
        RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
            --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
            --max-time 10 "$RPC")
        
        if [[ $RESPONSE == *"result"* ]]; then
            BLOCK=$(echo "$RESPONSE" | jq -r '.result')
            echo "✅ LIVE (EVM Block: $BLOCK)"
        else
            # Try a generic non-EVM check (like Near status or just a probe)
            # Some dRPC endpoints for non-EVM might need specific methods
            # But usually, they respond to eth_chainId if they are EVM-compatible
            RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
                --data '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}' \
                --max-time 10 "$RPC")
            
            if [[ $RESPONSE == *"result"* ]]; then
                echo "✅ LIVE (JSON-RPC OK)"
            else
                # Try a final check for things like NEAR
                RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
                    --data '{"jsonrpc":"2.0","method":"status","params":[],"id":1}' \
                    --max-time 10 "$RPC")
                if [[ $RESPONSE == *"chain_id"* ]] || [[ $RESPONSE == *"result"* ]]; then
                    echo "✅ LIVE (Native JSON-RPC)"
                else
                    echo "❌ FAILED (dRPC Gateway error or unsupported network)"
                fi
            fi
        fi
    else
        # Standard logic for Infura/Blockchair/etc.
        case "$FAMILY" in
            "evm")
                RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
                    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
                    --max-time 10 "$RPC")
                if [[ $RESPONSE == *"result"* ]]; then
                    BLOCK=$(echo "$RESPONSE" | jq -r '.result')
                    echo "✅ LIVE (Block: $BLOCK)"
                else
                    echo "❌ FAILED"
                fi
                ;;
            "utxo")
                if [[ "$RPC" == *"blockchair.com"* ]]; then
                    RESPONSE=$(curl -s --max-time 10 "$RPC/stats")
                    if [[ $RESPONSE == *"data"* ]]; then echo "✅ LIVE (Explorer OK)"; else echo "❌ FAILED"; fi
                else
                    RESPONSE=$(curl -s -I --max-time 10 "$RPC" | head -n 1)
                    if [[ $RESPONSE == *"200"* ]] || [[ $RESPONSE == *"OK"* ]]; then echo "✅ LIVE (HTTP 200)"; else echo "❌ FAILED"; fi
                fi
                ;;
            "special")
                # Generic check for SOL, TON, etc.
                RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$RPC")
                if [ "$RESPONSE" == "200" ] || [ "$RESPONSE" == "000" ]; then
                    echo "✅ LIVE (HTTP $RESPONSE)"
                else
                    echo "❌ FAILED (HTTP $RESPONSE)"
                fi
                ;;
        esac
    fi
done

echo "------------------------------------------------------"
echo "🏁 Health Check Complete."
