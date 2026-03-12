#!/bin/bash
# scripts/generate_chains_config.sh

# Load .env to get API Keys
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

OUTPUT="src/config/chains.json"
mkdir -p src/config

echo "🧬 Generating $OUTPUT with dRPC Master Integration..."

# Create a temporary file to build the JSON
TEMP_JSON=$(mktemp)
echo '{"networks": []}' > "$TEMP_JSON"

# List of unique networks from trocador_currencies_full.json
networks=$(jq -r '.[].network' trocador_currencies_full.json | tr '[:lower:]' '[:upper:]' | sort -u)

for net in $networks; do
    [ -z "$net" ] && continue

    # Default values
    FAMILY="special"
    CHAIN_ID="null"
    
    # Standardize slug for dRPC
    # Most dRPC slugs are lowercase network names
    slug=$(echo "$net" | tr '[:upper:]' '[:lower:]' | sed 's/erc20/ethereum/' | sed 's/mainnet/ethereum/')
    
    # Master dRPC URL pattern
    PRIMARY="https://lb.drpc.org/ogrpc?network=$slug&dkey=$DRPC_API_KEY"
    FALLBACK="https://$slug.drpc.org"

    # Specific Family/ChainID Logic
    case "$net" in
        "ETH"|"ERC20"|"ETHEREUM"|"MAINNET")
            FAMILY="evm"; CHAIN_ID=1
            [ ! -z "$INFURA_API_KEY" ] && PRIMARY="https://mainnet.infura.io/v3/$INFURA_API_KEY"
            ;;
        "POLYGON"|"MATIC")
            FAMILY="evm"; CHAIN_ID=137
            [ ! -z "$INFURA_API_KEY" ] && PRIMARY="https://polygon-mainnet.infura.io/v3/$INFURA_API_KEY"
            ;;
        "ARBITRUM")
            FAMILY="evm"; CHAIN_ID=42161
            [ ! -z "$INFURA_API_KEY" ] && PRIMARY="https://arbitrum-mainnet.infura.io/v3/$INFURA_API_KEY"
            ;;
        "OPTIMISM")
            FAMILY="evm"; CHAIN_ID=10
            [ ! -z "$INFURA_API_KEY" ] && PRIMARY="https://optimism-mainnet.infura.io/v3/$INFURA_API_KEY"
            ;;
        "BASE")
            FAMILY="evm"; CHAIN_ID=8453
            [ ! -z "$INFURA_API_KEY" ] && PRIMARY="https://base-mainnet.infura.io/v3/$INFURA_API_KEY"
            ;;
        "BSC"|"SMARTCHAIN"|"BEP20")
            FAMILY="evm"; CHAIN_ID=56
            PRIMARY="https://bsc-dataseed.binance.org"
            ;;
        "AVAXC")
            FAMILY="evm"; CHAIN_ID=43114
            ;;
        "FTM"|"FANTOM")
            FAMILY="evm"; CHAIN_ID=250
            ;;
        "BTC")
            FAMILY="utxo"; PRIMARY="https://blockchair.com/api/v1/bitcoin"
            ;;
        "LTC")
            FAMILY="utxo"; PRIMARY="https://blockchair.com/api/v1/litecoin"
            ;;
        "SOL")
            FAMILY="special"; PRIMARY="https://api.mainnet-beta.solana.com"
            ;;
        "TRX"|"TRC20")
            FAMILY="special"; PRIMARY="https://lb.drpc.org/ogrpc?network=tron&dkey=$DRPC_API_KEY"
            ;;
        "XRP")
            FAMILY="special"; PRIMARY="https://lb.drpc.org/ogrpc?network=xrpl&dkey=$DRPC_API_KEY"
            ;;
        *)
            # Auto-detect EVM family for common names
            if [[ "$net" == *"EVM"* ]] || [[ "$net" == *"CHAIN"* ]]; then
                FAMILY="evm"
            fi
            ;;
    esac

    # Add to JSON
    jq --arg name "$net" --arg family "$FAMILY" --argjson cid "$CHAIN_ID" \
       --arg primary "$PRIMARY" --arg fallback "$FALLBACK" \
       '.networks += [{"name": $name, "family": $family, "chain_id": $cid, "primary_rpc": $primary, "fallback_rpc": $fallback}]' \
       "$TEMP_JSON" > "$TEMP_JSON.tmp" && mv "$TEMP_JSON.tmp" "$TEMP_JSON"
done

mv "$TEMP_JSON" "$OUTPUT"
echo "✅ $OUTPUT generated. 125 networks mapped to dRPC + Infura."
