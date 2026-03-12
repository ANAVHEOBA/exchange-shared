#!/bin/bash

# RPC Endpoint Updater and Tester
# Fetches working RPC endpoints from chainlist.org and tests them

set -e

# API Keys
INFURA_API_KEY="970b0c9fd9c0424ea863ef783a452041"
ALCHEMY_API_KEY="_BbLKZkEIvBAOFWlMTtFe"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

CHAINS_FILE="src/config/chains.json"
CHAINLIST_API="https://chainlist.org/rpcs.json"
OUTPUT_FILE="src/config/chains_updated.json"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   RPC Updater & Connectivity Tester  ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is required but not installed${NC}"
    exit 1
fi

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is required but not installed${NC}"
    exit 1
fi

echo -e "${CYAN}Step 1: Fetching latest RPC endpoints from chainlist.org...${NC}"
echo ""

# Fetch chainlist data
curl -s "$CHAINLIST_API" -o /tmp/chainlist.json 2>/dev/null

if [ ! -s /tmp/chainlist.json ]; then
    echo -e "${RED}Failed to fetch chainlist data${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Fetched $(jq 'length' /tmp/chainlist.json) chains from chainlist.org${NC}"
echo ""

# Create Python script to merge and update chains
cat > /tmp/update_chains.py << 'PYTHON_SCRIPT'
import json
import sys

# Load chainlist data
with open('/tmp/chainlist.json', 'r') as f:
    chainlist_data = json.load(f)

# Load existing chains config
with open('src/config/chains.json', 'r') as f:
    existing_data = json.load(f)

# Create lookup by name/chainId
chainlist_lookup = {}
for chain in chainlist_data:
    chain_id = chain.get('chainId')
    name = chain.get('name', '').upper().replace(' ', '')
    short_name = chain.get('shortName', '').upper()
    rpcs = chain.get('rpc', [])
    
    # Index by chainId
    if chain_id:
        chainlist_lookup[f'id_{chain_id}'] = {
            'rpcs': rpcs,
            'name': chain.get('name', ''),
            'chainId': chain_id,
            'explorers': chain.get('explorers', [])
        }
    
    # Index by name variations
    chainlist_lookup[f'name_{name}'] = {
        'rpcs': rpcs,
        'name': chain.get('name', ''),
        'chainId': chain_id,
        'explorers': chain.get('explorers', [])
    }
    
    if short_name:
        chainlist_lookup[f'short_{short_name}'] = {
            'rpcs': rpcs,
            'name': chain.get('name', ''),
            'chainId': chain_id,
            'explorers': chain.get('explorers', [])
        }

# Update existing networks
updated_networks = []
for network in existing_data['networks']:
    name = network.get('name', '')
    chain_id = network.get('chain_id')
    family = network.get('family', 'special')
    
    # Try to find matching chain in chainlist
    matching_chain = None
    
    # Try by chain_id for EVM chains
    if chain_id and family == 'evm':
        matching_chain = chainlist_lookup.get(f'id_{chain_id}')
    
    # Try by name
    if not matching_chain:
        name_key = f'name_{name.upper().replace(" ", "")}'
        matching_chain = chainlist_lookup.get(name_key)
    
    if matching_chain and matching_chain.get('rpcs'):
        rpcs = matching_chain['rpcs']
        # Filter out URLs that need API keys (infura, alchemy, ankr with paths)
        public_rpcs = []
        for rpc in rpcs:
            if not any(x in rpc for x in ['infura.io/v3/', 'alchemy.com/', 'ankr.com/']):
                public_rpcs.append(rpc)
        
        # Use first public RPC as primary, second as fallback
        primary_rpc = public_rpcs[0] if public_rpcs else network.get('primary_rpc')
        fallback_rpc = public_rpcs[1] if len(public_rpcs) > 1 else network.get('fallback_rpc')
        
        updated_network = {
            'name': name,
            'family': family,
            'chain_id': chain_id,
            'primary_rpc': primary_rpc,
            'fallback_rpc': fallback_rpc,
            'source': 'chainlist' if matching_chain else 'original'
        }
    else:
        updated_network = {
            'name': name,
            'family': family,
            'chain_id': chain_id,
            'primary_rpc': network.get('primary_rpc'),
            'fallback_rpc': network.get('fallback_rpc'),
            'source': 'original'
        }
    
    updated_networks.append(updated_network)

# Save updated config
output_data = {'networks': updated_networks}
with open('src/config/chains_updated.json', 'w') as f:
    json.dump(output_data, f, indent=2)

print(f"Updated {len(updated_networks)} networks")
PYTHON_SCRIPT

echo -e "${CYAN}Step 2: Processing and updating RPC endpoints...${NC}"
python3 /tmp/update_chains.py
echo ""

# Copy updated file
cp "$OUTPUT_FILE" "${CHAINS_FILE}.backup"
echo -e "${GREEN}✓ Backup created: ${CHAINS_FILE}.backup${NC}"
echo ""

# Now test the updated endpoints
echo -e "${CYAN}Step 3: Testing updated RPC endpoints...${NC}"
echo ""

# Create test script
cat > /tmp/test_updated.py << 'PYTHON_TEST'
import json
import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

INFURA_API_KEY = "970b0c9fd9c0424ea863ef783a452041"

def test_evm_rpc(rpc_url, name, timeout=10):
    """Test EVM RPC with eth_blockNumber"""
    if 'infura.io/v3/' in rpc_url:
        rpc_url = rpc_url + INFURA_API_KEY
    
    try:
        response = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
            headers={"Content-Type": "application/json"},
            timeout=timeout
        )
        data = response.json()
        if 'result' in data:
            block_num = int(data['result'], 16)
            return True, f"Block: {block_num}"
        return False, data.get('error', {}).get('message', 'Unknown error')
    except Exception as e:
        return False, str(e)

def test_generic_rpc(rpc_url, name, timeout=10):
    """Test generic HTTP endpoint"""
    if 'infura.io/v3/' in rpc_url:
        rpc_url = rpc_url + INFURA_API_KEY
    
    try:
        response = requests.get(rpc_url, timeout=timeout)
        if response.status_code < 400:
            return True, f"HTTP {response.status_code}"
        return False, f"HTTP {response.status_code}"
    except Exception as e:
        return False, str(e)

# Load updated chains
with open('src/config/chains_updated.json', 'r') as f:
    data = json.load(f)

results = []
total = 0
success = 0
failed = 0

def test_network(network):
    name = network['name']
    family = network.get('family', 'special')
    primary_rpc = network.get('primary_rpc')
    fallback_rpc = network.get('fallback_rpc')
    source = network.get('source', 'unknown')
    
    result = {
        'name': name,
        'family': family,
        'source': source,
        'primary_status': 'N/A',
        'primary_msg': '',
        'fallback_status': 'N/A',
        'fallback_msg': ''
    }
    
    if not primary_rpc or primary_rpc == 'null':
        return result
    
    # Test primary
    if family == 'evm' and primary_rpc:
        ok, msg = test_evm_rpc(primary_rpc, name)
    else:
        ok, msg = test_generic_rpc(primary_rpc, name)
    
    result['primary_status'] = '✓' if ok else '✗'
    result['primary_msg'] = msg
    
    # Test fallback if primary failed
    if not ok and fallback_rpc and fallback_rpc != 'null':
        if family == 'evm':
            ok2, msg2 = test_evm_rpc(fallback_rpc, name + " (fallback)")
        else:
            ok2, msg2 = test_generic_rpc(fallback_rpc, name + " (fallback)")
        result['fallback_status'] = '✓' if ok2 else '✗'
        result['fallback_msg'] = msg2
    
    return result

# Test with thread pool for speed
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(test_network, net): net for net in data['networks']}
    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        
        total += 1
        if result['primary_status'] == '✓' or result['fallback_status'] == '✓':
            success += 1
        else:
            failed += 1
        
        # Print result
        status_color = GREEN if result['primary_status'] == '✓' else RED
        print(f"{status_color}{result['primary_status']}{NC} {result['name']:20s} Primary: {result['primary_msg'][:40]:40s} Fallback: {result['fallback_msg'][:40]:40s} [{result['source']}]")

print()
print("=" * 60)
print(f"Total: {total} | Success: {success} | Failed: {failed}")
print(f"Success Rate: {success/total*100:.1f}%" if total > 0 else "")
PYTHON_TEST

python3 -c "
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

INFURA_API_KEY = '$INFURA_API_KEY'

def test_evm_rpc(rpc_url, name, timeout=10):
    if 'infura.io/v3/' in rpc_url:
        rpc_url = rpc_url + INFURA_API_KEY
    
    try:
        response = requests.post(
            rpc_url,
            json={'jsonrpc': '2.0', 'method': 'eth_blockNumber', 'params': [], 'id': 1},
            headers={'Content-Type': 'application/json'},
            timeout=timeout
        )
        data = response.json()
        if 'result' in data:
            block_num = int(data['result'], 16)
            return True, f'Block: {block_num}'
        return False, data.get('error', {}).get('message', 'Unknown error')
    except Exception as e:
        return False, str(e)

def test_generic_rpc(rpc_url, name, timeout=10):
    if 'infura.io/v3/' in rpc_url:
        rpc_url = rpc_url + INFURA_API_KEY
    
    try:
        response = requests.get(rpc_url, timeout=timeout)
        if response.status_code < 400:
            return True, f'HTTP {response.status_code}'
        return False, f'HTTP {response.status_code}'
    except Exception as e:
        return False, str(e)

with open('src/config/chains_updated.json', 'r') as f:
    data = json.load(f)

results = []
total = 0
success = 0
failed = 0

def test_network(network):
    name = network['name']
    family = network.get('family', 'special')
    primary_rpc = network.get('primary_rpc')
    fallback_rpc = network.get('fallback_rpc')
    source = network.get('source', 'unknown')
    
    result = {
        'name': name,
        'family': family,
        'source': source,
        'primary_status': 'N/A',
        'primary_msg': '',
        'fallback_status': 'N/A',
        'fallback_msg': ''
    }
    
    if not primary_rpc or primary_rpc == 'null':
        return result
    
    if family == 'evm' and primary_rpc:
        ok, msg = test_evm_rpc(primary_rpc, name)
    else:
        ok, msg = test_generic_rpc(primary_rpc, name)
    
    result['primary_status'] = 'OK' if ok else 'FAIL'
    result['primary_msg'] = msg[:50]
    
    if not ok and fallback_rpc and fallback_rpc != 'null':
        if family == 'evm':
            ok2, msg2 = test_evm_rpc(fallback_rpc, name + ' (fallback)')
        else:
            ok2, msg2 = test_generic_rpc(fallback_rpc, name + ' (fallback)')
        result['fallback_status'] = 'OK' if ok2 else 'FAIL'
        result['fallback_msg'] = msg2[:50]
    
    return result

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(test_network, net): net for net in data['networks']}
    for future in as_completed(futures):
        result = future.result()
        results.append(result)
        
        total += 1
        if result['primary_status'] == 'OK' or result['fallback_status'] == 'OK':
            success += 1
        else:
            failed += 1
        
        status = '✓' if result['primary_status'] == 'OK' else '✗'
        print(f'{status} {result[\"name\"]:20s} Primary: {result[\"primary_msg\"]:50s} Fallback: {result[\"fallback_msg\"]:50s} [{result[\"source\"]}]')

print()
print('=' * 80)
print(f'Total: {total} | Success: {success} | Failed: {failed}')
if total > 0:
    print(f'Success Rate: {success/total*100:.1f}%')
"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}              Complete                  ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Updated config saved to: ${OUTPUT_FILE}"
echo -e "Backup saved to: ${CHAINS_FILE}.backup"
echo ""
echo -e "${YELLOW}To apply the updated config:${NC}"
echo "  cp ${OUTPUT_FILE} ${CHAINS_FILE}"
