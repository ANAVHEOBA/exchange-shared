#!/usr/bin/env python3
"""
RPC Endpoint Tester and Updater
Fetches working public RPC endpoints from multiple sources and tests connectivity
"""

import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List, Dict

# API Keys
INFURA_API_KEY = "970b0c9fd9c0424ea863ef783a452041"
ALCHEMY_API_KEY = "_BbLKZkEIvBAOFWlMTtFe"

# Known working public RPC endpoints (curated from multiple sources)
# Source: chainlist.org, drpc.org, publicnode.com, etc.
PUBLIC_RPC_ENDPOINTS = {
    # EVM Chains - Chain ID : [primary_rpc, fallback_rpc]
    1: ("https://eth.llamarpc.com", "https://rpc.ankr.com/eth"),
    10: ("https://mainnet.optimism.io", "https://optimism-rpc.publicnode.com"),
    14: ("https://rpc.flare.network/flare", "https://flare-api.publicnode.com"),
    25: ("https://evm.cronos.org", "https://cronos-evm-rpc.publicnode.com"),
    56: ("https://bsc-dataseed.binance.org", "https://bsc-rpc.publicnode.com"),
    137: ("https://polygon-rpc.com", "https://polygon-bor-rpc.publicnode.com"),
    143: ("https://rpc.monad.xyz", "https://monad-rpc.publicnode.com"),
    250: ("https://rpc.ftm.tools", "https://fantom-rpc.publicnode.com"),
    324: ("https://mainnet.era.zksync.io", "https://zksync-rpc.publicnode.com"),
    480: ("https://rpc.worldchain.org", "https://worldchain-rpc.publicnode.com"),
    747: ("https://mainnet.evm.calflow.org", "https://flow-evm-rpc.publicnode.com"),
    1116: ("https://rpc.coredao.org", "https://core-rpc.publicnode.com"),
    1329: ("https://evm-rpc.sei-apis.com", "https://sei-evm-rpc.publicnode.com"),
    1868: ("https://rpc.soneium.org", "https://soneium-rpc.publicnode.com"),
    1923: ("https://mainnet-rpc.swellnetwork.io", "https://swell-rpc.publicnode.com"),
    2020: ("https://api.roninchain.com/ronin", "https://ronin-rpc.publicnode.com"),
    2741: ("https://rpc.abs.xyz", "https://abstract-rpc.publicnode.com"),
    3338: ("https://rpc1.poa.peaq.network", "https://peaq-rpc.publicnode.com"),
    5031: ("https://dream-rpc.somnia.network", "https://somnia-rpc.publicnode.com"),
    5330: ("https://mainnet.superseed.xyz", "https://superseed-rpc.publicnode.com"),
    7897: ("https://rpc.arena-z.games", "https://arena-z-rpc.publicnode.com"),
    8333: ("https://mainnet-rpc.b3.fun", "https://b3-rpc.publicnode.com"),
    42161: ("https://arb1.arbitrum.io/rpc", "https://arbitrum-rpc.publicnode.com"),
    43114: ("https://api.avax.network/ext/bc/C/rpc", "https://avalanche-rpc.publicnode.com"),
    59144: ("https://rpc.linea.build", "https://linea-rpc.publicnode.com"),
    747474: ("https://rpc.katana.kakaroto.org", "https://katana-rpc.publicnode.com"),
    80094: ("https://rpc.berachain.com", "https://berachain-rpc.publicnode.com"),
    8453: ("https://mainnet.base.org", "https://base-rpc.publicnode.com"),
    
    # Add more as needed
}

# Non-EVM chains with known working endpoints
NON_EVM_RPC = {
    "SOL": ("https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com"),
    "ALGO": ("https://mainnet-api.algonode.cloud", "https://mainnet-idx.algonode.cloud"),
    "BTC": ("https://mempool.space/api", "https://blockstream.info/api"),
    "ADA": ("https://api.koios.rest/api/v1", "https://cardano-mainnet.blockfrost.io/api/v0"),
    "NEAR": ("https://rpc.mainnet.near.org", "https://free.rpc.fastnear.com"),
    "HBAR": ("https://mainnet.hashio.io/api", "https://mainnet-public.mirrornode.hedera.com"),
}


def extract_rpc_url(rpc_data) -> Optional[str]:
    """Extract URL from RPC data (handles both string and dict formats)"""
    if isinstance(rpc_data, str):
        return rpc_data
    elif isinstance(rpc_data, dict):
        return rpc_data.get('url')
    return None


def test_evm_rpc(rpc_url: str, timeout: int = 10) -> Tuple[bool, str]:
    """Test EVM RPC with eth_blockNumber"""
    if not rpc_url:
        return False, "No URL"
    
    # Inject API key for Infura URLs
    if 'infura.io/v3/' in rpc_url and INFURA_API_KEY:
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
            return True, f"Block: {block_num:,}"
        error = data.get('error', {}).get('message', 'Unknown error')
        return False, error[:50]
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Connection failed"
    except Exception as e:
        return False, str(e)[:50]


def test_generic_rpc(rpc_url: str, timeout: int = 10) -> Tuple[bool, str]:
    """Test generic HTTP endpoint"""
    if not rpc_url:
        return False, "No URL"
    
    try:
        response = requests.get(rpc_url, timeout=timeout)
        if response.status_code < 400:
            return True, f"HTTP {response.status_code}"
        return False, f"HTTP {response.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Connection failed"
    except Exception as e:
        return False, str(e)[:50]


def fetch_chainlist_data() -> List[Dict]:
    """Fetch latest chain data from chainlist.org"""
    try:
        response = requests.get("https://chainlist.org/rpcs.json", timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Failed to fetch chainlist data: {e}")
        return []


def get_best_rpc(chain_id: Optional[int], name: str, family: str) -> Tuple[Optional[str], Optional[str]]:
    """Get best available RPC endpoints for a chain"""
    
    # Check our curated list first
    if family == "evm" and chain_id and chain_id in PUBLIC_RPC_ENDPOINTS:
        return PUBLIC_RPC_ENDPOINTS[chain_id]
    
    # Check non-EVM chains
    if name.upper() in NON_EVM_RPC:
        return NON_EVM_RPC[name.upper()]
    
    # Try to fetch from chainlist
    chainlist_data = fetch_chainlist_data()
    if chainlist_data:
        for chain in chainlist_data:
            c_chain_id = chain.get('chainId')
            c_name = chain.get('name', '').upper().replace(' ', '').replace('-', '')
            c_short = chain.get('shortName', '').upper()
            
            # Match by chain_id for EVM
            if family == "evm" and chain_id and c_chain_id == chain_id:
                rpcs = chain.get('rpc', [])
                public_rpcs = []
                for rpc in rpcs:
                    url = extract_rpc_url(rpc)
                    if url and not any(x in url for x in ['infura.io/v3/', 'alchemy.com/']):
                        public_rpcs.append(url)
                
                if len(public_rpcs) >= 2:
                    return (public_rpcs[0], public_rpcs[1])
                elif len(public_rpcs) == 1:
                    return (public_rpcs[0], None)
            
            # Match by name
            if c_name == name.upper().replace(' ', '').replace('-', ''):
                rpcs = chain.get('rpc', [])
                for rpc in rpcs:
                    url = extract_rpc_url(rpc)
                    if url and 'llamarpc.com' in url:
                        return (url, None)
    
    return (None, None)


def test_network(network: Dict) -> Dict:
    """Test a single network's RPC endpoints"""
    name = network['name']
    family = network.get('family', 'special')
    chain_id = network.get('chain_id')
    primary_rpc = network.get('primary_rpc')
    fallback_rpc = network.get('fallback_rpc')
    
    # Extract URL if it's a dict
    if isinstance(primary_rpc, dict):
        primary_rpc = primary_rpc.get('url')
    if isinstance(fallback_rpc, dict):
        fallback_rpc = fallback_rpc.get('url')
    
    result = {
        'name': name,
        'family': family,
        'chain_id': chain_id,
        'primary_rpc': primary_rpc,
        'fallback_rpc': fallback_rpc,
        'primary_status': 'N/A',
        'primary_msg': '',
        'fallback_status': 'N/A',
        'fallback_msg': '',
        'working': False
    }
    
    if not primary_rpc or primary_rpc == 'null':
        return result
    
    # Test primary
    if family == 'evm':
        ok, msg = test_evm_rpc(primary_rpc)
    else:
        ok, msg = test_generic_rpc(primary_rpc)
    
    result['primary_status'] = 'OK' if ok else 'FAIL'
    result['primary_msg'] = msg
    
    if ok:
        result['working'] = True
    
    # Test fallback if primary failed
    if not ok and fallback_rpc and fallback_rpc != 'null':
        if family == 'evm':
            ok2, msg2 = test_evm_rpc(fallback_rpc)
        else:
            ok2, msg2 = test_generic_rpc(fallback_rpc)
        result['fallback_status'] = 'OK' if ok2 else 'FAIL'
        result['fallback_msg'] = msg2
        if ok2:
            result['working'] = True
    
    return result


def update_chains_config(chains_file: str = "src/config/chains.json",
                         output_file: str = "src/config/chains_updated.json"):
    """Update chains config with working RPC endpoints"""
    
    print("Loading existing chains config...")
    with open(chains_file, 'r') as f:
        existing_data = json.load(f)
    
    print(f"Found {len(existing_data['networks'])} networks")
    print("\nUpdating RPC endpoints with known working URLs...")
    
    updated_networks = []
    for network in existing_data['networks']:
        name = network['name']
        family = network.get('family', 'special')
        chain_id = network.get('chain_id')
        
        # Get best available RPC
        primary, fallback = get_best_rpc(chain_id, name, family)
        
        updated_network = {
            'name': name,
            'family': family,
            'chain_id': chain_id,
            'primary_rpc': primary if primary else network.get('primary_rpc'),
            'fallback_rpc': fallback if fallback else network.get('fallback_rpc'),
            'updated': primary is not None
        }
        updated_networks.append(updated_network)
    
    # Save updated config
    output_data = {'networks': updated_networks}
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Saved updated config to {output_file}")
    return output_data


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='RPC Endpoint Tester and Updater')
    parser.add_argument('--update', action='store_true', help='Update chains config with working RPCs')
    parser.add_argument('--test', action='store_true', help='Test current RPC endpoints')
    parser.add_argument('--chains-file', default='src/config/chains.json', help='Path to chains config')
    parser.add_argument('--output-file', default='src/config/chains_updated.json', help='Path to output file')
    
    args = parser.parse_args()
    
    if args.update:
        update_chains_config(args.chains_file, args.output_file)
        return
    
    if args.test:
        # Load and test chains
        chains_file = args.output_file if args.output_file else args.chains_file
        print(f"Loading chains from {chains_file}...")
        
        with open(chains_file, 'r') as f:
            data = json.load(f)
        
        networks = data.get('networks', [])
        print(f"Testing {len(networks)} networks...\n")
        
        results = []
        total = 0
        success = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(test_network, net): net for net in networks}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                total += 1
                if result['working']:
                    success += 1
                else:
                    failed += 1
                
                # Print result
                if result['working']:
                    status = "✓"
                    msg = result['primary_msg'] if result['primary_status'] == 'OK' else result['fallback_msg']
                else:
                    status = "✗"
                    msg = f"{result['primary_msg']} | {result['fallback_msg']}"
                
                print(f"{status} {result['name']:20s} {msg[:60]}")
        
        print("\n" + "=" * 80)
        print(f"Total: {total} | Success: {success} | Failed: {failed}")
        if total > 0:
            print(f"Success Rate: {success/total*100:.1f}%")
        
        # Save results
        with open('rpc_test_results.json', 'w') as f:
            json.dump({'results': results, 'summary': {'total': total, 'success': success, 'failed': failed}}, f, indent=2)
        print("\nResults saved to rpc_test_results.json")
        return
    
    # Default: update and test
    print("RPC Endpoint Tester and Updater")
    print("=" * 50)
    print("\nUsage:")
    print("  python3 test_rpc.py --update    Update chains config with working RPCs")
    print("  python3 test_rpc.py --test      Test current RPC endpoints")
    print("  python3 test_rpc.py --update --test  Update then test")
    print()
    
    if args.update:
        update_chains_config(args.chains_file, args.output_file)
    if args.test:
        # Test the updated file
        with open(args.output_file, 'r') as f:
            data = json.load(f)
        
        networks = data.get('networks', [])
        print(f"\nTesting {len(networks)} networks...\n")
        
        results = []
        total = success = failed = 0
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(test_network, net): net for net in networks}
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                total += 1
                if result['working']:
                    success += 1
                else:
                    failed += 1
                
                if result['working']:
                    status = "✓"
                    msg = result['primary_msg'] if result['primary_status'] == 'OK' else result['fallback_msg']
                else:
                    status = "✗"
                    msg = f"{result['primary_msg']} | {result['fallback_msg']}"
                
                print(f"{status} {result['name']:20s} {msg[:60]}")
        
        print("\n" + "=" * 80)
        print(f"Total: {total} | Success: {success} | Failed: {failed}")
        if total > 0:
            print(f"Success Rate: {success/total*100:.1f}%")


if __name__ == "__main__":
    main()
