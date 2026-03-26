Primary Network
Learn about the Avalanche Primary Network and its three blockchains.

Avalanche is a heterogeneous network of blockchains. As opposed to homogeneous networks, where all applications reside in the same chain, heterogeneous networks allow separate chains to be created for different applications.

Primary Network Architecture

The Primary Network is a special Avalanche L1 that runs three blockchains:

The Contract Chain (C-Chain)
The Platform Chain (P-Chain)
The Exchange Chain (X-Chain)
Note

Avalanche Mainnet is comprised of the Primary Network and all deployed Avalanche L1s.

A node can become a validator for the Primary Network by staking at least 2,000 AVAX.

C-Chain (Contract Chain)
The C-Chain is an implementation of the Ethereum Virtual Machine (EVM). The C-Chain's API supports Geth's API and supports the deployment and execution of smart contracts written in Solidity.

The C-Chain is an instance of the Coreth Virtual Machine.

Property	Mainnet	Fuji Testnet
Network Name	Avalanche C-Chain	Avalanche Fuji C-Chain
Chain ID	43114 (0xA86A)	43113 (0xA869)
Currency	AVAX	AVAX
RPC URL	https://api.avax.network/ext/bc/C/rpc	https://api.avax-test.network/ext/bc/C/rpc
Explorer	https://subnets.avax.network/c-chain	https://subnets-test.avax.network/c-chain
Faucet	-	Get Test AVAX
Add to Wallet	Add to Wallet	Add to Wallet
P-Chain (Platform Chain)
The P-Chain is responsible for all validator and Avalanche L1-level operations. The P-Chain API supports the creation of new blockchains and Avalanche L1s, the addition of validators to Avalanche L1s, staking operations, and other platform-level operations.

The P-Chain is an instance of the Platform Virtual Machine.

Property	Mainnet	Fuji Testnet
RPC URL	https://api.avax.network/ext/bc/P	https://api.avax-test.network/ext/bc/P
Currency	AVAX	AVAX
Explorer	https://subnets.avax.network/p-chain	https://subnets-test.avax.network/p-chain
X-Chain (Exchange Chain)
The X-Chain is responsible for operations on digital smart assets known as Avalanche Native Tokens. A smart asset is a representation of a real-world resource (for example, equity, or a bond) with sets of rules that govern its behavior, like "can't be traded until tomorrow." The X-Chain API supports the creation and trade of Avalanche Native Tokens.

One asset traded on the X-Chain is AVAX. When you issue a transaction to a blockchain on Avalanche, you pay a fee denominated in AVAX.

The X-Chain is an instance of the Avalanche Virtual Machine (AVM).

Property	Mainnet	Fuji Testnet
RPC URL	https://api.avax.network/ext/bc/X	https://api.avax-test.network/ext/bc/X
Currency	AVAX	AVAX
Explorer	https://subnets.avax.network/x-chain	https://subnets-test.avax.network/x-chain
Explore More
Avalanche L1s

Discover how to build sovereign networks with custom rules and token economics.


Data APIs

Access data APIs for the C-Chain, P-Chain, and X-Chain.


Console

Access developer tools, deploy contracts, and manage your blockchain infrastructure.


Is this guide helpful?

Yes
No
Copy Markdown
Snowman Consensus

Learn about the Snowman Consensus protocol.



Available endpoints:

Endpoints that require arguments:
//agoric.rpc.kjnodes.com/abci_info?
//agoric.rpc.kjnodes.com/abci_query?path=_&data=_&height=_&prove=_
//agoric.rpc.kjnodes.com/block?height=_
//agoric.rpc.kjnodes.com/block_by_hash?hash=_
//agoric.rpc.kjnodes.com/block_results?height=_
//agoric.rpc.kjnodes.com/block_search?query=_&page=_&per_page=_&order_by=_
//agoric.rpc.kjnodes.com/blockchain?minHeight=_&maxHeight=_
//agoric.rpc.kjnodes.com/broadcast_evidence?evidence=_
//agoric.rpc.kjnodes.com/broadcast_tx_async?tx=_
//agoric.rpc.kjnodes.com/broadcast_tx_commit?tx=_
//agoric.rpc.kjnodes.com/broadcast_tx_sync?tx=_
//agoric.rpc.kjnodes.com/check_tx?tx=_
//agoric.rpc.kjnodes.com/commit?height=_
//agoric.rpc.kjnodes.com/consensus_params?height=_
//agoric.rpc.kjnodes.com/consensus_state?
//agoric.rpc.kjnodes.com/dump_consensus_state?
//agoric.rpc.kjnodes.com/genesis?
//agoric.rpc.kjnodes.com/genesis_chunked?chunk=_
//agoric.rpc.kjnodes.com/header?height=_
//agoric.rpc.kjnodes.com/header_by_hash?hash=_
//agoric.rpc.kjnodes.com/health?
//agoric.rpc.kjnodes.com/net_info?
//agoric.rpc.kjnodes.com/num_unconfirmed_txs?
//agoric.rpc.kjnodes.com/status?
//agoric.rpc.kjnodes.com/subscribe?query=_
//agoric.rpc.kjnodes.com/tx?hash=_&prove=_
//agoric.rpc.kjnodes.com/tx_search?query=_&prove=_&page=_&per_page=_&order_by=_
//agoric.rpc.kjnodes.com/unconfirmed_txs?limit=_
//agoric.rpc.kjnodes.com/unsubscribe?query=_
//agoric.rpc.kjnodes.com/unsubscribe_all?
//agoric.rpc.kjnodes.com/validators?height=_&page=_&per_page=_




