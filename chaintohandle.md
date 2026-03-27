  1 +# Prod RPC Inventory
     2 +
     3 +Derived from the current worktree RPC catalog and direct-local send coverage:
     4 +- `/home/a/exchange-shared/src/services/rpc/config_builder.rs`
     5 +- `/home/a/exchange-shared/src/services/wallet/manager.rs`
     6 +
     7 +## Counts
     8 +
     9 +- Total configured chains: `223`
    10 +- Chains with at least one vendor slug (`Alchemy` or `Infura` or `Ankr`): `82`
    11 +- Current direct-local send set: `106`
    12 +- Current direct-local send set split:
    13 +  - Vendor-backed: `43`
    14 +  - Public-only: `63`
    15 +- If target is `~120` local-send chains, gap from current `106` is: `14`
    16 +
    17 +## Provider Lists
    18 +
    19 +### Alchemy (`43`)
    20 +
    21 +ApeChain, Aptos, Arbitrum One, Astar, Avalanche C-Chain, BNB Smart Chain, Base, Blast, Cardano, Celo, Chiliz, Cronos, Ethereum, Filecoin, Flare, Flow, Kaia, Kaia Lega
        cy, Linea, Manta Pacific, Mantle, Metis, Mode, Monad, Moonbeam, NEAR, Optimism, Polkadot, Polygon, Scroll, Sei, Solana, Sonic, Stacks, Starknet, Stellar, Taiko, Tezos
        , XRP, ZetaChain, Zora, opBNB, zkSync Era
    22 +
    23 +### Infura (`10`)
    24 +
    25 +Arbitrum One, Aurora, Avalanche C-Chain, Base, Ethereum, Fraxtal, Linea, Optimism, Polygon, Starknet
    26 +
    27 +### Ankr (`74`)
    28 +
    29 +Algorand, Aptos, Arbitrum One, Astar, Avalanche C-Chain, BNB Smart Chain, Base, BeraChain, Bitcoin, Blast, Cardano, Celo, Chiliz, Core DAO, Cosmos Hub, Dash, DigiByte
        , Dogecoin, EOS, Ethereum, Fantom, Filecoin, Flare, Flow, Gnosis, Harmony, Hedera, ICON, IOTA EVM, IoTeX, Kaia, Kaia Legacy, Kusama, Linea, Litecoin, Manta Pacific, M
        antle, Metis, Midnight, Mina, Monad, Moonbeam, Moonriver, MultiversX, NEAR, Neo N3, Optimism, Osmosis, Peaq, Polkadot, Polygon, Ronin, Scroll, Sei, Solana, Sonic, Sta
        cks, Starknet, Stellar, Sui, Syscoin NEVM, TON, TRON, Taiko, Telos, Tezos, Theta, VeChain, Waves, XRP, Zcash, Zilliqa, xDai, zkSync Era
    30 +
    31 +## Vendor-Backed And Already Local-Sendable (`43`)
    32 +
    33 +Algorand, Arbitrum One, Aurora, Avalanche C-Chain, Base, Bitcoin, Blast, BNB Smart Chain, Celo, Chiliz, Core DAO, Cosmos Hub, Cronos, Ethereum, Fantom, Filecoin, Flar
        e, Gnosis, Harmony, IOTA EVM, IoTeX, Linea, Mantle, Metis, Mode, Moonbeam, Moonriver, Optimism, Osmosis, Polygon, Ronin, Scroll, Sei, Solana, Syscoin NEVM, Taiko, Tel
        os, TRON, xDai, ZetaChain, zkSync Era, Zora, opBNB
    34 +
    35 +## Public-Only But Already Local-Sendable (`63`)
    36 +
    37 +Agoric, Akash, Axelar, Bahamut, Band, Beam, BounceBit, Canto, Celestia, Cheqd, Coreum, dYdX, Dymension, Electroneum, Energy Web, Ethereum Classic, EthereumPoW, Evmos,
         Fetch, Findora, Fuse, GMMT, GraphLinq, Haqq, Humanode, Initia, Japan Open Chain, Juno, KCC, Kava EVM, KYVE, KaiChain, LUKSO, Lisk, MAP Protocol, Meter, Morph, Neutro
        n, Neon, OKX Chain, Oraichain, Persistence, PulseChain, REI Network, Redbelly, Rootstock, Secret, Shentu, Songbird, Sophon, Stargaze, Step Network, Stratis EVM, Supra
        , Terra, ThunderCore, TomoChain, U2U, Vanar, Velas, Viction, Wanchain, X Layer
    38 +
    39 +## Vendor-Backed But Not Yet Local-Send Ready (`39`)
    40 +
    41 +ApeChain, Aptos, Astar, BeraChain, Cardano, Dash, DigiByte, Dogecoin, EOS, Flow, Fraxtal, Hedera, ICON, Kaia, Kaia Legacy, Kusama, Litecoin, Manta Pacific, Midnight,
        Mina, Monad, MultiversX, NEAR, Neo N3, Peaq, Polkadot, Sonic, Stacks, Starknet, Stellar, Sui, TON, Tezos, Theta, VeChain, Waves, XRP, Zcash, Zilliqa
    42 +
    43 +## Best `14` Candidate Additions If The Goal Is `~120` Local-Send Chains
    44 +
    45 +These are the most obvious vendor-backed additions if the only goal is to move from `106` to `~120`:
    46 +
    47 +XRP, Stellar, NEAR, Cardano, Polkadot, TON, Aptos, Sui, Litecoin, Dogecoin, Dash, Zcash, Starknet, Flow
    48 +
    49 +This is a count target only, not a production recommendation.
    50 +
    51 +## Practical Recommendation
    52 +
    53 +For production, do not force the number to `120` just to hit a target.
    54 +
    55 +Safer rollout:
    56 +1. Keep the core trusted local-send set active first.
    57 +2. Let Trocador handle the rest.
    58 +3. Add from the `14`-candidate list only after chain-specific sender validation and canary testing.



 - Ethereum
  - Polygon
  - Arbitrum One
  - Optimism
  - Base
  - BNB Smart Chain
  - Avalanche C-Chain
  - Solana
  - Tron
  - Bitcoin only if backed by a real trusted node/provider