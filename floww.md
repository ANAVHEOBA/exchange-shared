
  1. Critical: payout routing loses exact network detail and collapses too much by family.

  - Payout dispatch is driven mostly by coin_type, not the exact resolved (ticker, network) route in src/services/wallet/manager.rs:113.
  - All EVM-like payouts collapse into one handler on coin_type 60 in src/services/wallet/manager.rs:199, and that handler hardcodes chain_id: 1 in src/services/wallet/
    manager.rs:1064.
  - Wallet metadata is also incomplete: blockchain_id is always saved as 1 in src/modules/wallet/crud.rs:61.
  - That means address generation can be correct, while actual payout broadcasting can still target the wrong chain semantics.

  2. Critical: duplicate payout risk is not fully closed.

  - There are two separate settlement triggers: the blockchain listener in src/services/blockchain/listener.rs:107 and the monitor fallback in src/services/monitor/
    engine.rs:220.
  - The payout “idempotency” check is only “does payout_tx_hash already exist?” in src/services/wallet/manager.rs:92.
  - There is no DB lock or “payout in progress” transition before signing/broadcasting in src/services/settlement.rs:40.
  - Two workers can both observe “not paid yet” and both try to send.

  3. Critical: refund calculation double-subtracts fees.

  - Refund logic does deposit_amount - platform_fee - total_fee - gas_cost_estimate in src/services/refund/calculator.rs:51.
  - But total_fee already includes platform_fee when swaps are created in src/modules/swap/service.rs:199.
  - So platform_fee gets subtracted twice.

  4. High: the system validates the internal address, not the user’s final payout destination during swap creation.

  - create_swap validates only internal_payout_address with Trocador in src/modules/swap/service.rs:89.
  - The user’s recipient_address is stored in src/modules/swap/service.rs:195, but not validated there.
  - There is a separate /swap/validate-address path in src/modules/swap/crud.rs:789, but it is not enforced as part of swap creation.

  5. High: memo/tag handling is not wired through payout execution.

  - recipient_extra_id is stored in src/modules/swap/service.rs:196 and src/modules/wallet/crud.rs:53.
  - But it is not used by the payout handlers; a repo-wide search shows no payout-path reads of recipient_extra_id.
  - That is dangerous for networks that require destination tags/memos.
  - XRP’s builder explicitly supports DestinationTag in src/services/wallet/tx_builders/xrp.rs:25, but the payout path in src/services/wallet/manager.rs:492 never passes one.

  6. High: many non-BTC/non-Solana payout builders are still placeholder implementations.

  - Cardano handler says “simplified implementation” in src/services/wallet/manager.rs:436.
  - Cosmos handler says real production should use cosmrs in src/services/wallet/manager.rs:1288.
  - Substrate handler says real production should use subxt in src/services/wallet/manager.rs:1361.
  - XRP blob building says simplified in src/services/wallet/tx_builders/xrp.rs:88.
  - These are not small polish issues; they mean transaction construction is not chain-native.

  7. High: the provider abstraction is overly EVM-shaped for a multi-chain payout engine.

  - The generic provider exposes eth_getTransactionCount, eth_gasPrice, eth_sendRawTransaction, eth_getBalance in src/services/wallet/rpc.rs:19.
  - The RpcManagerAdapter forwards those exact ETH RPC methods for any configured chain in src/services/rpc/blockchain_adapter.rs:26.
  - That is structurally wrong for many non-EVM chains even if the address generation is fine.

  8. Medium: settlement detection and accounting are balance-snapshot based, not transaction-event based.

  - The listener decides “funds arrived” by checking whether current balance is at least expected_amount * 0.95 in src/services/blockchain/listener.rs:97.
  - actual_received is then recorded from the observed balance snapshot in src/modules/wallet/crud.rs:103, not from a confirmed incoming tx delta.
  - That is fragile around dust, prior residue, partial sends, or delayed top-ups.

  9. Medium: several payout handlers reuse generic Cosmos/EVM signing helpers for unrelated chains.

  - derive_cosmos_key is literally CosmosHubDerivation.derive_private_key(...) in src/services/wallet/derivation.rs:548.
  - Yet that helper is reused in Cardano, XRP, Tron, Tezos, Stellar, Cosmos, and others in src/services/wallet/manager.rs:453 and src/services/wallet/manager.rs:533.
  - That is a strong signal the payout layer was built as a broad placeholder, not as chain-specific execution.

  Short version: the address-generation side is much stronger now; the payout/settlement side is where the codebase is still not right for serious production use.

  If you want, I can turn this into a concrete hardening plan in priority order:

  1. fix refund double-charge bug
  2. enforce recipient validation plus memo/tag validation at create_swap
  3. add DB-level payout lock / in-progress state
  4. persist exact payout network metadata
  5. replace placeholder payout builders family by family



   1. exact network-aware payout routing, not just coin_type
  2. token-aware payout execution for ERC20/BEP20/TRC20/...
  3. chain-specific provider selection instead of generic eth_* RPC for everything
  4. DB lock / payout-in-progress state
  5. recipient address plus memo/tag enforcement
  6. refund fee bug fix
  7. replacement of placeholder tx builders
