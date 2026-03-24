/// High-level dispatcher for blockchain address derivation
///
/// This module routes address derivation requests to the appropriate
/// blockchain-specific implementation in the blockchains/ folder.
///
/// All actual derivation logic lives in src/services/wallet/blockchains/
use crate::services::wallet::blockchains::{
    Algorand, AptosDerivation, AvalancheXDerivation, BeamDerivation, BinanceChainDerivation,
    Bitcoin, BitcoinCash, BitcoinLightningDerivation, BitcoinSvDerivation, BitcoinzDerivation,
    BlockchainDerivation, Brc20Derivation, CardanoDerivation, CosmosHubDerivation, DashDerivation,
    DefichainDerivation, DockDerivation, Dogecoin, EosDerivation, EverscaleDerivation, EvmChain,
    FactomDerivation, FlowDerivation, FluxDerivation, HederaDerivation, IconDerivation,
    KusamaDerivation, Litecoin, MinaDerivation, MoneroDerivation, MultiversxDerivation, Near,
    NeoDerivation, NimiqDerivation, OmniDerivation, OntologyDerivation, OsmosisDerivation,
    PartisiaDerivation, PocketDerivation, PolkadotDerivation, RavencoinDerivation, Solana,
    StacksDerivation, StarknetDerivation, StellarDerivation, SuiDerivation, TerraDerivation,
    TezosDerivation, ThetaDerivation, TonDerivation, TronDerivation, VechainDerivation,
    WavesDerivation, XrpDerivation, ZanoDerivation, ZcashDerivation, ZilliqaDerivation,
};

// Re-export key derivation functions
pub use crate::services::wallet::blockchains::special::{derive_algorand_key, derive_near_key};

// Re-export trait helper
pub use crate::services::wallet::blockchains::traits::is_valid_seed_phrase;

struct GenericCosmosDerivation {
    prefix: &'static str,
    name: &'static str,
}

impl BlockchainDerivation for GenericCosmosDerivation {
    fn coin_type(&self) -> u32 {
        118
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use ripemd::Ripemd160;
        use secp256k1::{PublicKey, Secp256k1, SecretKey};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.prefix.as_bytes());
        let derived = hasher.finalize();

        let secret_key =
            SecretKey::from_slice(&derived).map_err(|e| format!("Invalid secret key: {}", e))?;

        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &secret_key);
        let pub_bytes = public_key.serialize();

        let mut hasher = Sha256::new();
        hasher.update(&pub_bytes);
        let sha_hash = hasher.finalize();

        let mut hasher = Ripemd160::new();
        hasher.update(&sha_hash);
        let account_id = hasher.finalize();

        Ok(format!("{}1{}", self.prefix, hex::encode(account_id)))
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.prefix.as_bytes());
        let derived = hasher.finalize();

        Ok(hex::encode(derived))
    }
}

struct GenericSubstrateDerivation {
    coin_type: u32,
    name: &'static str,
    network_id: u8,
    salt: &'static [u8],
}

impl BlockchainDerivation for GenericSubstrateDerivation {
    fn coin_type(&self) -> u32 {
        self.coin_type
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use blake2::Blake2b512;
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.salt);
        let derived = hasher.finalize();

        let mut payload = vec![self.network_id];
        payload.extend_from_slice(&derived);

        let mut hasher = Blake2b512::new();
        hasher.update(b"SS58PRE");
        hasher.update(&payload);
        let checksum_hash = hasher.finalize();
        payload.extend_from_slice(&checksum_hash[0..2]);

        Ok(bs58::encode(payload).into_string())
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        use bip39::{Language, Mnemonic};
        use sha2::{Digest, Sha256};

        let mnemonic = Mnemonic::parse_in_normalized(Language::English, seed)
            .map_err(|e| format!("Invalid mnemonic: {}", e))?;
        let seed = mnemonic.to_seed("");

        let mut hasher = Sha256::new();
        hasher.update(&seed);
        hasher.update(&index.to_le_bytes());
        hasher.update(self.salt);
        let derived = hasher.finalize();

        Ok(hex::encode(derived))
    }
}

struct MonacoinDerivation;

impl BlockchainDerivation for MonacoinDerivation {
    fn coin_type(&self) -> u32 {
        22
    }

    fn name(&self) -> &'static str {
        "Monacoin"
    }

    fn derive_address(&self, seed: &str, index: u32) -> Result<String, String> {
        let litecoin_address = Litecoin.derive_address(seed, index)?;

        if let Some(rest) = litecoin_address.strip_prefix('L') {
            Ok(format!("M{}", rest))
        } else {
            Ok(format!("M{}", litecoin_address))
        }
    }

    fn derive_private_key(&self, seed: &str, index: u32) -> Result<String, String> {
        Litecoin.derive_private_key(seed, index)
    }
}

/// Main entry point for address derivation
/// Routes to the appropriate blockchain implementation based on network
pub async fn derive_address(
    seed_phrase: &str,
    ticker: &str,
    network: &str,
    index: u32,
) -> Result<String, String> {
    let chain = select_derivation(ticker, network)?;
    chain.derive_address(seed_phrase, index)
}

/// Resolve the persisted coin type for a ticker/network pair.
/// This must stay aligned with `derive_address` so payout routing
/// follows the same blockchain family as address generation.
pub fn resolve_coin_type(ticker: &str, network: &str) -> Result<u32, String> {
    let chain = select_derivation(ticker, network)?;
    Ok(chain.coin_type())
}

fn select_derivation(ticker: &str, network: &str) -> Result<Box<dyn BlockchainDerivation>, String> {
    let network_lower = network.to_lowercase();
    let ticker_lower = ticker.to_lowercase();

    // First, check ticker for unambiguous matches
    match ticker_lower.as_str() {
        "xmr" => return Ok(Box::new(MoneroDerivation)),
        "btc" if network_lower == "bitcoin" || network_lower == "mainnet" => {
            return Ok(Box::new(Bitcoin));
        }
        "dash" if network_lower == "mainnet" => return Ok(Box::new(DashDerivation)),
        "zec" if network_lower == "mainnet" => return Ok(Box::new(ZcashDerivation)),
        "rvn" if network_lower == "mainnet" => return Ok(Box::new(RavencoinDerivation)),
        "mona" if network_lower == "mainnet" => return Ok(Box::new(MonacoinDerivation)),
        "vtc" | "dgb" | "grs" | "nmc" | "via" | "pivx" if network_lower == "mainnet" => {
            return Ok(Box::new(Bitcoin));
        }
        "sys" if network_lower == "mainnet" => return Ok(Box::new(Bitcoin)),
        "sol" => return Ok(Box::new(Solana)),
        "algo" => return Ok(Box::new(Algorand)),
        "near" => return Ok(Box::new(Near)),
        "ada" => return Ok(Box::new(CardanoDerivation)),
        "dot" => return Ok(Box::new(PolkadotDerivation)),
        "ksm" => return Ok(Box::new(KusamaDerivation)),
        "juno" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "juno",
                name: "Juno",
            }));
        }
        "akt" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "akash",
                name: "Akash",
            }));
        }
        "inj" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "inj",
                name: "Injective",
            }));
        }
        "regen" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "regen",
                name: "Regen",
            }));
        }
        "stars" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "stars",
                name: "Stargaze",
            }));
        }
        "scrt" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "secret",
                name: "Secret",
            }));
        }
        "band" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "band",
                name: "Band",
            }));
        }
        "ion" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "ion",
                name: "Ion",
            }));
        }
        "gravitybg" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "gravity",
                name: "Gravity Bridge",
            }));
        }
        "cro" if network_lower == "cronos" => {
            return Ok(Box::new(GenericCosmosDerivation {
                prefix: "cro",
                name: "Cronos",
            }));
        }
        "aca" => {
            return Ok(Box::new(GenericSubstrateDerivation {
                coin_type: 354,
                name: "Acala",
                network_id: 10,
                salt: b"acala",
            }));
        }
        "astr" => {
            return Ok(Box::new(GenericSubstrateDerivation {
                coin_type: 354,
                name: "Astar",
                network_id: 11,
                salt: b"astar",
            }));
        }
        "sdn" => {
            return Ok(Box::new(GenericSubstrateDerivation {
                coin_type: 354,
                name: "Shiden",
                network_id: 12,
                salt: b"shiden",
            }));
        }
        "para" => {
            return Ok(Box::new(GenericSubstrateDerivation {
                coin_type: 354,
                name: "Parallel",
                network_id: 13,
                salt: b"parallel",
            }));
        }
        "xrp" => return Ok(Box::new(XrpDerivation)),
        "trx" => return Ok(Box::new(TronDerivation)),
        "atom" => return Ok(Box::new(CosmosHubDerivation)),
        "sui" => return Ok(Box::new(SuiDerivation)),
        _ => {}
    }

    // Then check network for specific matches
    match network_lower.as_str() {
        // ===== BITCOIN FAMILY =====
        "bitcoin" | "btc" => Ok(Box::new(Bitcoin)),
        "litecoin" | "ltc" => Ok(Box::new(Litecoin)),
        "dogecoin" | "doge" => Ok(Box::new(Dogecoin)),
        "bitcoin_cash" | "bch" => Ok(Box::new(BitcoinCash)),
        "dash" => Ok(Box::new(DashDerivation)),
        "ravencoin" | "rvn" => Ok(Box::new(RavencoinDerivation)),
        "zcash" | "zec" => Ok(Box::new(ZcashDerivation)),
        "brc20" | "bitcoin_brc20" => Ok(Box::new(Brc20Derivation)),
        "lightning" | "bitcoin_lightning" => Ok(Box::new(BitcoinLightningDerivation)),
        "bitcoin_sv" | "bsv" | "bchsv" => Ok(Box::new(BitcoinSvDerivation)),
        "bitcoinz" | "btcz" => Ok(Box::new(BitcoinzDerivation)),

        // ===== SOLANA =====
        "solana" | "sol" => Ok(Box::new(Solana)),

        // ===== COSMOS SDK =====
        "cosmos" | "cosmos_hub" => Ok(Box::new(CosmosHubDerivation)),
        "osmosis" => Ok(Box::new(OsmosisDerivation)),
        "juno" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "juno",
            name: "Juno",
        })),
        "akash" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "akash",
            name: "Akash",
        })),
        "injective" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "inj",
            name: "Injective",
        })),
        "regen" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "regen",
            name: "Regen",
        })),
        "stargaze" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "stars",
            name: "Stargaze",
        })),
        "secret" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "secret",
            name: "Secret",
        })),
        "band" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "band",
            name: "Band",
        })),
        "ion" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "ion",
            name: "Ion",
        })),
        "gravity" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "gravity",
            name: "Gravity Bridge",
        })),
        "cronos" => Ok(Box::new(GenericCosmosDerivation {
            prefix: "cro",
            name: "Cronos",
        })),

        // ===== SUBSTRATE =====
        "polkadot" | "dot" => Ok(Box::new(PolkadotDerivation)),
        "kusama" | "ksm" => Ok(Box::new(KusamaDerivation)),
        "acala" => Ok(Box::new(GenericSubstrateDerivation {
            coin_type: 354,
            name: "Acala",
            network_id: 10,
            salt: b"acala",
        })),
        "astar" => Ok(Box::new(GenericSubstrateDerivation {
            coin_type: 354,
            name: "Astar",
            network_id: 11,
            salt: b"astar",
        })),
        "shiden" => Ok(Box::new(GenericSubstrateDerivation {
            coin_type: 354,
            name: "Shiden",
            network_id: 12,
            salt: b"shiden",
        })),
        "parallel" => Ok(Box::new(GenericSubstrateDerivation {
            coin_type: 354,
            name: "Parallel",
            network_id: 13,
            salt: b"parallel",
        })),

        // ===== SPECIAL CHAINS =====
        "cardano" | "ada" => Ok(Box::new(CardanoDerivation)),
        "monero" | "xmr" => Ok(Box::new(MoneroDerivation)),
        "neo" => Ok(Box::new(NeoDerivation)),
        "neo_n2" | "n2" | "n3" | "neo3" => Ok(Box::new(NeoDerivation)), // Neo N2 is old Neo, N3 is new Neo
        "icon" | "icx" => Ok(Box::new(IconDerivation)),
        "algorand" | "algo" => Ok(Box::new(Algorand)),
        "near" => Ok(Box::new(Near)),
        "tezos" | "xtz" => Ok(Box::new(TezosDerivation)),
        "ripple" | "xrp" => Ok(Box::new(XrpDerivation)),
        "stacks" | "stx" => Ok(Box::new(StacksDerivation)),
        "stellar" | "xlm" => Ok(Box::new(StellarDerivation)),
        "tron" | "trx" | "trc20" => Ok(Box::new(TronDerivation)),
        "waves" => Ok(Box::new(WavesDerivation)),
        "ton" => Ok(Box::new(TonDerivation)),
        "vechain" | "vet" => Ok(Box::new(VechainDerivation)),
        "sui" => Ok(Box::new(SuiDerivation)),
        "eos" => Ok(Box::new(EosDerivation)),
        "hedera" | "hbar" => Ok(Box::new(HederaDerivation)),
        "mina" => Ok(Box::new(MinaDerivation)),
        "aptos" | "apt" => Ok(Box::new(AptosDerivation)),
        "flow" => Ok(Box::new(FlowDerivation)),
        "starknet" | "stark" => Ok(Box::new(StarknetDerivation)),
        "theta" => Ok(Box::new(ThetaDerivation)),
        "zilliqa" | "zil" => Ok(Box::new(ZilliqaDerivation)),
        "multiversx" | "egld" => Ok(Box::new(MultiversxDerivation)),
        "nimiq" | "nim" => Ok(Box::new(NimiqDerivation)),
        "flux" | "zel" => Ok(Box::new(FluxDerivation)),
        "ontology" | "ont" => Ok(Box::new(OntologyDerivation)),
        "pocket" | "pokt" => Ok(Box::new(PocketDerivation)),
        "omni" => Ok(Box::new(OmniDerivation)),
        "zano" => Ok(Box::new(ZanoDerivation)),
        "binance_chain" | "bep2" => Ok(Box::new(BinanceChainDerivation)),
        "partisia" | "mpc" => Ok(Box::new(PartisiaDerivation)),
        "dock" => Ok(Box::new(DockDerivation)),
        "defichain" | "dfi" => Ok(Box::new(DefichainDerivation)),
        "beam" => Ok(Box::new(BeamDerivation)),
        "everscale" | "freeton" | "ever" => Ok(Box::new(EverscaleDerivation)),
        "terra" | "terra_classic" | "luna" | "lunc" => Ok(Box::new(TerraDerivation)),
        "factom" | "fct" => Ok(Box::new(FactomDerivation)),
        "avalanche_x" | "avaxx" => Ok(Box::new(AvalancheXDerivation)),
        "a2z" => Ok(Box::new(EvmChain::ethereum())), // A2Z is Ethereum fork
        "shielded" => Ok(Box::new(ZcashDerivation)), // Zcash shielded variant
        "strk" => Ok(Box::new(StarknetDerivation)),  // Starknet duplicate ticker

        // ===== EVM FAMILY (80+ chains) =====
        // Trocador aliases: ETH, MAINNET, MATIC, AVAXC, FTM, KAI, KAIA, KIP7, KLAY,
        // MANTA, METALL2, SEIEVM, SMARTCHAIN, SYSNEVM, TLOSEVM, HAQQ, HYPEREVM,
        // ISLMEVM, FILEVM, FITFI, FLR, CHZ, CFXCORE, BTT, BERA, OAS, PULSE, RSK,
        // STARK, STRAX, KATANA, LAVA, KLC
        "ethereum" | "eth" | "mainnet" | "polygon" | "matic" | "bsc" | "smartchain"
        | "arbitrum" | "optimism" | "erc20" | "bep20" | "base" | "avalanche" | "avaxc"
        | "fantom" | "ftm" | "celo" | "harmony" | "klaytn" | "klay" | "kai" | "kaia" | "kip7"
        | "metis" | "metall2" | "boba" | "gnosis" | "fuse" | "iotex" | "scroll" | "zksync"
        | "linea" | "mantle" | "manta_pacific" | "manta" | "mode" | "blast" | "taiko" | "zora"
        | "sonic" | "moonbeam" | "moonriver" | "aurora" | "evmos" | "kava" | "oasis"
        | "oasis sapphire" | "rootstock" | "rsk" | "syscoin" | "sysnevm" | "telos" | "tlosevm"
        | "thundercore" | "tomochain" | "velas" | "wanchain" | "whitechain" | "x_layer"
        | "zkfair" | "shibarium" | "opbnb" | "fraxtal" | "merlin" | "morph" | "redbelly"
        | "rei" | "step_network" | "fitfi" | "stratis" | "strax" | "cyber" | "endurance"
        | "hyper_evm" | "hyperevm" | "iota_evm" | "islm_evm" | "islmevm" | "haqq" | "okx_chain"
        | "oasys" | "oas" | "peaq" | "pulsechain" | "pulse" | "ronin" | "zeta" | "bitgert"
        | "botanix" | "bttc" | "btt" | "cfx" | "cfxcore" | "chiliz" | "chz" | "conflux_espace"
        | "core" | "filecoin" | "filevm" | "flare" | "flr" | "kcc" | "klc" | "bahamut" | "b2"
        | "berachain" | "bera" | "apechain" | "katana" | "lava" | "sei" | "seievm" => {
            Ok(Box::new(EvmChain::ethereum()))
        }

        // ===== LEGACY IMPLEMENTATIONS (in old module files) =====
        _ => Err(format!(
            "Blockchain '{}' not yet migrated to modular structure",
            network
        )),
    }
}

// Stub functions for backward compatibility - these call the modular implementations
pub async fn derive_evm_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    EvmChain::ethereum().derive_address(seed_phrase, index)
}

pub async fn derive_btc_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Bitcoin.derive_address(seed_phrase, index)
}

pub async fn derive_solana_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Solana.derive_address(seed_phrase, index)
}

pub async fn derive_algorand_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Algorand.derive_address(seed_phrase, index)
}

pub async fn derive_near_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    Near.derive_address(seed_phrase, index)
}

pub async fn derive_cardano_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    CardanoDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_polkadot_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    PolkadotDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_ripple_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    XrpDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_tron_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    TronDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_cosmos_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    CosmosHubDerivation.derive_address(seed_phrase, index)
}

pub async fn derive_sui_address(seed_phrase: &str, index: u32) -> Result<String, String> {
    SuiDerivation.derive_address(seed_phrase, index)
}

pub async fn sign_message_with_seed(
    seed_phrase: &str,
    index: u32,
    message: &str,
) -> Result<String, String> {
    use secp256k1::{Message, Secp256k1, SecretKey};
    use sha2::{Digest, Sha256};

    let private_key = derive_evm_key(seed_phrase, index).await?;
    let key_bytes = hex::decode(private_key.trim_start_matches("0x"))
        .map_err(|e| format!("Invalid derived key hex: {}", e))?;
    let secret_key =
        SecretKey::from_slice(&key_bytes).map_err(|e| format!("Invalid secret key: {}", e))?;

    let digest = Sha256::digest(message.as_bytes());
    let message = Message::from_digest_slice(&digest)
        .map_err(|e| format!("Invalid message digest: {}", e))?;

    let secp = Secp256k1::new();
    let signature = secp.sign_ecdsa(&message, &secret_key);

    Ok(hex::encode(signature.serialize_compact()))
}

// Key derivation functions (for signing) - these route to blockchain implementations
pub async fn derive_evm_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    EvmChain::ethereum().derive_private_key(seed_phrase, index)
}

pub async fn derive_btc_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    Bitcoin.derive_private_key(seed_phrase, index)
}

pub async fn derive_solana_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    Solana.derive_private_key(seed_phrase, index)
}

pub async fn derive_cosmos_key(seed_phrase: &str, index: u32) -> Result<String, String> {
    CosmosHubDerivation.derive_private_key(seed_phrase, index)
}

pub async fn derive_substrate_seed(seed_phrase: &str, index: u32) -> Result<Vec<u8>, String> {
    // Substrate uses seed bytes instead of hex key
    let key_hex = PolkadotDerivation.derive_private_key(seed_phrase, index)?;
    hex::decode(key_hex.trim_start_matches("0x"))
        .map_err(|e| format!("Failed to decode substrate seed: {}", e))
}
