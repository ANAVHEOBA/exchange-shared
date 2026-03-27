use crate::services::wallet::catalog::{mainnet_family, MainnetFamily};

pub fn canonical_chain_key(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut last_was_separator = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_separator = false;
        } else if !last_was_separator {
            normalized.push('_');
            last_was_separator = true;
        }
    }

    normalized.trim_matches('_').to_string()
}

pub fn chain_key_candidates(ticker: &str, network: &str) -> Vec<String> {
    let ticker_key = canonical_chain_key(ticker);
    let network_key = canonical_chain_key(network);
    let mut candidates = Vec::new();

    if network_key.is_empty() {
        return candidates;
    }

    if network_key == "mainnet" {
        push_candidate(&mut candidates, resolve_mainnet_chain_key(&ticker_key));
        return candidates;
    }

    if let Some(alias) = network_alias_chain_key(&network_key) {
        push_candidate(&mut candidates, alias);
    }

    push_candidate(&mut candidates, network_key);
    candidates
}

pub fn resolve_configured_chain_key<F>(
    ticker: &str,
    network: &str,
    mut is_configured: F,
) -> Result<String, String>
where
    F: FnMut(&str) -> bool,
{
    let candidates = chain_key_candidates(ticker, network);
    for candidate in &candidates {
        if is_configured(candidate) {
            return Ok(candidate.clone());
        }
    }

    Err(format!(
        "No configured RPC chain matches {}/{} (candidates: {}).",
        ticker,
        network,
        if candidates.is_empty() {
            "<none>".to_string()
        } else {
            candidates.join(", ")
        }
    ))
}

fn push_candidate(candidates: &mut Vec<String>, value: impl Into<String>) {
    let value = value.into();
    if !value.is_empty() && !candidates.iter().any(|candidate| candidate == &value) {
        candidates.push(value);
    }
}

fn network_alias_chain_key(network_key: &str) -> Option<&'static str> {
    match network_key {
        "aleo" => Some("aleo"),
        "arb" => Some("arbitrum_one"),
        "ae" => Some("aeternity"),
        "alph" => Some("alephium"),
        "area" => Some("arena_z"),
        "dag" => Some("constellation_dag"),
        "eth" | "erc20" => Some("ethereum"),
        "btc" | "lightning" | "omni" | "brc20" => Some("bitcoin"),
        "bsc" | "smartchain" | "bep20" => Some("bnb_smart_chain"),
        "bb" => Some("bouncebit"),
        "bep2" => Some("binance_chain"),
        "bera" => Some("berachain"),
        "etn" => Some("electroneum"),
        "ethw" => Some("ethereumpow"),
        "trx" | "trc20" => Some("tron"),
        "sol" | "spl" => Some("solana"),
        "hmnd" => Some("humanode"),
        "htr" => Some("hathor"),
        "matic" => Some("polygon"),
        "arbitrum" => Some("arbitrum_one"),
        "avaxc" => Some("avalanche_c_chain"),
        "avaxx" => Some("avalanche_x"),
        "ada" => Some("cardano"),
        "algo" => Some("algorand"),
        "bchsv" => Some("bitcoin_sv"),
        "btt" | "bttc" => Some("bittorrent"),
        "cfxcore" => Some("conflux_core"),
        "chz" => Some("chiliz"),
        "core" => Some("core_dao"),
        "dfi" => Some("defichain"),
        "dot" => Some("polkadot"),
        "egld" => Some("multiversx"),
        "fct" => Some("factom"),
        "fil" => Some("filecoin"),
        "filevm" => Some("filecoin"),
        "fio" => Some("fio"),
        "fitfi" => Some("step_network"),
        "flr" => Some("flare"),
        "ftm" => Some("fantom"),
        "freeton" => Some("everscale"),
        "glq" => Some("graphlinq"),
        "gmmt" => Some("gmmt"),
        "hbar" => Some("hedera"),
        "hyperevm" => Some("hyper_evm"),
        "islmevm" => Some("haqq"),
        "joc" => Some("japan_open_chain"),
        "kai" => Some("kaichain"),
        "kava" => Some("kava_evm"),
        "kip7" | "klay" => Some("kaia_legacy"),
        "kaia" => Some("kaia"),
        "kda" => Some("kadena"),
        "klc" => Some("kalychain"),
        "manta" => Some("manta_pacific"),
        "mapo" => Some("map_protocol"),
        "meter" => Some("meter"),
        "metall2" => Some("metal_l2"),
        "mnt" => Some("mantle"),
        "nrg" => Some("energi"),
        "n2" | "n3" | "neo" | "neo3" => Some("neo_n3"),
        "nim" => Some("nimiq"),
        "oas" => Some("oasys"),
        "op" => Some("optimism"),
        "ont" => Some("ontology"),
        "pokt" => Some("pocket"),
        "pulse" => Some("pulsechain"),
        "reef" => Some("reef"),
        "rei" => Some("rei_network"),
        "rsk" => Some("rootstock"),
        "scr" => Some("scroll"),
        "seievm" => Some("sei"),
        "sgb" => Some("songbird"),
        "shielded" => Some("zcash"),
        "soph" => Some("sophon"),
        "supra" => Some("supra"),
        "stark" | "strk" => Some("starknet"),
        "strax" => Some("stratis_evm"),
        "steem" => Some("steem"),
        "stx" => Some("stacks"),
        "sys" => Some("syscoin_nevm"),
        "sysnevm" => Some("syscoin_nevm"),
        "tlos" => Some("telos"),
        "tlosevm" => Some("telos"),
        "vanry" => Some("vanar"),
        "vet" => Some("vechain"),
        "venom" => Some("venom"),
        "vic" => Some("viction"),
        "wan" => Some("wanchain"),
        "xlm" => Some("stellar"),
        "xno" => Some("nano"),
        "xrp" => Some("xrp"),
        "xrd" => Some("radix"),
        "xtz" => Some("tezos"),
        "zel" => Some("flux"),
        "zeta" => Some("zetachain"),
        "zklink" => Some("zklink"),
        "zksync" => Some("zksync_era"),
        _ => None,
    }
}

fn resolve_mainnet_chain_key(ticker_key: &str) -> String {
    match mainnet_family(ticker_key) {
        MainnetFamily::Monero => "monero".to_string(),
        MainnetFamily::Bitcoin => "bitcoin".to_string(),
        MainnetFamily::Litecoin => "litecoin".to_string(),
        MainnetFamily::Dogecoin => "dogecoin".to_string(),
        MainnetFamily::BitcoinCash => "bitcoin_cash".to_string(),
        MainnetFamily::BitcoinSv => "bitcoin_sv".to_string(),
        MainnetFamily::Dash => "dash".to_string(),
        MainnetFamily::Zcash => "zcash".to_string(),
        MainnetFamily::Ravencoin => "ravencoin".to_string(),
        MainnetFamily::Bitcoinz => "bitcoinz".to_string(),
        MainnetFamily::Monacoin => "monacoin".to_string(),
        MainnetFamily::BitcoinLike => ticker_key.to_string(),
        MainnetFamily::Solana => "solana".to_string(),
        MainnetFamily::Algorand => "algorand".to_string(),
        MainnetFamily::Near => "near".to_string(),
        MainnetFamily::Cardano => "cardano".to_string(),
        MainnetFamily::Polkadot => "polkadot".to_string(),
        MainnetFamily::Kusama => "kusama".to_string(),
        MainnetFamily::Acala => "acala".to_string(),
        MainnetFamily::Astar => "astar".to_string(),
        MainnetFamily::Shiden => "shiden".to_string(),
        MainnetFamily::Ripple => "xrp".to_string(),
        MainnetFamily::Tron => "tron".to_string(),
        MainnetFamily::Stellar => "stellar".to_string(),
        MainnetFamily::Sui => "sui".to_string(),
        MainnetFamily::Aptos => "aptos".to_string(),
        MainnetFamily::Multiversx => "multiversx".to_string(),
        MainnetFamily::Eos => "eos".to_string(),
        MainnetFamily::Hedera => "hedera".to_string(),
        MainnetFamily::Icon => "icon".to_string(),
        MainnetFamily::Mina => "mina".to_string(),
        MainnetFamily::Neo3 => "neo_n3".to_string(),
        MainnetFamily::Nimiq => "nimiq".to_string(),
        MainnetFamily::Ontology => "ontology".to_string(),
        MainnetFamily::Pocket => "pocket".to_string(),
        MainnetFamily::Dock => "dock".to_string(),
        MainnetFamily::Defichain => "defichain".to_string(),
        MainnetFamily::Flow => "flow".to_string(),
        MainnetFamily::Stacks => "stacks".to_string(),
        MainnetFamily::Starknet => "starknet".to_string(),
        MainnetFamily::Tezos => "tezos".to_string(),
        MainnetFamily::Theta => "theta".to_string(),
        MainnetFamily::Ton => "ton".to_string(),
        MainnetFamily::Terra => "terra".to_string(),
        MainnetFamily::Vechain => "vechain".to_string(),
        MainnetFamily::Waves => "waves".to_string(),
        MainnetFamily::Zilliqa => "zilliqa".to_string(),
        MainnetFamily::Everscale => "everscale".to_string(),
        MainnetFamily::Factom => "factom".to_string(),
        MainnetFamily::Flux => "flux".to_string(),
        MainnetFamily::CosmosHub => "cosmos_hub".to_string(),
        MainnetFamily::Osmosis => "osmosis".to_string(),
        MainnetFamily::Juno => "juno".to_string(),
        MainnetFamily::Akash => "akash".to_string(),
        MainnetFamily::Injective => "injective".to_string(),
        MainnetFamily::Regen => "regen".to_string(),
        MainnetFamily::Stargaze => "stargaze".to_string(),
        MainnetFamily::Secret => "secret".to_string(),
        MainnetFamily::Band => "band".to_string(),
        MainnetFamily::Ion => "ion".to_string(),
        MainnetFamily::GravityBridge => "gravity_bridge".to_string(),
        MainnetFamily::Cronos => "cronos".to_string(),
        MainnetFamily::Kava => "kava_evm".to_string(),
        MainnetFamily::Agoric => "agoric".to_string(),
        MainnetFamily::Axelar => "axelar".to_string(),
        MainnetFamily::Cheqd => "cheqd".to_string(),
        MainnetFamily::Coreum => "coreum".to_string(),
        MainnetFamily::Shentu => "shentu".to_string(),
        MainnetFamily::Dydx => "dydx".to_string(),
        MainnetFamily::Dymension => "dymension".to_string(),
        MainnetFamily::Fetch => "fetch".to_string(),
        MainnetFamily::Initia => "initia".to_string(),
        MainnetFamily::Kyve => "kyve".to_string(),
        MainnetFamily::Neutron => "neutron".to_string(),
        MainnetFamily::Oraichain => "oraichain".to_string(),
        MainnetFamily::Persistence => "persistence".to_string(),
        MainnetFamily::Sei => "sei".to_string(),
        MainnetFamily::Celestia => "celestia".to_string(),
        MainnetFamily::Thorchain => "thorchain".to_string(),
        MainnetFamily::AlephZero => "aleph_zero".to_string(),
        MainnetFamily::Avail => "avail".to_string(),
        MainnetFamily::Bittensor => "bittensor".to_string(),
        MainnetFamily::Centrifuge => "centrifuge".to_string(),
        MainnetFamily::Karura => "karura".to_string(),
        MainnetFamily::Picasso => "picasso".to_string(),
        MainnetFamily::Polkadex => "polkadex".to_string(),
        MainnetFamily::Polymesh => "polymesh".to_string(),
        MainnetFamily::Ternoa => "ternoa".to_string(),
        MainnetFamily::Vara => "vara".to_string(),
        MainnetFamily::Evm => evm_mainnet_chain_key(ticker_key)
            .unwrap_or(ticker_key)
            .to_string(),
    }
}

fn evm_mainnet_chain_key(ticker_key: &str) -> Option<&'static str> {
    match ticker_key {
        "aleo" => Some("aleo"),
        "ae" => Some("aeternity"),
        "arb" => Some("arbitrum_one"),
        "abey" => Some("abeychain"),
        "bb" => Some("bouncebit"),
        "alph" => Some("alephium"),
        "area" => Some("arena_z"),
        "ark" => Some("ark"),
        "bera" => Some("berachain"),
        "brise" => Some("bitgert"),
        "core" => Some("core_dao"),
        "canto" => Some("canto"),
        "coti" => Some("coti"),
        "dag" => Some("constellation_dag"),
        "deso" => Some("deso"),
        "dero" => Some("dero"),
        "dione" => Some("dione"),
        "elf" => Some("aelf"),
        "eth" => Some("ethereum"),
        "etn" => Some("electroneum"),
        "etc" => Some("ethereum_classic"),
        "ethw" => Some("ethereumpow"),
        "ergo" => Some("ergo"),
        "fil" => Some("filecoin"),
        "firo" => Some("firo"),
        "glq" => Some("graphlinq"),
        "gmmt" => Some("gmmt"),
        "bnb" => Some("bnb_smart_chain"),
        "fra" => Some("findora"),
        "matic" | "pol" => Some("polygon"),
        "avax" => Some("avalanche_c_chain"),
        "ftm" => Some("fantom"),
        "ewt" => Some("energy_web"),
        "hmnd" => Some("humanode"),
        "htr" => Some("hathor"),
        "iost" => Some("iost"),
        "joc" => Some("japan_open_chain"),
        "klay" => Some("kaia_legacy"),
        "kda" => Some("kadena"),
        "lsk" => Some("lisk"),
        "lyx" => Some("lukso"),
        "mapo" => Some("map_protocol"),
        "manta" => Some("manta_pacific"),
        "mnt" => Some("mantle"),
        "mtrg" => Some("meter"),
        "nrg" => Some("energi"),
        "oas" => Some("oasys"),
        "op" => Some("optimism"),
        "pivx" => Some("pivx"),
        "reef" => Some("reef"),
        "rei" => Some("rei_network"),
        "s" => Some("sonic"),
        "scr" => Some("scroll"),
        "sgb" => Some("songbird"),
        "celo" => Some("celo"),
        "glmr" => Some("moonbeam"),
        "movr" => Some("moonriver"),
        "cro" => Some("cronos"),
        "boba" => Some("boba_network"),
        "cfx" => Some("conflux_core"),
        "chz" => Some("chiliz"),
        "kai" => Some("kaichain"),
        "one" => Some("harmony"),
        "iotx" => Some("iotex"),
        "ron" => Some("ronin"),
        "flr" => Some("flare"),
        "rbtc" => Some("rootstock"),
        "pls" => Some("pulsechain"),
        "quai" => Some("quai"),
        "rose" => Some("oasis_sapphire"),
        "soph" => Some("sophon"),
        "steem" => Some("steem"),
        "supra" => Some("supra"),
        "sys" => Some("syscoin_nevm"),
        "tlos" => Some("telos"),
        "u2u" => Some("u2u"),
        "vanry" => Some("vanar"),
        "venom" => Some("venom"),
        "vic" => Some("viction"),
        "wan" => Some("wanchain"),
        "xno" => Some("nano"),
        "xrd" => Some("radix"),
        "zk" => Some("zksync_era"),
        "zetrix" => Some("zetrix"),
        "zeta" => Some("zetachain"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{canonical_chain_key, chain_key_candidates, resolve_configured_chain_key};
    use crate::services::rpc::build_default_rpc_configs;
    use serde::Deserialize;
    use std::collections::{BTreeMap, BTreeSet, HashSet};

    #[derive(Debug, Deserialize)]
    struct SnapshotCoin {
        ticker: String,
        network: String,
    }

    #[derive(Debug, Deserialize)]
    struct ChainMetadata {
        name: String,
    }

    #[test]
    fn canonical_chain_key_normalizes_symbols() {
        assert_eq!(
            canonical_chain_key("Avalanche C-Chain"),
            "avalanche_c_chain"
        );
        assert_eq!(canonical_chain_key("zkSync Era"), "zksync_era");
        assert_eq!(
            canonical_chain_key("Factom Accumulate"),
            "factom_accumulate"
        );
    }

    #[test]
    fn mainnet_resolution_uses_ticker_family() {
        assert_eq!(chain_key_candidates("ADA", "Mainnet"), vec!["cardano"]);
        assert_eq!(chain_key_candidates("XRP", "MAINNET"), vec!["xrp"]);
        assert_eq!(chain_key_candidates("ETH", "Mainnet"), vec!["ethereum"]);
    }

    #[test]
    fn live_aliases_map_to_expected_chain_keys() {
        assert_eq!(
            chain_key_candidates("ETH", "ERC20"),
            vec!["ethereum", "erc20"]
        );
        assert_eq!(chain_key_candidates("SYN", "FTM"), vec!["fantom", "ftm"]);
        assert_eq!(chain_key_candidates("BERA", "Mainnet"), vec!["berachain"]);
        assert_eq!(chain_key_candidates("kai", "MAINNET"), vec!["kaichain"]);
        assert_eq!(
            chain_key_candidates("BNB", "BEP20"),
            vec!["bnb_smart_chain", "bep20"]
        );
        assert_eq!(chain_key_candidates("KAI", "KAI"), vec!["kaichain", "kai"]);
        assert_eq!(
            chain_key_candidates("BTC", "Lightning"),
            vec!["bitcoin", "lightning"]
        );
        assert_eq!(chain_key_candidates("mtrg", "MAINNET"), vec!["meter"]);
        assert_eq!(
            chain_key_candidates("dag", "MAINNET"),
            vec!["constellation_dag"]
        );
        assert_eq!(chain_key_candidates("aleo", "MAINNET"), vec!["aleo"]);
        assert_eq!(chain_key_candidates("steem", "MAINNET"), vec!["steem"]);
        assert_eq!(chain_key_candidates("venom", "MAINNET"), vec!["venom"]);
        assert_eq!(chain_key_candidates("xno", "MAINNET"), vec!["nano"]);
        assert_eq!(chain_key_candidates("sgb", "MAINNET"), vec!["songbird"]);
        assert_eq!(chain_key_candidates("bb", "MAINNET"), vec!["bouncebit"]);
        assert_eq!(chain_key_candidates("caps", "MAINNET"), vec!["ternoa"]);
        assert_eq!(chain_key_candidates("soph", "MAINNET"), vec!["sophon"]);
        assert_eq!(chain_key_candidates("supra", "MAINNET"), vec!["supra"]);
        assert_eq!(chain_key_candidates("quai", "MAINNET"), vec!["quai"]);
        assert_eq!(chain_key_candidates("vic", "MAINNET"), vec!["viction"]);
        assert_eq!(chain_key_candidates("vanry", "MAINNET"), vec!["vanar"]);
        assert_eq!(chain_key_candidates("area", "MAINNET"), vec!["arena_z"]);
        assert_eq!(
            chain_key_candidates("joc", "MAINNET"),
            vec!["japan_open_chain"]
        );
        assert_eq!(chain_key_candidates("nrg", "MAINNET"), vec!["energi"]);
        assert_eq!(chain_key_candidates("glq", "MAINNET"), vec!["graphlinq"]);
        assert_eq!(chain_key_candidates("eth", "KATANA"), vec!["katana"]);
        assert_eq!(
            chain_key_candidates("MBX", "KIP7"),
            vec!["kaia_legacy", "kip7"]
        );
        assert_eq!(
            chain_key_candidates("ZEC", "Shielded"),
            vec!["zcash", "shielded"]
        );
    }

    #[test]
    fn configured_resolution_picks_available_key() {
        let configured = HashSet::from([
            "ethereum".to_string(),
            "cardano".to_string(),
            "tron".to_string(),
        ]);

        let resolved =
            resolve_configured_chain_key("ADA", "Mainnet", |key| configured.contains(key))
                .expect("cardano should resolve");
        assert_eq!(resolved, "cardano");
    }

    #[test]
    fn generated_chain_keys_are_unique() {
        let chains: Vec<ChainMetadata> =
            serde_json::from_str(include_str!("../../config/chains.json"))
                .expect("chains.json should parse");
        let mut seen = HashSet::new();

        for chain in chains {
            let key = canonical_chain_key(&chain.name);
            assert!(
                seen.insert(key.clone()),
                "duplicate generated chain key found in chains.json: {}",
                key
            );
        }
    }

    #[test]
    fn live_trocador_snapshot_pairs_have_chain_key_candidates() {
        let coins: Vec<SnapshotCoin> =
            serde_json::from_str(include_str!("../../../trocador_currencies_full.json"))
                .expect("snapshot should parse");

        for coin in coins {
            let candidates = chain_key_candidates(&coin.ticker, &coin.network);
            assert!(
                !candidates.is_empty(),
                "missing chain key candidates for {}/{}",
                coin.ticker,
                coin.network
            );
        }
    }

    #[test]
    fn live_trocador_snapshot_audit_prints_configured_coverage() {
        let configured = build_default_rpc_configs();
        let coins: Vec<SnapshotCoin> =
            serde_json::from_str(include_str!("../../../trocador_currencies_full.json"))
                .expect("snapshot should parse");

        let mut resolved = 0usize;
        let mut unresolved = BTreeSet::new();
        let mut unresolved_candidates = BTreeMap::<String, usize>::new();

        for coin in &coins {
            match resolve_configured_chain_key(&coin.ticker, &coin.network, |key| {
                configured.contains_key(key)
            }) {
                Ok(_) => resolved += 1,
                Err(_) => {
                    unresolved.insert(format!("{}/{}", coin.ticker, coin.network));
                    let candidate_key =
                        chain_key_candidates(&coin.ticker, &coin.network).join(" | ");
                    *unresolved_candidates.entry(candidate_key).or_default() += 1;
                }
            }
        }

        println!(
            "configured send-path resolution coverage: {}/{}",
            resolved,
            coins.len()
        );
        println!("unresolved pair count: {}", unresolved.len());
        for sample in unresolved.iter().take(25) {
            println!("unresolved: {}", sample);
        }
        println!("missing chain candidates:");
        for (candidate, count) in unresolved_candidates {
            println!("{} -> {}", candidate, count);
        }
    }
}
