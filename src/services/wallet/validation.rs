use crate::services::wallet::catalog::{mainnet_family, MainnetFamily};
use crate::services::wallet::blockchains::encoding::{
    base32_decode_nopad, base32_encode_nopad, c32check_decode, cashaddr_decode, cashaddr_encode,
    crc16_xmodem, sha512_256, waves_secure_hash,
};
use alloy::primitives::Address as EvmAddress;
use base64::{engine::general_purpose, Engine};
use bech32::decode as bech32_decode;
use bitcoin::{address::NetworkUnchecked, Address as BitcoinAddress, Network};
use blake2::{Blake2b512, Digest as Blake2Digest};
use monero::{Address as MoneroAddress, Network as MoneroNetwork};
use ripemd::Ripemd160;
use sha2::Sha256;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tiny_keccak::{Hasher, Keccak};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressValidation {
    Valid {
        family: &'static str,
    },
    Invalid {
        family: &'static str,
        reason: String,
    },
    Unsupported {
        family: &'static str,
        reason: String,
    },
}

impl AddressValidation {
    pub fn family(&self) -> &'static str {
        match self {
            AddressValidation::Valid { family }
            | AddressValidation::Invalid { family, .. }
            | AddressValidation::Unsupported { family, .. } => family,
        }
    }
}

pub fn validate_address_by_network_family(
    ticker: &str,
    network: &str,
    address: &str,
) -> AddressValidation {
    let ticker_lower = ticker.to_ascii_lowercase();
    let network_lower = network.to_ascii_lowercase();

    if network_lower == "mainnet" {
        return validate_mainnet_family(&ticker_lower, address);
    }

    match network_lower.as_str() {
        "bitcoin" | "btc" | "lightning" | "bitcoin_lightning" | "brc20" | "bitcoin_brc20"
        | "omni" => validate_bitcoin_network(address),
        "litecoin" | "ltc" => validate_base58check(
            "litecoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x30],
            &[20],
        ),
        "dogecoin" | "doge" => validate_base58check(
            "dogecoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x1e],
            &[20],
        ),
        "bitcoin_cash" | "bch" => validate_cashaddr_p2pkh(address),
        "bitcoin_sv" | "bsv" | "bchsv" => validate_bitcoin_network(address),
        "dash" => validate_base58check("dash", address, &bs58::Alphabet::DEFAULT, &[0x4c], &[20]),
        "ravencoin" | "rvn" => validate_base58check(
            "ravencoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x3c],
            &[20],
        ),
        "zcash" | "zec" | "shielded" => validate_base58check_prefix(
            "zcash",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        "bitcoinz" | "btcz" => validate_base58check_prefix(
            "bitcoinz",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        "solana" | "sol" => validate_solana(address),
        "cosmos" | "cosmos_hub" => validate_bech32_hrp("cosmos", address),
        "osmosis" => validate_bech32_hrp("osmo", address),
        "juno" => validate_bech32_hrp("juno", address),
        "akash" => validate_bech32_hrp("akash", address),
        "injective" => validate_bech32_hrp("inj", address),
        "regen" => validate_bech32_hrp("regen", address),
        "stargaze" => validate_bech32_hrp("stars", address),
        "secret" => validate_bech32_hrp("secret", address),
        "band" => validate_bech32_hrp("band", address),
        "ion" => validate_bech32_hrp("ion", address),
        "gravity" => validate_bech32_hrp("gravity", address),
        "polkadot" | "dot" | "kusama" | "ksm" | "acala" | "astar" | "shiden" | "parallel"
        | "dock" => validate_ss58(address),
        "cardano" | "ada" => validate_bech32_hrp("addr", address),
        "monero" | "xmr" => validate_monero_mainnet(address),
        "neo" | "neo_n2" | "n2" | "n3" | "neo3" => {
            validate_base58check("neo", address, &bs58::Alphabet::DEFAULT, &[0x35], &[20])
        }
        "icon" | "icx" => validate_hex_prefixed("icon", address, "hx", 40),
        "algorand" | "algo" => validate_algorand(address),
        "near" => validate_near_account(address),
        "tezos" | "xtz" => validate_tezos(address),
        "ripple" | "xrp" => validate_xrp_classic(address),
        "stacks" | "stx" => validate_stacks(address),
        "stellar" | "xlm" => validate_stellar(address),
        "tron" | "trx" | "trc20" => validate_tron(address),
        "waves" => validate_waves(address),
        "ton" => validate_ton(address),
        "vechain" | "vet" => validate_evm_address("vechain", address),
        "sui" | "aptos" | "apt" | "starknet" | "stark" | "strk" => {
            validate_hex_prefixed("hex-32", address, "0x", 64)
        }
        "eos" => validate_eos_legacy(address),
        "hedera" | "hbar" => validate_hedera_account(address),
        "mina" => validate_mina(address),
        "flow" => validate_hex_prefixed("flow", address, "0x", 16),
        "theta" => validate_evm_address("theta", address),
        "zilliqa" | "zil" => validate_bech32_hrp("zil", address),
        "multiversx" | "egld" => validate_bech32_hrp("erd", address),
        "nimiq" | "nim" => validate_nimiq_current(address),
        "flux" | "zel" => validate_base58check_prefix(
            "flux",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        "ontology" | "ont" => validate_base58check(
            "ontology",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x17],
            &[20],
        ),
        "pocket" | "pokt" => validate_hex_len("pocket", address, 64),
        "zano" => validate_zano_current(address),
        "binance_chain" | "bep2" => validate_bech32_hrp("bnb", address),
        "partisia" | "mpc" => validate_hex_prefixed("partisia", address, "00", 64),
        "defichain" | "dfi" => validate_base58check(
            "defichain",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x1c],
            &[20],
        ),
        "beam" => validate_beam_current(address),
        "everscale" | "freeton" | "ever" => validate_everscale(address),
        "terra" | "terra_classic" | "luna" | "lunc" => validate_bech32_hrp("terra", address),
        "factom" | "fct" => validate_factom_current(address),
        "avalanche_x" | "avaxx" => validate_prefixed_bech32("X-", "avax", address),
        "a2z" => validate_evm_address("a2z", address),
        _ if is_evm_network(&network_lower) => validate_evm_address("evm", address),
        _ => unsupported(
            "unknown",
            format!("no validator mapped for network {network}"),
        ),
    }
}

fn validate_mainnet_family(ticker_lower: &str, address: &str) -> AddressValidation {
    match mainnet_family(ticker_lower) {
        MainnetFamily::Monero => validate_monero_mainnet(address),
        MainnetFamily::Bitcoin | MainnetFamily::BitcoinLike | MainnetFamily::BitcoinSv => {
            validate_bitcoin_network(address)
        }
        MainnetFamily::Litecoin => validate_base58check(
            "litecoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x30],
            &[20],
        ),
        MainnetFamily::Dogecoin => validate_base58check(
            "dogecoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x1e],
            &[20],
        ),
        MainnetFamily::BitcoinCash => validate_cashaddr_p2pkh(address),
        MainnetFamily::Dash => {
            validate_base58check("dash", address, &bs58::Alphabet::DEFAULT, &[0x4c], &[20])
        }
        MainnetFamily::Zcash => validate_base58check_prefix(
            "zcash",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        MainnetFamily::Ravencoin => validate_base58check(
            "ravencoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x3c],
            &[20],
        ),
        MainnetFamily::Bitcoinz => validate_base58check_prefix(
            "bitcoinz",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        MainnetFamily::Monacoin => validate_base58check(
            "monacoin",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x32],
            &[20],
        ),
        MainnetFamily::Solana => validate_solana(address),
        MainnetFamily::Algorand => validate_algorand(address),
        MainnetFamily::Near => validate_near_account(address),
        MainnetFamily::Cardano => validate_bech32_hrp("addr", address),
        MainnetFamily::Polkadot
        | MainnetFamily::Kusama
        | MainnetFamily::Acala
        | MainnetFamily::Astar
        | MainnetFamily::Shiden
        | MainnetFamily::AlephZero
        | MainnetFamily::Avail
        | MainnetFamily::Bittensor
        | MainnetFamily::Centrifuge
        | MainnetFamily::Karura
        | MainnetFamily::Picasso
        | MainnetFamily::Polkadex
        | MainnetFamily::Polymesh
        | MainnetFamily::Ternoa
        | MainnetFamily::Vara
        | MainnetFamily::Dock => validate_ss58(address),
        MainnetFamily::Ripple => validate_xrp_classic(address),
        MainnetFamily::Tron => validate_tron(address),
        MainnetFamily::Stellar => validate_stellar(address),
        MainnetFamily::Sui | MainnetFamily::Aptos | MainnetFamily::Starknet => {
            validate_hex_prefixed("hex-32", address, "0x", 64)
        }
        MainnetFamily::Multiversx => validate_bech32_hrp("erd", address),
        MainnetFamily::Eos => validate_eos_legacy(address),
        MainnetFamily::Hedera => validate_hedera_account(address),
        MainnetFamily::Icon => validate_hex_prefixed("icon", address, "hx", 40),
        MainnetFamily::Mina => validate_mina(address),
        MainnetFamily::Neo3 => {
            validate_base58check("neo", address, &bs58::Alphabet::DEFAULT, &[0x35], &[20])
        }
        MainnetFamily::Nimiq => validate_nimiq_current(address),
        MainnetFamily::Ontology => validate_base58check(
            "ontology",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x17],
            &[20],
        ),
        MainnetFamily::Pocket => validate_hex_len("pocket", address, 64),
        MainnetFamily::Defichain => validate_base58check(
            "defichain",
            address,
            &bs58::Alphabet::DEFAULT,
            &[0x1c],
            &[20],
        ),
        MainnetFamily::Flow => validate_hex_prefixed("flow", address, "0x", 16),
        MainnetFamily::Stacks => validate_stacks(address),
        MainnetFamily::Tezos => validate_tezos(address),
        MainnetFamily::Theta | MainnetFamily::Vechain | MainnetFamily::Evm => {
            validate_evm_address("evm", address)
        }
        MainnetFamily::Ton => validate_ton(address),
        MainnetFamily::Terra => validate_bech32_hrp("terra", address),
        MainnetFamily::Waves => validate_waves(address),
        MainnetFamily::Zilliqa => validate_bech32_hrp("zil", address),
        MainnetFamily::Everscale => validate_everscale(address),
        MainnetFamily::Factom => validate_factom_current(address),
        MainnetFamily::Flux => validate_base58check_prefix(
            "flux",
            address,
            &bs58::Alphabet::DEFAULT,
            &[&[0x1c, 0xb8]],
            &[20],
        ),
        MainnetFamily::CosmosHub => validate_bech32_hrp("cosmos", address),
        MainnetFamily::Osmosis => validate_bech32_hrp("osmo", address),
        MainnetFamily::Juno => validate_bech32_hrp("juno", address),
        MainnetFamily::Akash => validate_bech32_hrp("akash", address),
        MainnetFamily::Injective => validate_bech32_hrp("inj", address),
        MainnetFamily::Regen => validate_bech32_hrp("regen", address),
        MainnetFamily::Stargaze => validate_bech32_hrp("stars", address),
        MainnetFamily::Secret => validate_bech32_hrp("secret", address),
        MainnetFamily::Band => validate_bech32_hrp("band", address),
        MainnetFamily::Ion => validate_bech32_hrp("ion", address),
        MainnetFamily::GravityBridge => validate_bech32_hrp("gravity", address),
        MainnetFamily::Cronos => validate_bech32_hrp("cro", address),
        MainnetFamily::Kava => validate_bech32_hrp("kava", address),
        MainnetFamily::Agoric => validate_bech32_hrp("agoric", address),
        MainnetFamily::Axelar => validate_bech32_hrp("axelar", address),
        MainnetFamily::Cheqd => validate_bech32_hrp("cheqd", address),
        MainnetFamily::Coreum => validate_bech32_hrp("core", address),
        MainnetFamily::Shentu => validate_bech32_hrp("shentu", address),
        MainnetFamily::Dydx => validate_bech32_hrp("dydx", address),
        MainnetFamily::Dymension => validate_bech32_hrp("dym", address),
        MainnetFamily::Fetch => validate_bech32_hrp("fetch", address),
        MainnetFamily::Initia => validate_bech32_hrp("init", address),
        MainnetFamily::Kyve => validate_bech32_hrp("kyve", address),
        MainnetFamily::Neutron => validate_bech32_hrp("neutron", address),
        MainnetFamily::Oraichain => validate_bech32_hrp("orai", address),
        MainnetFamily::Persistence => validate_bech32_hrp("persistence", address),
        MainnetFamily::Sei => validate_bech32_hrp("sei", address),
        MainnetFamily::Celestia => validate_bech32_hrp("celestia", address),
        MainnetFamily::Thorchain => validate_bech32_hrp("thor", address),
    }
}

fn validate_bitcoin_network(address: &str) -> AddressValidation {
    match address
        .parse::<BitcoinAddress<NetworkUnchecked>>()
        .and_then(|parsed| parsed.require_network(Network::Bitcoin))
    {
        Ok(_) => valid("bitcoin"),
        Err(err) => invalid("bitcoin", err.to_string()),
    }
}

fn validate_solana(address: &str) -> AddressValidation {
    match Pubkey::from_str(address) {
        Ok(_) => valid("solana"),
        Err(err) => invalid("solana", err.to_string()),
    }
}

fn validate_monero_mainnet(address: &str) -> AddressValidation {
    match MoneroAddress::from_str(address) {
        Ok(parsed) if parsed.network == MoneroNetwork::Mainnet => valid("monero"),
        Ok(parsed) => invalid(
            "monero",
            format!("unexpected Monero network {:?}", parsed.network),
        ),
        Err(err) => invalid("monero", err.to_string()),
    }
}

fn validate_cashaddr_p2pkh(address: &str) -> AddressValidation {
    match cashaddr_decode(address) {
        Ok((prefix, version, payload)) => {
            if prefix != "bitcoincash" {
                return invalid(
                    "bitcoin-cash",
                    format!("unexpected CashAddr prefix {prefix}"),
                );
            }
            if version != 0 {
                return invalid(
                    "bitcoin-cash",
                    format!("unexpected CashAddr version byte {version}"),
                );
            }
            if payload.len() != 20 {
                return invalid(
                    "bitcoin-cash",
                    format!("unexpected CashAddr payload length {}", payload.len()),
                );
            }

            match cashaddr_encode(&prefix, version, &payload) {
                Ok(canonical) if canonical == address.to_ascii_lowercase() => valid("bitcoin-cash"),
                Ok(_) => invalid("bitcoin-cash", "non-canonical CashAddr encoding".to_string()),
                Err(err) => invalid("bitcoin-cash", err),
            }
        }
        Err(err) => invalid("bitcoin-cash", err),
    }
}

fn validate_evm_address(family: &'static str, address: &str) -> AddressValidation {
    if EvmAddress::parse_checksummed(address, None).is_ok() || EvmAddress::from_str(address).is_ok()
    {
        valid(family)
    } else {
        invalid(family, "invalid EVM address".to_string())
    }
}

fn validate_bech32_hrp(expected_hrp: &'static str, address: &str) -> AddressValidation {
    match bech32_decode(address) {
        Ok((hrp, data)) => {
            if hrp.as_str().eq_ignore_ascii_case(expected_hrp) {
                if data.is_empty() {
                    invalid(expected_hrp, "bech32 payload is empty".to_string())
                } else {
                    valid(expected_hrp)
                }
            } else {
                invalid(
                    expected_hrp,
                    format!(
                        "unexpected bech32 hrp {}, expected {}",
                        hrp.as_str(),
                        expected_hrp
                    ),
                )
            }
        }
        Err(err) => invalid(expected_hrp, err.to_string()),
    }
}

fn validate_prefixed_bech32(
    prefix: &'static str,
    expected_hrp: &'static str,
    address: &str,
) -> AddressValidation {
    let Some(rest) = address.strip_prefix(prefix) else {
        return invalid(expected_hrp, format!("address must start with {prefix}"));
    };

    validate_bech32_hrp(expected_hrp, rest)
}

fn validate_hex_prefixed(
    family: &'static str,
    address: &str,
    prefix: &'static str,
    hex_len: usize,
) -> AddressValidation {
    let Some(rest) = address.strip_prefix(prefix) else {
        return invalid(family, format!("address must start with {prefix}"));
    };

    if rest.len() != hex_len {
        return invalid(
            family,
            format!("expected {hex_len} hex chars, found {}", rest.len()),
        );
    }

    if rest.chars().all(|c| c.is_ascii_hexdigit()) {
        valid(family)
    } else {
        invalid(family, "address contains non-hex characters".to_string())
    }
}

fn validate_hex_len(family: &'static str, address: &str, hex_len: usize) -> AddressValidation {
    if address.len() != hex_len {
        return invalid(
            family,
            format!("expected {hex_len} hex chars, found {}", address.len()),
        );
    }

    if address.chars().all(|c| c.is_ascii_hexdigit()) {
        valid(family)
    } else {
        invalid(family, "address contains non-hex characters".to_string())
    }
}

fn validate_near_account(address: &str) -> AddressValidation {
    if is_lower_hex(address) && address.len() == 64 {
        return valid("near");
    }

    if !(2..=64).contains(&address.len()) {
        return invalid(
            "near",
            "NEAR account id length must be between 2 and 64".to_string(),
        );
    }
    if address.starts_with(['-', '_', '.']) || address.ends_with(['-', '_', '.']) {
        return invalid(
            "near",
            "NEAR account id must not start or end with a separator".to_string(),
        );
    }

    let mut prev_sep = false;
    for ch in address.chars() {
        let is_sep = matches!(ch, '-' | '_' | '.');
        let is_valid = ch.is_ascii_lowercase() || ch.is_ascii_digit() || is_sep;
        if !is_valid {
            return invalid("near", format!("invalid NEAR account character {ch}"));
        }
        if is_sep && prev_sep {
            return invalid(
                "near",
                "NEAR account id must not contain consecutive separators".to_string(),
            );
        }
        prev_sep = is_sep;
    }

    valid("near")
}

fn validate_tezos(address: &str) -> AddressValidation {
    let data = match bs58::decode(address).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("tezos", err.to_string()),
    };

    if data.len() != 27 {
        return invalid(
            "tezos",
            format!("unexpected Tezos payload length {}", data.len()),
        );
    }

    if data[..3] != [6, 161, 159] {
        return invalid("tezos", "unexpected Tezos prefix bytes".to_string());
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let mut hasher = Blake2b512::new();
    hasher.update(body);
    let digest = hasher.finalize();
    if checksum != &digest[..4] {
        return invalid("tezos", "Tezos checksum mismatch".to_string());
    }

    valid("tezos")
}

fn validate_hedera_account(address: &str) -> AddressValidation {
    let parts: Vec<_> = address.split('.').collect();
    if parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()))
    {
        valid("hedera")
    } else {
        invalid("hedera", "invalid Hedera account id".to_string())
    }
}

fn validate_ton(address: &str) -> AddressValidation {
    let decoded = general_purpose::URL_SAFE
        .decode(address)
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(address));

    let data = match decoded {
        Ok(data) => data,
        Err(err) => return invalid("ton", err.to_string()),
    };

    if data.len() != 36 {
        return invalid(
            "ton",
            format!("TON address must decode to 36 bytes, found {}", data.len()),
        );
    }

    let expected_crc = crc16_xmodem(&data[..34]).to_be_bytes();
    if data[34..] != expected_crc {
        return invalid("ton", "TON CRC16 checksum mismatch".to_string());
    }

    valid("ton")
}

fn validate_everscale(address: &str) -> AddressValidation {
    let Some((workchain, hash)) = address.split_once(':') else {
        return invalid(
            "everscale",
            "Everscale address must contain ':'".to_string(),
        );
    };

    if workchain.parse::<i8>().is_err() {
        return invalid("everscale", "invalid Everscale workchain id".to_string());
    }
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return invalid("everscale", "invalid Everscale hash payload".to_string());
    }

    valid("everscale")
}

fn validate_tron(address: &str) -> AddressValidation {
    validate_base58check("tron", address, &bs58::Alphabet::DEFAULT, &[0x41], &[20])
}

fn validate_xrp_classic(address: &str) -> AddressValidation {
    validate_base58check("xrp", address, bs58::Alphabet::RIPPLE, &[0x00], &[20])
}

fn validate_algorand(address: &str) -> AddressValidation {
    if address.len() != 58 {
        return invalid(
            "algorand",
            format!("Algorand address must be 58 chars, found {}", address.len()),
        );
    }

    let decoded = match base32_decode_nopad(address) {
        Ok(decoded) => decoded,
        Err(err) => return invalid("algorand", err),
    };

    if decoded.len() != 36 {
        return invalid(
            "algorand",
            format!("Algorand payload must decode to 36 bytes, found {}", decoded.len()),
        );
    }

    let (public_key, checksum) = decoded.split_at(32);
    let expected = sha512_256(public_key);
    if checksum != &expected[28..32] {
        return invalid("algorand", "Algorand checksum mismatch".to_string());
    }

    if base32_encode_nopad(&decoded) != address.to_ascii_uppercase() {
        return invalid("algorand", "non-canonical Algorand encoding".to_string());
    }

    valid("algorand")
}

fn validate_stellar(address: &str) -> AddressValidation {
    let decoded = match base32_decode_nopad(address) {
        Ok(decoded) => decoded,
        Err(err) => return invalid("stellar", err),
    };

    if decoded.len() != 35 {
        return invalid(
            "stellar",
            format!("Stellar StrKey must decode to 35 bytes, found {}", decoded.len()),
        );
    }

    if decoded[0] != (6u8 << 3) {
        return invalid(
            "stellar",
            format!("unexpected Stellar version byte 0x{:02x}", decoded[0]),
        );
    }

    let checksum_index = decoded.len() - 2;
    let expected = crc16_xmodem(&decoded[..checksum_index]).to_le_bytes();
    if decoded[checksum_index..] != expected {
        return invalid("stellar", "Stellar checksum mismatch".to_string());
    }

    if base32_encode_nopad(&decoded) != address.to_ascii_uppercase() {
        return invalid("stellar", "non-canonical Stellar StrKey".to_string());
    }

    valid("stellar")
}

fn validate_stacks(address: &str) -> AddressValidation {
    match c32check_decode(address) {
        Ok((version, payload)) => {
            if version != 22 {
                return invalid(
                    "stacks",
                    format!("unexpected Stacks single-sig version {version}"),
                );
            }
            if payload.len() != 20 {
                return invalid(
                    "stacks",
                    format!("unexpected Stacks payload length {}", payload.len()),
                );
            }
            valid("stacks")
        }
        Err(err) => invalid("stacks", err),
    }
}

fn validate_waves(address: &str) -> AddressValidation {
    let data = match bs58::decode(address).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("waves", err.to_string()),
    };

    if data.len() != 26 {
        return invalid(
            "waves",
            format!("Waves address must decode to 26 bytes, found {}", data.len()),
        );
    }
    if data[0] != 0x01 {
        return invalid(
            "waves",
            format!("unexpected Waves version byte 0x{:02x}", data[0]),
        );
    }
    if data[1] != b'W' {
        return invalid(
            "waves",
            format!("unexpected Waves chain id 0x{:02x}", data[1]),
        );
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let expected = waves_secure_hash(body);
    if checksum != &expected[..4] {
        return invalid("waves", "Waves checksum mismatch".to_string());
    }

    valid("waves")
}

fn validate_eos_legacy(address: &str) -> AddressValidation {
    let Some(rest) = address.strip_prefix("EOS") else {
        return invalid("eos", "EOS legacy public key must start with EOS".to_string());
    };

    let data = match bs58::decode(rest).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("eos", err.to_string()),
    };

    if data.len() != 37 {
        return invalid(
            "eos",
            format!("EOS legacy public key payload must be 37 bytes, found {}", data.len()),
        );
    }

    let (public_key, checksum) = data.split_at(33);
    if !matches!(public_key[0], 0x02 | 0x03) {
        return invalid("eos", "EOS public key must be compressed".to_string());
    }

    let mut hasher = Ripemd160::new();
    hasher.update(public_key);
    hasher.update(b"EOS");
    let expected = hasher.finalize();
    if checksum != &expected[..4] {
        return invalid("eos", "EOS legacy checksum mismatch".to_string());
    }

    valid("eos")
}

fn validate_mina(address: &str) -> AddressValidation {
    let Some(rest) = address.strip_prefix("B62") else {
        return invalid("mina", "Mina address must start with B62".to_string());
    };

    validate_base58check_prefix("mina", rest, &bs58::Alphabet::DEFAULT, &[&[0xcb]], &[32])
}

fn validate_nimiq_current(address: &str) -> AddressValidation {
    validate_hex_prefixed("nimiq", address, "NQ", 40)
}

fn validate_beam_current(address: &str) -> AddressValidation {
    let data = match bs58::decode(address).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("beam", err.to_string()),
    };

    if data.len() == 32 {
        valid("beam")
    } else {
        invalid(
            "beam",
            format!("Beam placeholder address must decode to 32 bytes, found {}", data.len()),
        )
    }
}

fn validate_factom_current(address: &str) -> AddressValidation {
    validate_hex_prefixed("factom", address, "FA", 40)
}

fn validate_zano_current(address: &str) -> AddressValidation {
    let data = match bs58::decode(address).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("zano", err.to_string()),
    };

    if data.len() != 37 {
        return invalid(
            "zano",
            format!("Zano placeholder address must decode to 37 bytes, found {}", data.len()),
        );
    }
    if data[0] != 0x06 {
        return invalid(
            "zano",
            format!("unexpected Zano network byte 0x{:02x}", data[0]),
        );
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let mut expected = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(body);
    keccak.finalize(&mut expected);
    if checksum != &expected[..4] {
        return invalid("zano", "Zano checksum mismatch".to_string());
    }

    valid("zano")
}

fn validate_ss58(address: &str) -> AddressValidation {
    let data = match bs58::decode(address).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid("ss58", err.to_string()),
    };

    if data.len() < 3 {
        return invalid("ss58", "SS58 payload is too short".to_string());
    }

    let checksum_len = match data.len() {
        3..=34 => 1,
        _ => 2,
    };
    if data.len() <= checksum_len {
        return invalid("ss58", "SS58 payload is too short".to_string());
    }

    let (body, checksum) = data.split_at(data.len() - checksum_len);
    let mut hasher = Blake2b512::new();
    hasher.update(b"SS58PRE");
    hasher.update(body);
    let digest = hasher.finalize();

    if checksum != &digest[..checksum_len] {
        return invalid("ss58", "SS58 checksum mismatch".to_string());
    }

    valid("ss58")
}

fn validate_base58check(
    family: &'static str,
    address: &str,
    alphabet: &'static bs58::Alphabet,
    expected_versions: &[u8],
    payload_lengths: &[usize],
) -> AddressValidation {
    let data = match bs58::decode(address).with_alphabet(alphabet).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid(family, err.to_string()),
    };

    if data.len() < 5 {
        return invalid(family, "base58check payload is too short".to_string());
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let expected_checksum = double_sha256(body);
    if checksum != &expected_checksum[..4] {
        return invalid(family, "base58check checksum mismatch".to_string());
    }

    let version = body[0];
    if !expected_versions.is_empty() && !expected_versions.contains(&version) {
        return invalid(family, format!("unexpected version byte 0x{version:02x}"));
    }

    let payload_len = body.len() - 1;
    if !payload_lengths.is_empty() && !payload_lengths.contains(&payload_len) {
        return invalid(family, format!("unexpected payload length {payload_len}"));
    }

    valid(family)
}

fn validate_base58check_prefix(
    family: &'static str,
    address: &str,
    alphabet: &'static bs58::Alphabet,
    expected_prefixes: &[&[u8]],
    payload_lengths: &[usize],
) -> AddressValidation {
    let data = match bs58::decode(address).with_alphabet(alphabet).into_vec() {
        Ok(data) => data,
        Err(err) => return invalid(family, err.to_string()),
    };

    if data.len() < 5 {
        return invalid(family, "base58check payload is too short".to_string());
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let expected_checksum = double_sha256(body);
    if checksum != &expected_checksum[..4] {
        return invalid(family, "base58check checksum mismatch".to_string());
    }

    let version_len = match expected_prefixes
        .iter()
        .find(|expected_prefix| body.starts_with(expected_prefix))
    {
        Some(prefix) => prefix.len(),
        None => return invalid(family, "unexpected version bytes".to_string()),
    };

    let payload_len = body.len() - version_len;
    if !payload_lengths.is_empty() && !payload_lengths.contains(&payload_len) {
        return invalid(family, format!("unexpected payload length {payload_len}"));
    }

    valid(family)
}

fn double_sha256(data: &[u8]) -> [u8; 32] {
    let first = Sha256::digest(data);
    let second = Sha256::digest(first);
    let mut output = [0u8; 32];
    output.copy_from_slice(&second);
    output
}

fn is_lower_hex(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
}

fn is_evm_network(network_lower: &str) -> bool {
    matches!(
        network_lower,
        "ethereum"
            | "eth"
            | "polygon"
            | "matic"
            | "bsc"
            | "smartchain"
            | "arbitrum"
            | "optimism"
            | "erc20"
            | "bep20"
            | "base"
            | "cronos"
            | "avalanche"
            | "avaxc"
            | "fantom"
            | "ftm"
            | "celo"
            | "harmony"
            | "klaytn"
            | "klay"
            | "kai"
            | "kaia"
            | "kip7"
            | "metis"
            | "metall2"
            | "boba"
            | "gnosis"
            | "fuse"
            | "iotex"
            | "scroll"
            | "zksync"
            | "linea"
            | "mantle"
            | "manta_pacific"
            | "manta"
            | "mode"
            | "blast"
            | "taiko"
            | "zora"
            | "sonic"
            | "moonbeam"
            | "moonriver"
            | "aurora"
            | "evmos"
            | "kava"
            | "oasis"
            | "oasis sapphire"
            | "rootstock"
            | "rsk"
            | "syscoin"
            | "sysnevm"
            | "telos"
            | "tlosevm"
            | "thundercore"
            | "tomochain"
            | "velas"
            | "wanchain"
            | "whitechain"
            | "x_layer"
            | "zkfair"
            | "shibarium"
            | "opbnb"
            | "fraxtal"
            | "merlin"
            | "morph"
            | "redbelly"
            | "rei"
            | "step_network"
            | "fitfi"
            | "stratis"
            | "strax"
            | "cyber"
            | "endurance"
            | "hyper_evm"
            | "hyperevm"
            | "iota_evm"
            | "islm_evm"
            | "islmevm"
            | "haqq"
            | "okx_chain"
            | "oasys"
            | "oas"
            | "peaq"
            | "pulsechain"
            | "pulse"
            | "ronin"
            | "zeta"
            | "bitgert"
            | "botanix"
            | "bttc"
            | "btt"
            | "cfx"
            | "cfxcore"
            | "chiliz"
            | "chz"
            | "conflux_espace"
            | "core"
            | "filecoin"
            | "filevm"
            | "flare"
            | "flr"
            | "kcc"
            | "klc"
            | "bahamut"
            | "b2"
            | "berachain"
            | "bera"
            | "apechain"
            | "katana"
            | "lava"
            | "sei"
            | "seievm"
    )
}

fn valid(family: &'static str) -> AddressValidation {
    AddressValidation::Valid { family }
}

fn invalid(family: &'static str, reason: String) -> AddressValidation {
    AddressValidation::Invalid { family, reason }
}

fn unsupported(family: &'static str, reason: impl Into<String>) -> AddressValidation {
    AddressValidation::Unsupported {
        family,
        reason: reason.into(),
    }
}
