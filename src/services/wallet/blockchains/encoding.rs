use bech32::{Bech32, Hrp};
use blake2::digest::consts::U32;
use blake2::Blake2b;
use ripemd::Ripemd160;
use sha2::{Digest, Sha256, Sha512_256};
use tiny_keccak::{Hasher, Keccak};

const RFC4648_BASE32_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const CASHADDR_ALPHABET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const C32_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
type Blake2b256 = Blake2b<U32>;

pub(crate) fn hash160(data: &[u8]) -> [u8; 20] {
    let sha_hash = Sha256::digest(data);
    let ripe_hash = Ripemd160::digest(sha_hash);

    let mut out = [0u8; 20];
    out.copy_from_slice(&ripe_hash);
    out
}

pub(crate) fn double_sha256(data: &[u8]) -> [u8; 32] {
    let hash1 = Sha256::digest(data);
    let hash2 = Sha256::digest(hash1);

    let mut out = [0u8; 32];
    out.copy_from_slice(&hash2);
    out
}

pub(crate) fn base58check_encode(version: &[u8], payload: &[u8]) -> String {
    base58check_encode_with_alphabet(version, payload, bs58::Alphabet::DEFAULT)
}

pub(crate) fn base58check_encode_with_alphabet(
    version: &[u8],
    payload: &[u8],
    alphabet: &'static bs58::Alphabet,
) -> String {
    let mut body = Vec::with_capacity(version.len() + payload.len() + 4);
    body.extend_from_slice(version);
    body.extend_from_slice(payload);

    let checksum = double_sha256(&body);
    body.extend_from_slice(&checksum[..4]);

    bs58::encode(body).with_alphabet(alphabet).into_string()
}

pub(crate) fn base58check_decode(
    address: &str,
    alphabet: &'static bs58::Alphabet,
) -> Result<Vec<u8>, String> {
    let data = bs58::decode(address)
        .with_alphabet(alphabet)
        .into_vec()
        .map_err(|e| format!("Failed to decode base58check address: {e}"))?;

    if data.len() < 5 {
        return Err("Base58Check payload is too short".to_string());
    }

    let (body, checksum) = data.split_at(data.len() - 4);
    let expected = double_sha256(body);
    if checksum != &expected[..4] {
        return Err("Base58Check checksum mismatch".to_string());
    }

    Ok(body.to_vec())
}

pub(crate) fn bech32_encode(hrp: &str, data: &[u8]) -> Result<String, String> {
    let hrp = Hrp::parse(hrp).map_err(|e| format!("Invalid bech32 hrp {hrp}: {e}"))?;
    bech32::encode::<Bech32>(hrp, data).map_err(|e| format!("Failed to encode bech32: {e}"))
}

pub(crate) fn sha512_256(data: &[u8]) -> [u8; 32] {
    let digest = Sha512_256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

pub(crate) fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

pub(crate) fn base32_encode_nopad(data: &[u8]) -> String {
    encode_base32_with_alphabet(data, RFC4648_BASE32_ALPHABET)
}

pub(crate) fn base32_decode_nopad(input: &str) -> Result<Vec<u8>, String> {
    decode_base32_with_alphabet(input, base32_rfc4648_value)
}

pub(crate) fn cashaddr_encode(prefix: &str, version: u8, payload: &[u8]) -> Result<String, String> {
    let prefix = prefix.to_ascii_lowercase();

    if payload.is_empty() {
        return Err("CashAddr payload must not be empty".to_string());
    }

    let mut data = Vec::with_capacity(1 + payload.len());
    data.push(version);
    data.extend_from_slice(payload);

    let encoded_data = convert_bits(&data, 8, 5, true)?;
    let checksum = cashaddr_checksum(&prefix, &encoded_data);

    let mut out = String::with_capacity(prefix.len() + 1 + encoded_data.len() + checksum.len());
    out.push_str(&prefix);
    out.push(':');
    for value in encoded_data.iter().chain(checksum.iter()) {
        out.push(CASHADDR_ALPHABET[*value as usize] as char);
    }

    Ok(out)
}

pub(crate) fn cashaddr_decode(address: &str) -> Result<(String, u8, Vec<u8>), String> {
    let has_lower = address.chars().any(|c| c.is_ascii_lowercase());
    let has_upper = address.chars().any(|c| c.is_ascii_uppercase());
    if has_lower && has_upper {
        return Err("CashAddr must not mix upper and lower case".to_string());
    }

    let address = address.to_ascii_lowercase();
    let (prefix, payload) = address
        .split_once(':')
        .ok_or_else(|| "CashAddr must contain a prefix separator ':'".to_string())?;

    if prefix.is_empty() || payload.len() < 8 {
        return Err("CashAddr prefix or payload is too short".to_string());
    }

    let mut values = Vec::with_capacity(payload.len());
    for ch in payload.chars() {
        values.push(
            cashaddr_value(ch)
                .ok_or_else(|| format!("Invalid CashAddr character {ch}"))?,
        );
    }

    if !cashaddr_verify_checksum(prefix, &values) {
        return Err("CashAddr checksum mismatch".to_string());
    }

    let data = &values[..values.len() - 8];
    let decoded = convert_bits(data, 5, 8, false)?;
    let (&version, payload) = decoded
        .split_first()
        .ok_or_else(|| "CashAddr payload is empty".to_string())?;

    Ok((prefix.to_string(), version, payload.to_vec()))
}

pub(crate) fn c32check_encode(version: u8, payload: &[u8]) -> Result<String, String> {
    if version as usize >= C32_ALPHABET.len() {
        return Err(format!("Unsupported c32 version {version}"));
    }

    let mut body = payload.to_vec();
    let mut checksum_input = Vec::with_capacity(1 + payload.len());
    checksum_input.push(version);
    checksum_input.extend_from_slice(payload);
    let checksum = double_sha256(&checksum_input);
    body.extend_from_slice(&checksum[..4]);

    let mut out = String::from("S");
    out.push(C32_ALPHABET[version as usize] as char);
    out.push_str(&c32_encode_bytes(&body));
    Ok(out)
}

pub(crate) fn c32check_decode(address: &str) -> Result<(u8, Vec<u8>), String> {
    let rest = address
        .strip_prefix('S')
        .ok_or_else(|| "Stacks address must start with 'S'".to_string())?;
    let mut chars = rest.chars();
    let version_char = chars
        .next()
        .ok_or_else(|| "Stacks address is missing the c32 version character".to_string())?;
    let version = c32_value(version_char)
        .ok_or_else(|| format!("Invalid c32 version character {version_char}"))?;

    let body = c32_decode_bytes(chars.as_str())?;
    if body.len() < 5 {
        return Err("Stacks c32 payload is too short".to_string());
    }

    let (payload, checksum) = body.split_at(body.len() - 4);
    let mut checksum_input = Vec::with_capacity(1 + payload.len());
    checksum_input.push(version);
    checksum_input.extend_from_slice(payload);
    let expected = double_sha256(&checksum_input);
    if checksum != &expected[..4] {
        return Err("Stacks c32 checksum mismatch".to_string());
    }

    Ok((version, payload.to_vec()))
}

pub(crate) fn waves_secure_hash(data: &[u8]) -> [u8; 32] {
    let blake = Blake2b256::digest(data);
    let mut out = [0u8; 32];
    let mut keccak = Keccak::v256();
    keccak.update(&blake);
    keccak.finalize(&mut out);
    out
}

fn encode_base32_with_alphabet(data: &[u8], alphabet: &[u8; 32]) -> String {
    let mut out = String::new();
    let mut buffer: u16 = 0;
    let mut bits: usize = 0;

    for &byte in data {
        buffer = (buffer << 8) | byte as u16;
        bits += 8;

        while bits >= 5 {
            bits -= 5;
            let index = ((buffer >> bits) & 0x1f) as usize;
            out.push(alphabet[index] as char);
        }
    }

    if bits > 0 {
        let index = ((buffer << (5 - bits)) & 0x1f) as usize;
        out.push(alphabet[index] as char);
    }

    out
}

fn decode_base32_with_alphabet(
    input: &str,
    value_fn: impl Fn(char) -> Option<u8>,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut buffer: u32 = 0;
    let mut bits: usize = 0;

    for ch in input.chars() {
        if ch == '=' {
            break;
        }

        let value = value_fn(ch).ok_or_else(|| format!("Invalid base32 character {ch}"))?;
        buffer = (buffer << 5) | value as u32;
        bits += 5;

        while bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }

    if bits > 0 && (buffer & ((1 << bits) - 1)) != 0 {
        return Err("Invalid base32 padding".to_string());
    }

    Ok(out)
}

fn base32_rfc4648_value(ch: char) -> Option<u8> {
    match ch.to_ascii_uppercase() {
        'A'..='Z' => Some(ch.to_ascii_uppercase() as u8 - b'A'),
        '2'..='7' => Some(26 + (ch as u8 - b'2')),
        _ => None,
    }
}

fn convert_bits(data: &[u8], from: u32, to: u32, pad: bool) -> Result<Vec<u8>, String> {
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let maxv: u32 = (1 << to) - 1;
    let max_acc: u32 = (1 << (from + to - 1)) - 1;
    let mut out = Vec::new();

    for &value in data {
        if (value as u32) >> from != 0 {
            return Err("convert_bits received an out-of-range value".to_string());
        }

        acc = ((acc << from) | value as u32) & max_acc;
        bits += from;
        while bits >= to {
            bits -= to;
            out.push(((acc >> bits) & maxv) as u8);
        }
    }

    if pad {
        if bits > 0 {
            out.push(((acc << (to - bits)) & maxv) as u8);
        }
    } else if bits >= from || ((acc << (to - bits)) & maxv) != 0 {
        return Err("convert_bits found non-zero padding".to_string());
    }

    Ok(out)
}

fn cashaddr_prefix_expand(prefix: &str) -> Vec<u8> {
    let mut values = prefix.bytes().map(|b| b & 0x1f).collect::<Vec<_>>();
    values.push(0);
    values
}

fn cashaddr_polymod(values: &[u8]) -> u64 {
    let mut c: u64 = 1;
    for &value in values {
        let c0 = c >> 35;
        c = ((c & 0x07_ff_ff_ff_ff) << 5) ^ value as u64;
        if (c0 & 0x01) != 0 {
            c ^= 0x98_f2_bc_8e_61;
        }
        if (c0 & 0x02) != 0 {
            c ^= 0x79_b7_6d_99_e2;
        }
        if (c0 & 0x04) != 0 {
            c ^= 0xf3_3e_5f_b3_c4;
        }
        if (c0 & 0x08) != 0 {
            c ^= 0xae_2e_ab_e2_a8;
        }
        if (c0 & 0x10) != 0 {
            c ^= 0x1e_4f_43_e4_70;
        }
    }
    c ^ 1
}

fn cashaddr_checksum(prefix: &str, data: &[u8]) -> [u8; 8] {
    let mut values = cashaddr_prefix_expand(prefix);
    values.extend_from_slice(data);
    values.extend_from_slice(&[0u8; 8]);

    let polymod = cashaddr_polymod(&values);
    let mut checksum = [0u8; 8];
    for (index, slot) in checksum.iter_mut().enumerate() {
        *slot = ((polymod >> (5 * (7 - index))) & 0x1f) as u8;
    }
    checksum
}

fn cashaddr_verify_checksum(prefix: &str, data: &[u8]) -> bool {
    let mut values = cashaddr_prefix_expand(prefix);
    values.extend_from_slice(data);
    cashaddr_polymod(&values) == 0
}

fn cashaddr_value(ch: char) -> Option<u8> {
    CASHADDR_ALPHABET
        .iter()
        .position(|candidate| *candidate as char == ch)
        .map(|index| index as u8)
}

fn c32_encode_bytes(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }

    let leading_zero_bytes = data.iter().take_while(|&&byte| byte == 0).count();
    let mut working = data.to_vec();
    let mut encoded = Vec::new();
    let mut start = leading_zero_bytes;

    while start < working.len() {
        let mut remainder: u32 = 0;
        for byte in &mut working[start..] {
            let value = (remainder << 8) | *byte as u32;
            *byte = (value / 32) as u8;
            remainder = value % 32;
        }
        encoded.push(C32_ALPHABET[remainder as usize] as char);
        while start < working.len() && working[start] == 0 {
            start += 1;
        }
    }

    let mut out = String::new();
    for _ in 0..leading_zero_bytes {
        out.push('0');
    }
    while let Some(ch) = encoded.pop() {
        out.push(ch);
    }

    if out.is_empty() {
        out.push('0');
    }

    out
}

fn c32_decode_bytes(input: &str) -> Result<Vec<u8>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let leading_zero_chars = input.chars().take_while(|&ch| ch == '0').count();
    let mut out = Vec::<u8>::new();

    for ch in input.chars() {
        let value = c32_value(ch).ok_or_else(|| format!("Invalid c32 character {ch}"))? as u32;
        let mut carry = value;
        for byte in out.iter_mut().rev() {
            let next = (*byte as u32) * 32 + carry;
            *byte = (next & 0xff) as u8;
            carry = next >> 8;
        }
        while carry > 0 {
            out.insert(0, (carry & 0xff) as u8);
            carry >>= 8;
        }
    }

    let mut prefixed = vec![0u8; leading_zero_chars];
    prefixed.extend(out);
    Ok(prefixed)
}

fn c32_value(ch: char) -> Option<u8> {
    let upper = ch.to_ascii_uppercase();
    C32_ALPHABET
        .iter()
        .position(|candidate| *candidate as char == upper)
        .map(|index| index as u8)
}
