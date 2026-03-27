#[derive(Debug, Clone)]
pub struct CosmosSendTransaction {
    pub from_address: String,
    pub to_address: String,
    pub amount: String,
    pub denom: String,
    pub fee_amount: String,
    pub fee_denom: String,
    pub gas_limit: u64,
    pub chain_id: String,
    pub account_number: u64,
    pub sequence: u64,
    pub memo: Option<String>,
}

impl CosmosSendTransaction {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        from_address: String,
        to_address: String,
        amount: String,
        denom: String,
        fee_amount: String,
        fee_denom: String,
        gas_limit: u64,
        chain_id: String,
        account_number: u64,
        sequence: u64,
        memo: Option<String>,
    ) -> Self {
        Self {
            from_address,
            to_address,
            amount,
            denom,
            fee_amount,
            fee_denom,
            gas_limit,
            chain_id,
            account_number,
            sequence,
            memo,
        }
    }

    pub fn sign_doc_bytes(&self, public_key: &[u8]) -> Vec<u8> {
        let body_bytes = self.body_bytes();
        let auth_info_bytes = self.auth_info_bytes(public_key);

        let mut encoded = Vec::new();
        push_bytes_field(&mut encoded, 1, &body_bytes);
        push_bytes_field(&mut encoded, 2, &auth_info_bytes);
        push_string_field(&mut encoded, 3, &self.chain_id);
        push_varint_field(&mut encoded, 4, self.account_number);
        encoded
    }

    pub fn signed_tx_bytes(&self, public_key: &[u8], signature: &[u8]) -> Vec<u8> {
        let body_bytes = self.body_bytes();
        let auth_info_bytes = self.auth_info_bytes(public_key);

        let mut encoded = Vec::new();
        push_bytes_field(&mut encoded, 1, &body_bytes);
        push_bytes_field(&mut encoded, 2, &auth_info_bytes);
        push_bytes_field(&mut encoded, 3, signature);
        encoded
    }

    fn body_bytes(&self) -> Vec<u8> {
        let amount_coin = coin_bytes(&self.denom, &self.amount);
        let msg_send = msg_send_bytes(&self.from_address, &self.to_address, &amount_coin);
        let msg_any = any_bytes("/cosmos.bank.v1beta1.MsgSend", &msg_send);

        let mut encoded = Vec::new();
        push_message_field(&mut encoded, 1, &msg_any);

        if let Some(memo) = self.memo.as_ref().filter(|value| !value.is_empty()) {
            push_string_field(&mut encoded, 2, memo);
        }

        encoded
    }

    fn auth_info_bytes(&self, public_key: &[u8]) -> Vec<u8> {
        let fee_coin = coin_bytes(&self.fee_denom, &self.fee_amount);
        let pub_key_any = any_bytes(
            "/cosmos.crypto.secp256k1.PubKey",
            &secp256k1_pub_key_bytes(public_key),
        );
        let signer_info = signer_info_bytes(&pub_key_any, self.sequence);
        let fee = fee_bytes(&fee_coin, self.gas_limit);

        let mut encoded = Vec::new();
        push_message_field(&mut encoded, 1, &signer_info);
        push_message_field(&mut encoded, 2, &fee);
        encoded
    }
}

fn msg_send_bytes(from_address: &str, to_address: &str, amount_coin: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_string_field(&mut encoded, 1, from_address);
    push_string_field(&mut encoded, 2, to_address);
    push_message_field(&mut encoded, 3, amount_coin);
    encoded
}

fn signer_info_bytes(public_key_any: &[u8], sequence: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_message_field(&mut encoded, 1, public_key_any);
    push_message_field(&mut encoded, 2, &mode_info_bytes());
    push_varint_field(&mut encoded, 3, sequence);
    encoded
}

fn mode_info_bytes() -> Vec<u8> {
    let mut single = Vec::new();
    push_varint_field(&mut single, 1, 1); // SIGN_MODE_DIRECT

    let mut encoded = Vec::new();
    push_message_field(&mut encoded, 1, &single);
    encoded
}

fn fee_bytes(amount_coin: &[u8], gas_limit: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_message_field(&mut encoded, 1, amount_coin);
    push_varint_field(&mut encoded, 2, gas_limit);
    encoded
}

fn coin_bytes(denom: &str, amount: &str) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_string_field(&mut encoded, 1, denom);
    push_string_field(&mut encoded, 2, amount);
    encoded
}

fn any_bytes(type_url: &str, value: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_string_field(&mut encoded, 1, type_url);
    push_bytes_field(&mut encoded, 2, value);
    encoded
}

fn secp256k1_pub_key_bytes(public_key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_bytes_field(&mut encoded, 1, public_key);
    encoded
}

fn push_string_field(target: &mut Vec<u8>, field_number: u32, value: &str) {
    push_bytes_field(target, field_number, value.as_bytes());
}

fn push_message_field(target: &mut Vec<u8>, field_number: u32, value: &[u8]) {
    push_bytes_field(target, field_number, value);
}

fn push_bytes_field(target: &mut Vec<u8>, field_number: u32, value: &[u8]) {
    push_field_key(target, field_number, 2);
    push_varint(target, value.len() as u64);
    target.extend_from_slice(value);
}

fn push_varint_field(target: &mut Vec<u8>, field_number: u32, value: u64) {
    push_field_key(target, field_number, 0);
    push_varint(target, value);
}

fn push_field_key(target: &mut Vec<u8>, field_number: u32, wire_type: u8) {
    push_varint(target, ((field_number << 3) | wire_type as u32) as u64);
}

fn push_varint(target: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        target.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    target.push(value as u8);
}
