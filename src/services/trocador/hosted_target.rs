#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostedSwapRecipientConfig {
    pub label: Option<String>,
    pub to: String,
    pub network_to: String,
    pub recipient_address: String,
    pub recipient_extra_id: Option<String>,
}

impl HostedSwapRecipientConfig {
    pub fn from_env() -> Result<Option<Self>, String> {
        normalize_hosted_swap_recipient(
            std::env::var("DONATION_TARGET_TICKER").ok().as_deref(),
            std::env::var("DONATION_TARGET_NETWORK").ok().as_deref(),
            std::env::var("DONATION_TARGET_ADDRESS").ok().as_deref(),
            std::env::var("DONATION_TARGET_EXTRA_ID").ok().as_deref(),
            std::env::var("DONATION_TARGET_LABEL").ok().as_deref(),
        )
    }
}

fn normalize_hosted_swap_recipient(
    ticker: Option<&str>,
    network: Option<&str>,
    address: Option<&str>,
    extra_id: Option<&str>,
    label: Option<&str>,
) -> Result<Option<HostedSwapRecipientConfig>, String> {
    let ticker = normalize_optional_env(ticker);
    let network = normalize_optional_env(network);
    let address = normalize_optional_env(address);
    let extra_id = normalize_optional_env(extra_id);
    let label = normalize_optional_env(label);

    if ticker.is_none()
        && network.is_none()
        && address.is_none()
        && extra_id.is_none()
        && label.is_none()
    {
        return Ok(None);
    }

    let to = ticker.ok_or_else(|| {
        "DONATION_TARGET_TICKER must be set when configuring the hosted donation flow".to_string()
    })?;
    let network_to = network.ok_or_else(|| {
        "DONATION_TARGET_NETWORK must be set when configuring the hosted donation flow".to_string()
    })?;
    let recipient_address = address.ok_or_else(|| {
        "DONATION_TARGET_ADDRESS must be set when configuring the hosted donation flow".to_string()
    })?;

    Ok(Some(HostedSwapRecipientConfig {
        label,
        to,
        network_to,
        recipient_address,
        recipient_extra_id: extra_id,
    }))
}

fn normalize_optional_env(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{normalize_hosted_swap_recipient, HostedSwapRecipientConfig};

    #[test]
    fn empty_env_disables_hosted_target() {
        assert_eq!(
            normalize_hosted_swap_recipient(None, None, None, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn configured_target_is_loaded() {
        let config = normalize_hosted_swap_recipient(
            Some("xmr"),
            Some("Mainnet"),
            Some("83r6..."),
            None,
            Some("Monero donations"),
        )
        .unwrap();

        assert_eq!(
            config,
            Some(HostedSwapRecipientConfig {
                label: Some("Monero donations".to_string()),
                to: "xmr".to_string(),
                network_to: "Mainnet".to_string(),
                recipient_address: "83r6...".to_string(),
                recipient_extra_id: None,
            })
        );
    }

    #[test]
    fn partial_configuration_is_rejected() {
        let err = normalize_hosted_swap_recipient(Some("xmr"), Some("Mainnet"), None, None, None)
            .expect_err("address is mandatory");

        assert!(err.contains("DONATION_TARGET_ADDRESS"));
    }
}
