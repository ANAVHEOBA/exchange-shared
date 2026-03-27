use std::collections::BTreeSet;
use std::env;

const LOCAL_CERTIFIED_CHAINS_ENV: &str = "LOCAL_CERTIFIED_CHAINS";
const TROCADOR_ONLY_CHAINS_ENV: &str = "TROCADOR_ONLY_CHAINS";
const DEFAULT_LOCAL_CERTIFIED_CHAINS: &[&str] = &[
    "ethereum",
    "polygon",
    "arbitrum_one",
    "optimism",
    "base",
    "bnb_smart_chain",
    "avalanche_c_chain",
    "solana",
    "tron",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainPayoutPolicy {
    LocalCertified,
    TrocadorOnly,
}

impl ChainPayoutPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalCertified => "local_certified",
            Self::TrocadorOnly => "trocador_only",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PayoutPolicyConfig {
    local_certified: BTreeSet<String>,
    trocador_only: BTreeSet<String>,
}

impl Default for PayoutPolicyConfig {
    fn default() -> Self {
        let local_certified = DEFAULT_LOCAL_CERTIFIED_CHAINS
            .iter()
            .map(|chain| chain.to_string())
            .collect();

        Self {
            local_certified,
            trocador_only: BTreeSet::new(),
        }
    }
}

impl PayoutPolicyConfig {
    pub fn from_env() -> Self {
        let mut config = Self {
            local_certified: read_chain_set(LOCAL_CERTIFIED_CHAINS_ENV)
                .unwrap_or_else(|| Self::default().local_certified),
            trocador_only: read_chain_set(TROCADOR_ONLY_CHAINS_ENV).unwrap_or_default(),
        };

        config
            .local_certified
            .retain(|chain| !config.trocador_only.contains(chain));

        config
    }

    pub fn classify_chain_key(&self, chain_key: &str) -> ChainPayoutPolicy {
        let normalized = normalize_chain_key(chain_key);

        if self.trocador_only.contains(&normalized) {
            return ChainPayoutPolicy::TrocadorOnly;
        }

        if self.local_certified.contains(&normalized) {
            return ChainPayoutPolicy::LocalCertified;
        }

        ChainPayoutPolicy::TrocadorOnly
    }

    pub fn is_local_certified(&self, chain_key: &str) -> bool {
        self.classify_chain_key(chain_key) == ChainPayoutPolicy::LocalCertified
    }

    pub fn local_certified_chain_keys(&self) -> Vec<String> {
        self.local_certified.iter().cloned().collect()
    }

    pub fn trocador_only_chain_keys(&self) -> Vec<String> {
        self.trocador_only.iter().cloned().collect()
    }

    #[cfg(test)]
    fn from_sets(local_certified: &[&str], trocador_only: &[&str]) -> Self {
        let mut config = Self {
            local_certified: local_certified
                .iter()
                .map(|chain| normalize_chain_key(chain))
                .collect(),
            trocador_only: trocador_only
                .iter()
                .map(|chain| normalize_chain_key(chain))
                .collect(),
        };

        config
            .local_certified
            .retain(|chain| !config.trocador_only.contains(chain));

        config
    }
}

fn read_chain_set(name: &str) -> Option<BTreeSet<String>> {
    env::var(name).ok().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(normalize_chain_key)
            .collect()
    })
}

fn normalize_chain_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{ChainPayoutPolicy, PayoutPolicyConfig};

    #[test]
    fn default_policy_keeps_conservative_local_certified_set() {
        let policy = PayoutPolicyConfig::default();

        assert_eq!(
            policy.classify_chain_key("ethereum"),
            ChainPayoutPolicy::LocalCertified
        );
        assert_eq!(
            policy.classify_chain_key("solana"),
            ChainPayoutPolicy::LocalCertified
        );
        assert_eq!(
            policy.classify_chain_key("cardano"),
            ChainPayoutPolicy::TrocadorOnly
        );
        assert_eq!(
            policy.classify_chain_key("bitcoin"),
            ChainPayoutPolicy::TrocadorOnly
        );
    }

    #[test]
    fn trocador_only_override_wins_over_local_certified() {
        let policy = PayoutPolicyConfig::from_sets(&["ethereum", "tron"], &["tron"]);

        assert_eq!(
            policy.classify_chain_key("ethereum"),
            ChainPayoutPolicy::LocalCertified
        );
        assert_eq!(
            policy.classify_chain_key("tron"),
            ChainPayoutPolicy::TrocadorOnly
        );
    }
}
