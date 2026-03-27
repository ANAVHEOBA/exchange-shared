use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, sqlx::Type, ToSchema)]
#[serde(rename_all = "lowercase")]
#[sqlx(rename_all = "lowercase")]
pub enum SwapStatus {
    Waiting,
    Confirming,
    Exchanging,
    Sending,
    #[serde(rename = "funds_received")]
    #[sqlx(rename = "funds_received")]
    FundsReceived,
    Completed,
    Failed,
    Refunded,
    Expired,
}

impl SwapStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Confirming => "confirming",
            Self::Exchanging => "exchanging",
            Self::Sending => "sending",
            Self::FundsReceived => "funds_received",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Refunded => "refunded",
            Self::Expired => "expired",
        }
    }

    pub fn from_persisted(status: &str) -> Option<Self> {
        match status {
            "waiting" => Some(Self::Waiting),
            "confirming" => Some(Self::Confirming),
            "exchanging" => Some(Self::Exchanging),
            "sending" => Some(Self::Sending),
            "funds_received" => Some(Self::FundsReceived),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "refunded" => Some(Self::Refunded),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    pub fn from_trocador_status(status: &str) -> Self {
        match status {
            "new" | "waiting" => Self::Waiting,
            "confirming" => Self::Confirming,
            "exchanging" => Self::Exchanging,
            "sending" | "finished" | "paid partially" => Self::Sending,
            "failed" | "halted" => Self::Failed,
            "refunded" => Self::Refunded,
            "expired" => Self::Expired,
            _ => Self::Waiting,
        }
    }

    pub fn can_transition(&self, next: &Self) -> bool {
        if self == next {
            return true;
        }

        matches!(
            (self, next),
            (Self::Waiting, Self::Confirming)
                | (Self::Waiting, Self::Exchanging)
                | (Self::Waiting, Self::Sending)
                | (Self::Waiting, Self::Failed)
                | (Self::Waiting, Self::Refunded)
                | (Self::Waiting, Self::Expired)
                | (Self::Confirming, Self::Exchanging)
                | (Self::Confirming, Self::Sending)
                | (Self::Confirming, Self::Failed)
                | (Self::Confirming, Self::Refunded)
                | (Self::Confirming, Self::Expired)
                | (Self::Exchanging, Self::Sending)
                | (Self::Exchanging, Self::Failed)
                | (Self::Exchanging, Self::Refunded)
                | (Self::Exchanging, Self::Expired)
                | (Self::Sending, Self::FundsReceived)
                | (Self::Sending, Self::Failed)
                | (Self::Sending, Self::Refunded)
                | (Self::Sending, Self::Expired)
                | (Self::FundsReceived, Self::Completed)
                | (Self::FundsReceived, Self::Failed)
                | (Self::FundsReceived, Self::Refunded)
                | (Self::Failed, Self::Refunded)
        )
    }

    pub fn reconcile_with_provider(&self, provider_status: Self) -> Self {
        if self.can_transition(&provider_status) {
            provider_status
        } else {
            self.clone()
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Refunded | Self::Expired
        )
    }
}

impl Default for SwapStatus {
    fn default() -> Self {
        Self::Waiting
    }
}

impl std::fmt::Display for SwapStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::SwapStatus;

    #[test]
    fn maps_persisted_statuses() {
        assert_eq!(
            SwapStatus::from_persisted("funds_received"),
            Some(SwapStatus::FundsReceived)
        );
        assert_eq!(
            SwapStatus::from_persisted("completed"),
            Some(SwapStatus::Completed)
        );
        assert_eq!(SwapStatus::from_persisted("unknown"), None);
    }

    #[test]
    fn maps_trocador_completion_to_internal_sending_until_funds_arrive() {
        assert_eq!(
            SwapStatus::from_trocador_status("finished"),
            SwapStatus::Sending
        );
        assert_eq!(
            SwapStatus::from_trocador_status("paid partially"),
            SwapStatus::Sending
        );
    }

    #[test]
    fn enforces_core_transition_rules() {
        assert!(SwapStatus::Sending.can_transition(&SwapStatus::FundsReceived));
        assert!(SwapStatus::FundsReceived.can_transition(&SwapStatus::Completed));
        assert!(!SwapStatus::Completed.can_transition(&SwapStatus::FundsReceived));
    }

    #[test]
    fn does_not_downgrade_local_status_from_provider_update() {
        assert_eq!(
            SwapStatus::FundsReceived.reconcile_with_provider(SwapStatus::Sending),
            SwapStatus::FundsReceived
        );
        assert_eq!(
            SwapStatus::Completed.reconcile_with_provider(SwapStatus::Sending),
            SwapStatus::Completed
        );
    }
}
