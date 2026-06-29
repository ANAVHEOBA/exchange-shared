use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::modules::swap::crud::{CurrenciesResult, SwapCrud};
use crate::modules::swap::schema::{
    CreateSwapRequest, CurrenciesQuery, CurrencyResponse, RateResponse, RateType, RatesQuery,
    ValidateAddressRequest,
};
use crate::modules::whatsapp::crud::{SessionRecord, WhatsAppCrud};
use crate::services::whatsapp::{
    derive_whatsapp_client_id, InteractiveListOption, InteractiveListSection,
};
use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConversationState {
    Idle,
    AwaitingFromAssetSearch,
    AwaitingFromAssetChoice,
    AwaitingFromNetworkChoice,
    AwaitingToAssetSearch,
    AwaitingToAssetChoice,
    AwaitingToNetworkChoice,
    AwaitingAmount,
    AwaitingQuoteSelection,
    AwaitingRecipientAddress,
    AwaitingRecipientExtraId,
    AwaitingRefundAddress,
    AwaitingRefundExtraId,
    AwaitingConfirmation,
}

impl Default for ConversationState {
    fn default() -> Self {
        Self::Idle
    }
}

impl ConversationState {
    fn from_db(value: &str) -> Self {
        match value {
            "awaiting_from_asset" | "awaiting_from_asset_search" => Self::AwaitingFromAssetSearch,
            "awaiting_from_asset_choice" => Self::AwaitingFromAssetChoice,
            "awaiting_from_network_choice" => Self::AwaitingFromNetworkChoice,
            "awaiting_to_asset" | "awaiting_to_asset_search" => Self::AwaitingToAssetSearch,
            "awaiting_to_asset_choice" => Self::AwaitingToAssetChoice,
            "awaiting_to_network_choice" => Self::AwaitingToNetworkChoice,
            "awaiting_amount" => Self::AwaitingAmount,
            "awaiting_quote_selection" => Self::AwaitingQuoteSelection,
            "awaiting_recipient_address" => Self::AwaitingRecipientAddress,
            "awaiting_recipient_extra_id" => Self::AwaitingRecipientExtraId,
            "awaiting_refund_address" => Self::AwaitingRefundAddress,
            "awaiting_refund_extra_id" => Self::AwaitingRefundExtraId,
            "awaiting_confirmation" => Self::AwaitingConfirmation,
            _ => Self::Idle,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CurrencySelection {
    ticker: String,
    name: String,
    network: String,
    memo: bool,
    extra_id_name: Option<String>,
}

impl From<CurrencyResponse> for CurrencySelection {
    fn from(value: CurrencyResponse) -> Self {
        Self {
            ticker: value.ticker,
            name: value.name,
            network: value.network,
            memo: value.memo,
            extra_id_name: value.extra_id_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct AssetFamilySelection {
    ticker: String,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuoteChoice {
    index: usize,
    provider: String,
    provider_name: String,
    estimated_amount: f64,
    amount_to: f64,
    rate: f64,
    rate_type: RateType,
    min_amount: f64,
    max_amount: f64,
    kyc_required: bool,
    trade_id: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SwapDraft {
    from: Option<CurrencySelection>,
    to: Option<CurrencySelection>,
    pending_from_family: Option<AssetFamilySelection>,
    pending_to_family: Option<AssetFamilySelection>,
    amount: Option<f64>,
    quotes: Vec<QuoteChoice>,
    selected_quote: Option<QuoteChoice>,
    recipient_address: Option<String>,
    recipient_extra_id: Option<String>,
    refund_address: Option<String>,
    refund_extra_id: Option<String>,
}

#[derive(Debug)]
struct ParsedSwapIntent {
    amount: Option<f64>,
    from_phrase: Option<String>,
    to_phrase: Option<String>,
}

#[derive(Debug, Clone)]
struct AssetResolution {
    selected: Option<CurrencySelection>,
    ambiguous_options: Vec<CurrencySelection>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssetSide {
    From,
    To,
}

impl AssetSide {
    fn search_state(self) -> ConversationState {
        match self {
            Self::From => ConversationState::AwaitingFromAssetSearch,
            Self::To => ConversationState::AwaitingToAssetSearch,
        }
    }

    fn choice_state(self) -> ConversationState {
        match self {
            Self::From => ConversationState::AwaitingFromAssetChoice,
            Self::To => ConversationState::AwaitingToAssetChoice,
        }
    }

    fn network_state(self) -> ConversationState {
        match self {
            Self::From => ConversationState::AwaitingFromNetworkChoice,
            Self::To => ConversationState::AwaitingToNetworkChoice,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::From => "sending",
            Self::To => "receiving",
        }
    }

    fn asset_prompt(self) -> &'static str {
        match self {
            Self::From => {
                "What coin are you sending? Type the ticker or coin name. Examples: btc, xlm, usdc on stellar."
            }
            Self::To => {
                "What coin do you want to receive? Type the ticker or coin name. Examples: xmr, btc, usdt on tron."
            }
        }
    }

    fn network_prompt(self, family: &AssetFamilySelection) -> String {
        format!(
            "{} ({}) has multiple networks. Choose one.",
            family.name,
            family.ticker.to_uppercase(),
        )
    }

    fn family_prompt(self, count: usize) -> String {
        if count > 10 {
            format!(
                "I found {} matching {} assets. Showing the top 10. Choose the asset first or type a narrower search.",
                count,
                self.label()
            )
        } else {
            format!(
                "I found {} matching {} assets. Choose the asset first or type a narrower search.",
                count,
                self.label()
            )
        }
    }
}

#[derive(Debug, Clone)]
enum AssetInputPlan {
    Selected(CurrencySelection),
    ChooseResults {
        prompt: String,
        options: Vec<CurrencySelection>,
    },
    ChooseAsset(Vec<AssetFamilySelection>),
    ChooseNetwork {
        family: AssetFamilySelection,
        options: Vec<CurrencySelection>,
    },
    Error(String),
}

#[derive(Clone)]
struct CachedCurrencyCatalog {
    fetched_at: Instant,
    currencies: Vec<CurrencyResponse>,
}

fn currency_catalog_cache() -> &'static RwLock<Option<CachedCurrencyCatalog>> {
    static CACHE: OnceLock<RwLock<Option<CachedCurrencyCatalog>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

pub struct WhatsAppFlowService {
    state: Arc<AppState>,
}

impl WhatsAppFlowService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn process_message_event(
        &self,
        phone_number_id: &str,
        wa_id: &str,
        inbound_message_id: Option<&str>,
        text: &str,
    ) -> Result<(), String> {
        let crud = WhatsAppCrud::new(self.state.db.clone());
        let lock = crud
            .acquire_session_lock(wa_id, phone_number_id, 10)
            .await
            .map_err(|error| format!("failed to acquire WhatsApp session lock: {}", error))?;

        let result = self
            .process_message_event_locked(phone_number_id, wa_id, inbound_message_id, text)
            .await;

        if let Err(error) = lock.release().await {
            tracing::error!(
                "failed to release WhatsApp session lock for {} on {}: {}",
                wa_id,
                phone_number_id,
                error
            );
            if result.is_ok() {
                return Err(format!(
                    "failed to release WhatsApp session lock: {}",
                    error
                ));
            }
        }

        result
    }

    async fn process_message_event_locked(
        &self,
        phone_number_id: &str,
        wa_id: &str,
        inbound_message_id: Option<&str>,
        text: &str,
    ) -> Result<(), String> {
        let raw_trimmed = text.trim();
        if raw_trimmed.is_empty() {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    None,
                    "Type swap to start a new swap. Type status and then your swap ID to check progress.",
                )
                .await;
        }

        let normalized_command = normalize_quick_action_command(raw_trimmed);
        let trimmed = normalized_command.as_str();

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let session = crud
            .get_session(wa_id, phone_number_id)
            .await
            .map_err(|error| error.to_string())?;

        let (session_id, locale, mut draft, state) = match session {
            Some(record) => session_parts(record)?,
            None => (
                None,
                "en".to_string(),
                SwapDraft::default(),
                ConversationState::Idle,
            ),
        };

        let lowered = trimmed.to_ascii_lowercase();
        if lowered == "status_help" {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &Self::status_help_message(),
                )
                .await;
        }

        if matches!(lowered.as_str(), "cancel" | "restart" | "reset") {
            crud.upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::Idle,
                &locale,
                &SwapDraft::default(),
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    "Swap flow reset. Type swap to start again.",
                )
                .await;
        }

        if lowered == "examples" {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &Self::examples_message(),
                )
                .await;
        }

        if lowered == "help" || lowered == "menu" || lowered == "start" || is_greeting(&lowered) {
            return self
                .send_welcome_sequence(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    inbound_message_id,
                )
                .await;
        }

        if let Some(swap_id) = parse_status_command(trimmed) {
            return match self
                .send_status(wa_id, phone_number_id, session_id.as_deref(), &swap_id)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                        .await
                }
            };
        }

        match state {
            ConversationState::Idle => {
                if lowered == "swap" {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingFromAssetSearch,
                        &locale,
                        &SwapDraft::default(),
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            AssetSide::From.asset_prompt(),
                        )
                        .await;
                }

                if let Some(intent) = parse_swap_intent(trimmed) {
                    if let Some(amount) = intent.amount {
                        draft.amount = Some(amount);
                    }

                    let catalog = self.fetch_currency_catalog().await?;
                    let from_plan = match intent.from_phrase.as_deref() {
                        Some(value) => Some(self.resolve_asset_input(&catalog, value).await?),
                        None => None,
                    };
                    let to_plan = match intent.to_phrase.as_deref() {
                        Some(value) => Some(self.resolve_asset_input(&catalog, value).await?),
                        None => None,
                    };

                    if let Some(AssetInputPlan::Selected(selection)) = from_plan.as_ref() {
                        draft.from = Some(selection.clone());
                        draft.pending_from_family = None;
                    }

                    if let Some(AssetInputPlan::Selected(selection)) = to_plan.as_ref() {
                        draft.to = Some(selection.clone());
                        draft.pending_to_family = None;
                    }

                    if let Some(plan) = from_plan {
                        match plan {
                            AssetInputPlan::Selected(_) => {}
                            AssetInputPlan::ChooseResults { prompt, options } => {
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingFromAssetChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_currency_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &prompt,
                                        "Choose coin",
                                        &options,
                                    )
                                    .await;
                            }
                            AssetInputPlan::ChooseAsset(families) => {
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingFromAssetChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_asset_family_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &AssetSide::From.family_prompt(families.len()),
                                        &families,
                                    )
                                    .await;
                            }
                            AssetInputPlan::ChooseNetwork { family, options } => {
                                draft.pending_from_family = Some(family.clone());
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingFromNetworkChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_network_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &AssetSide::From.network_prompt(&family),
                                        &options,
                                    )
                                    .await;
                            }
                            AssetInputPlan::Error(message) => {
                                return self
                                    .reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                                    .await;
                            }
                        }
                    }

                    if let Some(plan) = to_plan {
                        match plan {
                            AssetInputPlan::Selected(_) => {}
                            AssetInputPlan::ChooseResults { prompt, options } => {
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingToAssetChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_currency_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &prompt,
                                        "Choose coin",
                                        &options,
                                    )
                                    .await;
                            }
                            AssetInputPlan::ChooseAsset(families) => {
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingToAssetChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_asset_family_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &AssetSide::To.family_prompt(families.len()),
                                        &families,
                                    )
                                    .await;
                            }
                            AssetInputPlan::ChooseNetwork { family, options } => {
                                draft.pending_to_family = Some(family.clone());
                                crud.upsert_session_state(
                                    wa_id,
                                    phone_number_id,
                                    &ConversationState::AwaitingToNetworkChoice,
                                    &locale,
                                    &draft,
                                    inbound_message_id,
                                )
                                .await
                                .map_err(|error| error.to_string())?;

                                return self
                                    .reply_network_options(
                                        wa_id,
                                        phone_number_id,
                                        session_id.as_deref(),
                                        &AssetSide::To.network_prompt(&family),
                                        &options,
                                    )
                                    .await;
                            }
                            AssetInputPlan::Error(message) => {
                                return self
                                    .reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                                    .await;
                            }
                        }
                    }

                    if draft.from.is_some() && draft.to.is_some() && draft.amount.is_some() {
                        return match self
                            .fetch_and_prompt_quotes(
                                wa_id,
                                phone_number_id,
                                session_id.as_deref(),
                                &locale,
                                draft,
                                inbound_message_id,
                            )
                            .await
                        {
                            Ok(()) => Ok(()),
                            Err(error) => {
                                self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                                    .await
                            }
                        };
                    }
                }

                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &Self::help_message(),
                )
                .await
            }
            ConversationState::AwaitingFromAssetSearch => {
                self.handle_asset_search_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::From,
                )
                .await
            }
            ConversationState::AwaitingFromAssetChoice => {
                self.handle_asset_choice_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::From,
                )
                .await
            }
            ConversationState::AwaitingFromNetworkChoice => {
                self.handle_network_choice_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::From,
                )
                .await
            }
            ConversationState::AwaitingToAssetSearch => {
                self.handle_asset_search_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::To,
                )
                .await
            }
            ConversationState::AwaitingToAssetChoice => {
                self.handle_asset_choice_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::To,
                )
                .await
            }
            ConversationState::AwaitingToNetworkChoice => {
                self.handle_network_choice_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                    AssetSide::To,
                )
                .await
            }
            ConversationState::AwaitingAmount => {
                let Some(amount) = parse_amount(trimmed) else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Amount not recognized. Reply with a number like 100 or 0.25.",
                        )
                        .await;
                };
                draft.amount = Some(amount);

                match self
                    .fetch_and_prompt_quotes(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        &locale,
                        draft,
                        inbound_message_id,
                    )
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                            .await
                    }
                }
            }
            ConversationState::AwaitingQuoteSelection => {
                let Some(choice_index) = parse_quote_selection(trimmed) else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Reply with the quote number you want, for example 1.",
                        )
                        .await;
                };

                let Some(selected_quote) = draft
                    .quotes
                    .iter()
                    .find(|quote| quote.index == choice_index)
                    .cloned()
                else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &format!(
                                "Quote {} is not available anymore. Reply with one of the listed numbers.",
                                choice_index
                            ),
                        )
                        .await;
                };

                draft.selected_quote = Some(selected_quote.clone());
                crud.upsert_session_state(
                    wa_id,
                    phone_number_id,
                    &ConversationState::AwaitingRecipientAddress,
                    &locale,
                    &draft,
                    inbound_message_id,
                )
                .await
                .map_err(|error| error.to_string())?;

                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &format!(
                        "Selected {}. Send the {} {} destination address.",
                        selected_quote.provider_name,
                        draft
                            .to
                            .as_ref()
                            .map(|asset| asset.name.as_str())
                            .unwrap_or("destination"),
                        draft
                            .to
                            .as_ref()
                            .map(|asset| format!("({})", asset.network))
                            .unwrap_or_default()
                    ),
                )
                .await
            }
            ConversationState::AwaitingRecipientAddress => {
                let Some(target) = draft.to.as_ref() else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Destination asset missing. Type swap to restart.",
                        )
                        .await;
                };
                if let Err(error) = self
                    .validate_address(&target.ticker, &target.network, trimmed)
                    .await
                {
                    return self
                        .reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                        .await;
                }
                draft.recipient_address = Some(trimmed.to_string());

                if target.memo {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRecipientExtraId,
                        &locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        &format!(
                            "This destination also needs {}. Reply with it now.",
                            target
                                .extra_id_name
                                .clone()
                                .unwrap_or_else(|| "the extra ID".to_string())
                        ),
                    )
                    .await
                } else {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRefundAddress,
                        &locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        "Optional but recommended: send a refund address for the asset you are sending, or reply skip.",
                    )
                    .await
                }
            }
            ConversationState::AwaitingRecipientExtraId => {
                if trimmed.is_empty() {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "The extra ID cannot be empty.",
                        )
                        .await;
                }

                draft.recipient_extra_id = Some(trimmed.to_string());
                crud.upsert_session_state(
                    wa_id,
                    phone_number_id,
                    &ConversationState::AwaitingRefundAddress,
                    &locale,
                    &draft,
                    inbound_message_id,
                )
                .await
                .map_err(|error| error.to_string())?;

                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    "Optional but recommended: send a refund address for the asset you are sending, or reply skip.",
                )
                .await
            }
            ConversationState::AwaitingRefundAddress => {
                if lowered == "skip" {
                    draft.refund_address = None;
                    draft.refund_extra_id = None;
                    return match self
                        .prompt_confirmation(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &locale,
                            draft,
                            inbound_message_id,
                        )
                        .await
                    {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                                .await
                        }
                    };
                }

                let Some(source) = draft.from.as_ref() else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Source asset missing. Type swap to restart.",
                        )
                        .await;
                };
                if let Err(error) = self
                    .validate_address(&source.ticker, &source.network, trimmed)
                    .await
                {
                    return self
                        .reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                        .await;
                }
                draft.refund_address = Some(trimmed.to_string());

                if source.memo {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRefundExtraId,
                        &locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        &format!(
                            "Your refund address also needs {}. Reply with it now.",
                            source
                                .extra_id_name
                                .clone()
                                .unwrap_or_else(|| "the extra ID".to_string())
                        ),
                    )
                    .await
                } else {
                    match self
                        .prompt_confirmation(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &locale,
                            draft,
                            inbound_message_id,
                        )
                        .await
                    {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                                .await
                        }
                    }
                }
            }
            ConversationState::AwaitingRefundExtraId => {
                if trimmed.is_empty() {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "The refund extra ID cannot be empty.",
                        )
                        .await;
                }

                draft.refund_extra_id = Some(trimmed.to_string());
                match self
                    .prompt_confirmation(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        &locale,
                        draft,
                        inbound_message_id,
                    )
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        self.reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                            .await
                    }
                }
            }
            ConversationState::AwaitingConfirmation => {
                if !matches!(lowered.as_str(), "confirm" | "yes" | "y") {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Reply confirm to create the swap, or cancel to reset.",
                        )
                        .await;
                }

                let response = match self
                    .create_swap_from_draft(phone_number_id, wa_id, &draft)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        return self
                            .reply(wa_id, phone_number_id, session_id.as_deref(), &error)
                            .await;
                    }
                };
                crud.upsert_session_state(
                    wa_id,
                    phone_number_id,
                    &ConversationState::Idle,
                    &locale,
                    &SwapDraft::default(),
                    inbound_message_id,
                )
                .await
                .map_err(|error| error.to_string())?;

                let summary = [
                    "Swap created successfully.".to_string(),
                    format!(
                        "Send exactly {} {} on {}",
                        trim_f64(response.deposit_amount),
                        response.from.to_uppercase(),
                        response.network_from
                    ),
                    format!(
                        "Expected receive: {} {}",
                        trim_f64(response.estimated_receive),
                        response.to.to_uppercase()
                    ),
                    format!("Provider: {}", response.provider),
                    "To check progress later, type status and then paste the swap ID.".to_string(),
                ]
                .join("\n");

                self.reply(wa_id, phone_number_id, session_id.as_deref(), &summary)
                    .await?;
                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &format!("Swap ID\n{}", response.swap_id),
                )
                .await?;
                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &format!("Deposit address\n{}", response.deposit_address),
                )
                .await?;

                if let Some(extra_id) = response.deposit_extra_id.as_ref() {
                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        &format!("Deposit extra ID\n{}", extra_id),
                    )
                    .await?;
                }

                Ok(())
            }
        }
    }

    async fn handle_asset_search_input(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        input: &str,
        side: AssetSide,
    ) -> Result<(), String> {
        if normalize_quick_action_command(input).eq_ignore_ascii_case("swap") {
            return self
                .reply(wa_id, phone_number_id, session_id, side.asset_prompt())
                .await;
        }

        let catalog = self.fetch_currency_catalog().await?;

        if let Some(selection) = parse_asset_selection_id(&catalog, input) {
            return self
                .continue_after_asset_selected(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    selection,
                )
                .await;
        }

        if let Some(family_key) = parse_family_selection_id(input) {
            return self
                .handle_asset_family_choice(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    family_key,
                    &catalog,
                )
                .await;
        }

        match self.resolve_asset_input(&catalog, input).await? {
            AssetInputPlan::Selected(selection) => {
                self.continue_after_asset_selected(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    selection,
                )
                .await
            }
            AssetInputPlan::ChooseResults { prompt, options } => {
                set_pending_family(&mut draft, side, None);
                WhatsAppCrud::new(self.state.db.clone())
                    .upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &side.choice_state(),
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                self.reply_currency_options(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &prompt,
                    "Choose coin",
                    &options,
                )
                .await
            }
            AssetInputPlan::ChooseAsset(families) => {
                let total_matches = families.len();
                let prompt = if total_matches > 10 {
                    format!(
                        "I found {} matching {} assets. Showing the top 10. Choose the asset first or type a narrower search.",
                        total_matches,
                        side.label()
                    )
                } else {
                    side.family_prompt(total_matches)
                };

                set_pending_family(&mut draft, side, None);
                WhatsAppCrud::new(self.state.db.clone())
                    .upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &side.choice_state(),
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                self.reply_asset_family_options(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &prompt,
                    &families,
                )
                .await
            }
            AssetInputPlan::ChooseNetwork { family, options } => {
                self.persist_network_choice_state(
                    wa_id,
                    phone_number_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    family.clone(),
                )
                .await?;

                self.reply_network_options(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &side.network_prompt(&family),
                    &options,
                )
                .await
            }
            AssetInputPlan::Error(message) => {
                self.reply(wa_id, phone_number_id, session_id, &message)
                    .await
            }
        }
    }

    async fn handle_asset_choice_input(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        draft: SwapDraft,
        inbound_message_id: Option<&str>,
        input: &str,
        side: AssetSide,
    ) -> Result<(), String> {
        let catalog = self.fetch_currency_catalog().await?;

        if let Some(family_key) = parse_family_selection_id(input) {
            return self
                .handle_asset_family_choice(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    family_key,
                    &catalog,
                )
                .await;
        }

        self.handle_asset_search_input(
            wa_id,
            phone_number_id,
            session_id,
            locale,
            draft,
            inbound_message_id,
            input,
            side,
        )
        .await
    }

    async fn handle_network_choice_input(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        draft: SwapDraft,
        inbound_message_id: Option<&str>,
        input: &str,
        side: AssetSide,
    ) -> Result<(), String> {
        let catalog = self.fetch_currency_catalog().await?;

        if let Some(selection) = parse_asset_selection_id(&catalog, input) {
            return self
                .continue_after_asset_selected(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    selection,
                )
                .await;
        }

        let Some(family) = pending_family(&draft, side).cloned() else {
            return self
                .handle_asset_search_input(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    input,
                    side,
                )
                .await;
        };

        if should_restart_asset_search_from_network_choice(&family, input) {
            let mut next_draft = draft.clone();
            set_pending_family(&mut next_draft, side, None);

            return self
                .handle_asset_search_input(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    next_draft,
                    inbound_message_id,
                    input,
                    side,
                )
                .await;
        }

        let family_catalog = find_family_currencies(&catalog, &family);
        if family_catalog.is_empty() {
            let mut next_draft = draft.clone();
            set_pending_family(&mut next_draft, side, None);
            WhatsAppCrud::new(self.state.db.clone())
                .upsert_session_state(
                    wa_id,
                    phone_number_id,
                    &side.search_state(),
                    locale,
                    &next_draft,
                    inbound_message_id,
                )
                .await
                .map_err(|error| error.to_string())?;

            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    "That asset family is no longer available. Start the search again.",
                )
                .await;
        }

        match resolve_currency_phrase(&family_catalog, input)? {
            AssetResolution {
                selected: Some(selection),
                ..
            } => {
                self.continue_after_asset_selected(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    selection,
                )
                .await
            }
            AssetResolution {
                ambiguous_options, ..
            } if !ambiguous_options.is_empty() => {
                let network_options = ambiguous_options;
                self.persist_network_choice_state(
                    wa_id,
                    phone_number_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    family.clone(),
                )
                .await?;

                self.reply_network_options(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &side.network_prompt(&family),
                    &network_options,
                )
                .await
            }
            resolution => {
                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    resolution.error.as_deref().unwrap_or(
                        "I could not match that network. Tap one of the listed networks or type it more clearly.",
                    ),
                )
                .await
            }
        }
    }

    async fn handle_asset_family_choice(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        draft: SwapDraft,
        inbound_message_id: Option<&str>,
        side: AssetSide,
        family_key: AssetFamilyKey,
        catalog: &[CurrencyResponse],
    ) -> Result<(), String> {
        let Some(family) = find_family_selection(catalog, &family_key) else {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    "That asset choice is no longer available. Search again.",
                )
                .await;
        };

        let family_catalog = find_family_currencies(catalog, &family);
        if family_catalog.is_empty() {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    "That asset choice is no longer available. Search again.",
                )
                .await;
        }

        if family_catalog.len() == 1 {
            return self
                .continue_after_asset_selected(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                    side,
                    CurrencySelection::from(family_catalog[0].clone()),
                )
                .await;
        }

        let options = family_catalog
            .into_iter()
            .map(CurrencySelection::from)
            .collect::<Vec<_>>();

        self.persist_network_choice_state(
            wa_id,
            phone_number_id,
            locale,
            draft,
            inbound_message_id,
            side,
            family.clone(),
        )
        .await?;

        self.reply_network_options(
            wa_id,
            phone_number_id,
            session_id,
            &side.network_prompt(&family),
            &options,
        )
        .await
    }

    async fn persist_network_choice_state(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        side: AssetSide,
        family: AssetFamilySelection,
    ) -> Result<(), String> {
        set_pending_family(&mut draft, side, Some(family));
        WhatsAppCrud::new(self.state.db.clone())
            .upsert_session_state(
                wa_id,
                phone_number_id,
                &side.network_state(),
                locale,
                &draft,
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    async fn continue_after_asset_selected(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        side: AssetSide,
        selection: CurrencySelection,
    ) -> Result<(), String> {
        match side {
            AssetSide::From => draft.from = Some(selection),
            AssetSide::To => draft.to = Some(selection),
        }
        set_pending_family(&mut draft, side, None);

        match side {
            AssetSide::From => {
                if draft.to.is_some() && draft.amount.is_some() {
                    self.fetch_and_prompt_quotes(
                        wa_id,
                        phone_number_id,
                        session_id,
                        locale,
                        draft,
                        inbound_message_id,
                    )
                    .await
                } else if draft.to.is_some() {
                    WhatsAppCrud::new(self.state.db.clone())
                        .upsert_session_state(
                            wa_id,
                            phone_number_id,
                            &ConversationState::AwaitingAmount,
                            locale,
                            &draft,
                            inbound_message_id,
                        )
                        .await
                        .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id,
                        "How much do you want to send? Reply with only the amount, for example 100 or 0.25.",
                    )
                    .await
                } else {
                    WhatsAppCrud::new(self.state.db.clone())
                        .upsert_session_state(
                            wa_id,
                            phone_number_id,
                            &ConversationState::AwaitingToAssetSearch,
                            locale,
                            &draft,
                            inbound_message_id,
                        )
                        .await
                        .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id,
                        AssetSide::To.asset_prompt(),
                    )
                    .await
                }
            }
            AssetSide::To => {
                if draft.amount.is_some() {
                    self.fetch_and_prompt_quotes(
                        wa_id,
                        phone_number_id,
                        session_id,
                        locale,
                        draft,
                        inbound_message_id,
                    )
                    .await
                } else {
                    WhatsAppCrud::new(self.state.db.clone())
                        .upsert_session_state(
                            wa_id,
                            phone_number_id,
                            &ConversationState::AwaitingAmount,
                            locale,
                            &draft,
                            inbound_message_id,
                        )
                        .await
                        .map_err(|error| error.to_string())?;

                    self.reply(
                        wa_id,
                        phone_number_id,
                        session_id,
                        "How much do you want to send? Reply with only the amount, for example 100 or 0.25.",
                    )
                    .await
                }
            }
        }
    }

    async fn fetch_and_prompt_quotes(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
    ) -> Result<(), String> {
        let from = draft
            .from
            .as_ref()
            .ok_or_else(|| "Source asset missing. Type swap to restart.".to_string())?;
        let to = draft
            .to
            .as_ref()
            .ok_or_else(|| "Destination asset missing. Type swap to restart.".to_string())?;
        let amount = draft
            .amount
            .ok_or_else(|| "Amount missing. Type swap to restart.".to_string())?;

        let swap_crud = self.swap_crud();
        let rates = match swap_crud
            .get_rates_optimized(&RatesQuery {
                from: from.ticker.clone(),
                network_from: from.network.clone(),
                to: to.ticker.clone(),
                network_to: to.network.clone(),
                amount,
                rate_type: None,
                provider: None,
                min_kycrating: None,
            })
            .await
        {
            Ok(rates) => rates,
            Err(error) => {
                return Err(format!(
                    "Failed to fetch live routes for {} on {} -> {} on {}: {}",
                    from.ticker.to_uppercase(),
                    from.network,
                    to.ticker.to_uppercase(),
                    to.network,
                    error
                ));
            }
        };

        if rates.rates.is_empty() {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    "No live routes are available for that pair right now. Try another amount or network.",
                )
                .await;
        }

        draft.quotes = rates
            .rates
            .iter()
            .take(5)
            .enumerate()
            .map(|(index, rate)| to_quote_choice(index + 1, rate, &rates.trade_id))
            .collect::<Vec<_>>();
        draft.selected_quote = None;

        WhatsAppCrud::new(self.state.db.clone())
            .upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::AwaitingQuoteSelection,
                locale,
                &draft,
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

        let mut lines = vec![format!(
            "Found {} live routes for {} {} on {} -> {} on {}.",
            rates.rates.len(),
            trim_f64(amount),
            from.ticker.to_uppercase(),
            from.network,
            to.ticker.to_uppercase(),
            to.network
        )];
        lines.push("Reply with the route number you want:".to_string());

        for quote in &draft.quotes {
            lines.push(format!(
                "{}. {} | receive {} {} | {} | {}",
                quote.index,
                quote.provider_name,
                trim_f64(quote.estimated_amount),
                to.ticker.to_uppercase(),
                quote.rate_type.as_db_str(),
                if quote.kyc_required {
                    "KYC"
                } else {
                    "No KYC flag"
                }
            ));
        }

        self.reply(wa_id, phone_number_id, session_id, &lines.join("\n"))
            .await
    }

    async fn prompt_confirmation(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        draft: SwapDraft,
        inbound_message_id: Option<&str>,
    ) -> Result<(), String> {
        WhatsAppCrud::new(self.state.db.clone())
            .upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::AwaitingConfirmation,
                locale,
                &draft,
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

        let from = draft
            .from
            .as_ref()
            .ok_or_else(|| "Source asset missing.".to_string())?;
        let to = draft
            .to
            .as_ref()
            .ok_or_else(|| "Destination asset missing.".to_string())?;
        let quote = draft
            .selected_quote
            .as_ref()
            .ok_or_else(|| "Quote missing.".to_string())?;
        let recipient_address = draft
            .recipient_address
            .as_ref()
            .ok_or_else(|| "Recipient address missing.".to_string())?;

        let mut lines = vec![
            "Review the swap:".to_string(),
            format!(
                "Send: {} {} on {}",
                trim_f64(draft.amount.unwrap_or_default()),
                from.ticker.to_uppercase(),
                from.network
            ),
            format!(
                "Receive: about {} {} on {}",
                trim_f64(quote.estimated_amount),
                to.ticker.to_uppercase(),
                to.network
            ),
            format!("Provider: {}", quote.provider_name),
            format!("Recipient address: {}", recipient_address),
        ];

        if let Some(extra_id) = draft.recipient_extra_id.as_ref() {
            lines.push(format!(
                "{}: {}",
                to.extra_id_name
                    .clone()
                    .unwrap_or_else(|| "Recipient extra ID".to_string()),
                extra_id
            ));
        }

        if let Some(refund_address) = draft.refund_address.as_ref() {
            lines.push(format!("Refund address: {}", refund_address));
        }

        if let Some(refund_extra_id) = draft.refund_extra_id.as_ref() {
            lines.push(format!(
                "{}: {}",
                from.extra_id_name
                    .clone()
                    .unwrap_or_else(|| "Refund extra ID".to_string()),
                refund_extra_id
            ));
        }

        lines.push("Reply confirm to create the swap, or cancel to abort.".to_string());

        self.reply(wa_id, phone_number_id, session_id, &lines.join("\n"))
            .await
    }

    async fn create_swap_from_draft(
        &self,
        phone_number_id: &str,
        wa_id: &str,
        draft: &SwapDraft,
    ) -> Result<crate::modules::swap::schema::CreateSwapResponse, String> {
        let from = draft
            .from
            .as_ref()
            .ok_or_else(|| "Source asset missing.".to_string())?;
        let to = draft
            .to
            .as_ref()
            .ok_or_else(|| "Destination asset missing.".to_string())?;
        let quote = draft
            .selected_quote
            .as_ref()
            .ok_or_else(|| "Quote missing.".to_string())?;
        let recipient_address = draft
            .recipient_address
            .as_ref()
            .ok_or_else(|| "Recipient address missing.".to_string())?;

        self.swap_crud()
            .create_swap(
                &CreateSwapRequest {
                    trade_id: Some(quote.trade_id.clone()),
                    from: from.ticker.clone(),
                    network_from: from.network.clone(),
                    to: to.ticker.clone(),
                    network_to: to.network.clone(),
                    amount: draft.amount.unwrap_or_default(),
                    provider: quote.provider.clone(),
                    recipient_address: recipient_address.clone(),
                    recipient_extra_id: draft.recipient_extra_id.clone(),
                    refund_address: draft.refund_address.clone(),
                    refund_extra_id: draft.refund_extra_id.clone(),
                    rate_type: quote.rate_type.clone(),
                    sandbox: false,
                    payment: false,
                    min_kycrating: None,
                },
                None,
                Some(derive_whatsapp_client_id(phone_number_id, wa_id)),
            )
            .await
            .map_err(|error| error.to_string())
    }

    async fn send_status(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        swap_id: &str,
    ) -> Result<(), String> {
        let client_id = derive_whatsapp_client_id(phone_number_id, wa_id);
        let status = self
            .swap_crud()
            .get_swap_status_for_client(swap_id, &client_id)
            .await
            .map_err(|error| format!("I could not find swap \"{}\": {}", swap_id, error))?;

        let mut lines = vec![
            format!("Swap {}", status.swap_id),
            format!("Status: {:?}", status.status),
            format!(
                "Send: {} {}",
                trim_f64(status.amount),
                status.from.to_uppercase()
            ),
            format!(
                "Receive estimate: {} {}",
                trim_f64(status.estimated_receive),
                status.to.to_uppercase()
            ),
            format!("Deposit address: {}", status.deposit_address),
            format!("Provider: {}", status.provider),
        ];

        if let Some(tx_hash_in) = status.tx_hash_in.as_ref() {
            lines.push(format!("Deposit tx: {}", tx_hash_in));
        }
        if let Some(tx_hash_out) = status.tx_hash_out.as_ref() {
            lines.push(format!("Payout tx: {}", tx_hash_out));
        }
        if let Some(error) = status.error.as_ref() {
            lines.push(format!("Error: {}", error));
        }

        self.reply(wa_id, phone_number_id, session_id, &lines.join("\n"))
            .await
    }

    async fn validate_address(
        &self,
        ticker: &str,
        network: &str,
        address: &str,
    ) -> Result<(), String> {
        let response = self
            .swap_crud()
            .validate_address(&ValidateAddressRequest {
                ticker: ticker.to_string(),
                network: network.to_string(),
                address: address.to_string(),
            })
            .await
            .map_err(|error| error.to_string())?;

        if response.valid {
            Ok(())
        } else {
            Err(format!(
                "That address does not look valid for {} on {}. Send another one.",
                ticker.to_uppercase(),
                network
            ))
        }
    }

    async fn fetch_currency_catalog(&self) -> Result<Vec<CurrencyResponse>, String> {
        if let Ok(cache) = currency_catalog_cache().read() {
            if let Some(cached) = cache.as_ref() {
                if cached.fetched_at.elapsed() <= Duration::from_secs(300) {
                    return Ok(cached.currencies.clone());
                }
            }
        }

        let currencies = match self
            .swap_crud()
            .get_currencies_optimized(CurrenciesQuery::default())
            .await
            .map_err(|error| error.to_string())?
        {
            CurrenciesResult::RawJson(json) => serde_json::from_str(&json).map_err(|error| {
                format!(
                    "failed to parse currency catalog from cache/upstream: {}",
                    error
                )
            }),
            CurrenciesResult::Structured(currencies) => Ok(currencies),
        }?;

        if let Ok(mut cache) = currency_catalog_cache().write() {
            *cache = Some(CachedCurrencyCatalog {
                fetched_at: Instant::now(),
                currencies: currencies.clone(),
            });
        }

        Ok(currencies)
    }

    async fn search_currency_matches(
        &self,
        catalog: &[CurrencyResponse],
        phrase: &str,
        limit: usize,
    ) -> Result<Vec<CurrencyResponse>, String> {
        let network_aliases = build_network_aliases(catalog);
        let (search_term, network_filter) = if let Some((asset_phrase, explicit_network)) =
            split_explicit_network_phrase(phrase, &network_aliases)
        {
            (asset_phrase, Some(explicit_network))
        } else {
            (phrase.trim().to_string(), None)
        };

        let query = CurrenciesQuery {
            network: network_filter.clone(),
            search: Some(search_term.clone()),
            page: Some(1),
            limit: Some(limit),
            ..Default::default()
        };

        let mut matches = match self
            .swap_crud()
            .get_currencies_optimized(query)
            .await
            .map_err(|error| error.to_string())?
        {
            CurrenciesResult::RawJson(json) => serde_json::from_str(&json)
                .map_err(|error| format!("failed to parse currency search results: {}", error))?,
            CurrenciesResult::Structured(currencies) => currencies,
        };

        if matches.is_empty() {
            let scoped_catalog = match network_filter.as_deref() {
                Some(network) => catalog
                    .iter()
                    .filter(|currency| currency.network.eq_ignore_ascii_case(network))
                    .cloned()
                    .collect::<Vec<_>>(),
                None => catalog.to_vec(),
            };

            if let Some(selected) =
                fuzzy_match_currency(&scoped_catalog, &normalize_phrase(&search_term), true)
            {
                matches.push(selected);
            }
        }

        Ok(matches)
    }

    async fn resolve_asset_input(
        &self,
        catalog: &[CurrencyResponse],
        input: &str,
    ) -> Result<AssetInputPlan, String> {
        let normalized_input = normalize_phrase(input);
        if normalized_input.is_empty() {
            return Ok(AssetInputPlan::Error(
                "Type a coin ticker or name.".to_string(),
            ));
        }

        let network_aliases = build_network_aliases(catalog);
        if split_explicit_network_phrase(input, &network_aliases).is_some() {
            return plan_asset_input(catalog, input);
        }

        let ranked_matches = self.search_currency_matches(catalog, input, 250).await?;
        if ranked_matches.is_empty() {
            return plan_asset_input(catalog, input);
        }

        let ranked_selections = ranked_matches
            .into_iter()
            .map(CurrencySelection::from)
            .collect::<Vec<_>>();
        if ranked_selections.len() == 1 {
            return Ok(AssetInputPlan::Selected(ranked_selections[0].clone()));
        }

        let exact_matches = ranked_selections
            .iter()
            .filter(|selection| {
                normalize_phrase(&selection.ticker) == normalized_input
                    || normalize_phrase(&selection.name) == normalized_input
            })
            .count();

        let prompt = if exact_matches > 1 {
            format!("\"{}\" matches multiple options. Choose one.", input.trim())
        } else if ranked_selections.len() > 10 {
            format!(
                "Top matches for \"{}\". Showing first 10. Choose one or narrow the search.",
                input.trim()
            )
        } else {
            format!("Top matches for \"{}\". Choose one.", input.trim())
        };

        Ok(AssetInputPlan::ChooseResults {
            prompt,
            options: ranked_selections.into_iter().take(10).collect(),
        })
    }

    fn swap_crud(&self) -> SwapCrud {
        SwapCrud::new(
            self.state.db.clone(),
            self.state.redis.clone(),
            self.state.wallet_mnemonic.clone(),
            self.state.rpc_manager.clone(),
            self.state.payout_policy.clone(),
        )
    }

    async fn reply(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
    ) -> Result<(), String> {
        let service = self
            .state
            .whatsapp_service
            .as_ref()
            .ok_or_else(|| "WhatsApp is not configured".to_string())?;

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let outbound_id = crud
            .record_outbound_message(session_id, wa_id, phone_number_id, "text", body)
            .await
            .map_err(|error| error.to_string())?;

        match service.send_text_message(wa_id, body).await {
            Ok(response) => {
                let provider_message_id =
                    response.messages.first().map(|message| message.id.as_str());
                crud.mark_outbound_sent(&outbound_id, provider_message_id)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(())
            }
            Err(error) => {
                let _ = crud
                    .mark_outbound_failed(&outbound_id, &error.to_string())
                    .await;
                Err(error.to_string())
            }
        }
    }

    async fn reply_image(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        image_link: &str,
        caption: Option<&str>,
    ) -> Result<(), String> {
        let service = self
            .state
            .whatsapp_service
            .as_ref()
            .ok_or_else(|| "WhatsApp is not configured".to_string())?;

        let body_for_audit = caption.unwrap_or(image_link);
        let crud = WhatsAppCrud::new(self.state.db.clone());
        let outbound_id = crud
            .record_outbound_message(session_id, wa_id, phone_number_id, "image", body_for_audit)
            .await
            .map_err(|error| error.to_string())?;

        match service.send_image_message(wa_id, image_link, caption).await {
            Ok(response) => {
                let provider_message_id =
                    response.messages.first().map(|message| message.id.as_str());
                crud.mark_outbound_sent(&outbound_id, provider_message_id)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(())
            }
            Err(error) => {
                let _ = crud
                    .mark_outbound_failed(&outbound_id, &error.to_string())
                    .await;
                Err(error.to_string())
            }
        }
    }

    async fn reply_asset_family_options(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        families: &[AssetFamilySelection],
    ) -> Result<(), String> {
        let rows = families
            .iter()
            .map(|family| InteractiveListOption {
                id: build_family_selection_id(family),
                title: truncate_whatsapp_text(&family.ticker.to_ascii_uppercase(), 24),
                description: Some(truncate_whatsapp_text(&family.name, 72)),
            })
            .collect::<Vec<_>>();

        self.reply_interactive_list(
            wa_id,
            phone_number_id,
            session_id,
            body,
            "Choose asset",
            rows,
        )
        .await
    }

    async fn reply_network_options(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        options: &[CurrencySelection],
    ) -> Result<(), String> {
        let rows = options
            .iter()
            .map(|option| InteractiveListOption {
                id: build_asset_selection_id(option),
                title: truncate_whatsapp_text(&option.network, 24),
                description: Some(truncate_whatsapp_text(
                    &format!("{} ({})", option.name, option.ticker.to_ascii_uppercase()),
                    72,
                )),
            })
            .collect::<Vec<_>>();

        self.reply_interactive_list(wa_id, phone_number_id, session_id, body, "Networks", rows)
            .await
    }

    async fn reply_currency_options(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        button_label: &str,
        options: &[CurrencySelection],
    ) -> Result<(), String> {
        let rows = options
            .iter()
            .map(|option| InteractiveListOption {
                id: build_asset_selection_id(option),
                title: truncate_whatsapp_text(
                    &format!(
                        "{} • {}",
                        option.ticker.to_ascii_uppercase(),
                        option.network
                    ),
                    24,
                ),
                description: Some(truncate_whatsapp_text(&option.name, 72)),
            })
            .collect::<Vec<_>>();

        self.reply_interactive_list(wa_id, phone_number_id, session_id, body, button_label, rows)
            .await
    }

    async fn reply_interactive_list(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        button_label: &str,
        rows: Vec<InteractiveListOption>,
    ) -> Result<(), String> {
        let service = self
            .state
            .whatsapp_service
            .as_ref()
            .ok_or_else(|| "WhatsApp is not configured".to_string())?;

        if rows.is_empty() {
            return Err("no WhatsApp list options available".to_string());
        }

        let sections = vec![InteractiveListSection {
            title: None,
            rows: rows.into_iter().take(10).collect::<Vec<_>>(),
        }];

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let outbound_id = crud
            .record_outbound_message(session_id, wa_id, phone_number_id, "interactive_list", body)
            .await
            .map_err(|error| error.to_string())?;

        match service
            .send_interactive_list_message(wa_id, body, button_label, sections)
            .await
        {
            Ok(response) => {
                let provider_message_id =
                    response.messages.first().map(|message| message.id.as_str());
                crud.mark_outbound_sent(&outbound_id, provider_message_id)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(())
            }
            Err(error) => {
                let _ = crud
                    .mark_outbound_failed(&outbound_id, &error.to_string())
                    .await;
                Err(error.to_string())
            }
        }
    }

    async fn send_welcome_sequence(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        inbound_message_id: Option<&str>,
    ) -> Result<(), String> {
        WhatsAppCrud::new(self.state.db.clone())
            .upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::Idle,
                locale,
                &SwapDraft::default(),
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

        let welcome_copy = Self::welcome_intro_message();
        if let Some(image_link) = self.branding_logo_url() {
            if let Err(error) = self
                .reply_image(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &image_link,
                    Some(&welcome_copy),
                )
                .await
            {
                tracing::warn!(
                    "failed to send WhatsApp welcome image with caption, retrying as text only: {}",
                    error
                );
                return self
                    .reply(wa_id, phone_number_id, session_id, &welcome_copy)
                    .await;
            }

            return Ok(());
        }

        self.reply(wa_id, phone_number_id, session_id, &welcome_copy)
            .await
    }

    fn branding_logo_url(&self) -> Option<String> {
        self.state
            .whatsapp_service
            .as_ref()
            .and_then(|service| service.config().public_base_url.as_ref())
            .map(|base| format!("{}/branding/assetar-logo.png", base.trim_end_matches('/')))
    }

    fn help_message() -> String {
        [
            "Assetar menu:",
            "- Type swap to start a guided swap",
            "- Type status and then your swap ID to check a swap",
            "- Type cancel to reset the current flow",
            "- Type help to see this menu again",
        ]
        .join("\n")
    }

    fn welcome_intro_message() -> String {
        [
            "Hi 👋 Welcome to Assetar.",
            "Assetar helps you compare live crypto swap routes and complete swaps easily in chat.",
            "Type swap to begin.",
            "Type status and then your swap ID to check progress later.",
            "Type cancel any time to reset the current flow.",
        ]
        .join("\n\n")
    }

    fn examples_message() -> String {
        [
            "Swap examples:",
            "- swap",
            "- swap 100 usdc on stellar to bitcoin",
            "- swap 0.5 btc to xmr",
        ]
        .join("\n")
    }

    fn status_help_message() -> String {
        "Type status and then your swap ID to check a swap. Example: status 4fd0c0e1-1234-5678-9abc-1234567890ab."
            .to_string()
    }
}

fn session_parts(
    record: SessionRecord,
) -> Result<(Option<String>, String, SwapDraft, ConversationState), String> {
    let draft = match record.draft_json {
        Some(json) if !json.trim().is_empty() && json.trim() != "null" => {
            serde_json::from_str::<SwapDraft>(&json)
                .map_err(|error| format!("failed to restore WhatsApp session draft: {}", error))?
        }
        _ => SwapDraft::default(),
    };

    Ok((
        Some(record.id),
        record.locale,
        draft,
        ConversationState::from_db(&record.state),
    ))
}

fn to_quote_choice(index: usize, rate: &RateResponse, trade_id: &str) -> QuoteChoice {
    QuoteChoice {
        index,
        provider: rate.provider.clone(),
        provider_name: rate.provider_name.clone(),
        estimated_amount: rate.estimated_amount,
        amount_to: rate.amount_to,
        rate: rate.rate,
        rate_type: rate.rate_type.clone(),
        min_amount: rate.min_amount,
        max_amount: rate.max_amount,
        kyc_required: rate.kyc_required,
        trade_id: trade_id.to_string(),
    }
}

fn normalize_quick_action_command(input: &str) -> String {
    let trimmed = input.trim();
    let lowered = trimmed.to_ascii_lowercase();

    match lowered.as_str() {
        "cta:start_swap" => "swap".to_string(),
        "cta:examples" => "examples".to_string(),
        "cta:status_help" => "status_help".to_string(),
        "/swap" | "start swap" | "i want to swap crypto" | "i want to make a swap" => {
            "swap".to_string()
        }
        "/help" | "/menu" => "help".to_string(),
        "/examples" | "swap examples" | "show examples" => "examples".to_string(),
        "/status" | "status" => "status_help".to_string(),
        "/cancel" | "/restart" | "/reset" => "cancel".to_string(),
        "track swap" | "check swap status" | "swap status" => "status_help".to_string(),
        _ => {
            if let Some(remainder) = trimmed.strip_prefix('/') {
                remainder.trim().to_string()
            } else {
                trimmed.to_string()
            }
        }
    }
}

fn is_greeting(input: &str) -> bool {
    matches!(
        input.trim(),
        "hi" | "hello" | "hey" | "good morning" | "good afternoon" | "good evening"
    )
}

#[derive(Debug, Clone)]
struct AssetFamilyKey {
    ticker: String,
    name: String,
}

fn parse_status_command(input: &str) -> Option<String> {
    let normalized = input.trim();
    let lowercase = normalized.to_ascii_lowercase();
    if !lowercase.starts_with("status ") {
        return None;
    }

    normalized
        .split_once(' ')
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_quote_selection(input: &str) -> Option<usize> {
    input.trim().parse::<usize>().ok()
}

fn parse_amount(input: &str) -> Option<f64> {
    let value = input.trim().replace(',', "");
    let parsed = value.parse::<f64>().ok()?;
    if parsed > 0.0 {
        Some(parsed)
    } else {
        None
    }
}

fn parse_asset_selection_id(
    catalog: &[CurrencyResponse],
    input: &str,
) -> Option<CurrencySelection> {
    let normalized = input.trim().to_ascii_lowercase();
    let remainder = normalized.strip_prefix("asset:")?;
    let (ticker, network_token) = remainder.split_once(':')?;

    catalog
        .iter()
        .find(|currency| {
            normalize_phrase(&currency.ticker) == normalize_phrase(ticker)
                && normalize_phrase(&currency.network).replace(' ', "_") == network_token
        })
        .cloned()
        .map(CurrencySelection::from)
}

fn build_asset_selection_id(option: &CurrencySelection) -> String {
    format!(
        "asset:{}:{}",
        normalize_phrase(&option.ticker).replace(' ', "_"),
        normalize_phrase(&option.network).replace(' ', "_")
    )
}

fn parse_family_selection_id(input: &str) -> Option<AssetFamilyKey> {
    let normalized = input.trim().to_ascii_lowercase();
    let remainder = normalized.strip_prefix("family:")?;
    let (ticker, name) = remainder.split_once(':')?;

    Some(AssetFamilyKey {
        ticker: ticker.to_string(),
        name: name.to_string(),
    })
}

fn build_family_selection_id(family: &AssetFamilySelection) -> String {
    format!(
        "family:{}:{}",
        normalize_phrase(&family.ticker).replace(' ', "_"),
        normalize_phrase(&family.name).replace(' ', "_")
    )
}

fn pending_family(draft: &SwapDraft, side: AssetSide) -> Option<&AssetFamilySelection> {
    match side {
        AssetSide::From => draft.pending_from_family.as_ref(),
        AssetSide::To => draft.pending_to_family.as_ref(),
    }
}

fn set_pending_family(draft: &mut SwapDraft, side: AssetSide, value: Option<AssetFamilySelection>) {
    match side {
        AssetSide::From => draft.pending_from_family = value,
        AssetSide::To => draft.pending_to_family = value,
    }
}

fn should_restart_asset_search_from_network_choice(
    family: &AssetFamilySelection,
    input: &str,
) -> bool {
    let normalized_input = normalize_phrase(input);
    if normalized_input.is_empty() {
        return false;
    }

    normalized_input == normalize_phrase(&family.ticker)
        || normalized_input == normalize_phrase(&family.name)
}

fn plan_asset_input(catalog: &[CurrencyResponse], phrase: &str) -> Result<AssetInputPlan, String> {
    let resolution = resolve_currency_phrase(catalog, phrase)?;

    if let Some(selected) = resolution.selected {
        return Ok(AssetInputPlan::Selected(selected));
    }

    if !resolution.ambiguous_options.is_empty() {
        let families = group_asset_families(&resolution.ambiguous_options);
        if families.len() == 1 {
            let family = families[0].clone();
            let options = find_family_currencies(catalog, &family)
                .into_iter()
                .map(CurrencySelection::from)
                .collect::<Vec<_>>();

            if options.len() == 1 {
                return Ok(AssetInputPlan::Selected(options[0].clone()));
            }

            return Ok(AssetInputPlan::ChooseNetwork { family, options });
        }

        return Ok(AssetInputPlan::ChooseAsset(families));
    }

    Ok(AssetInputPlan::Error(resolution.error.unwrap_or_else(
        || "I could not match that asset. Try another search.".to_string(),
    )))
}

fn group_asset_families(options: &[CurrencySelection]) -> Vec<AssetFamilySelection> {
    let mut families = Vec::new();
    let mut seen = HashSet::new();

    for option in options {
        let family = AssetFamilySelection {
            ticker: option.ticker.clone(),
            name: option.name.clone(),
        };
        let key = build_family_selection_id(&family);
        if seen.insert(key) {
            families.push(family);
        }
    }

    families
}

fn find_family_selection(
    catalog: &[CurrencyResponse],
    family_key: &AssetFamilyKey,
) -> Option<AssetFamilySelection> {
    catalog.iter().find_map(|currency| {
        let ticker = normalize_phrase(&currency.ticker).replace(' ', "_");
        let name = normalize_phrase(&currency.name).replace(' ', "_");

        (ticker == family_key.ticker && name == family_key.name).then(|| AssetFamilySelection {
            ticker: currency.ticker.clone(),
            name: currency.name.clone(),
        })
    })
}

fn find_family_currencies(
    catalog: &[CurrencyResponse],
    family: &AssetFamilySelection,
) -> Vec<CurrencyResponse> {
    let mut matches = catalog
        .iter()
        .filter(|currency| {
            normalize_phrase(&currency.ticker) == normalize_phrase(&family.ticker)
                && normalize_phrase(&currency.name) == normalize_phrase(&family.name)
        })
        .cloned()
        .collect::<Vec<_>>();

    matches.sort_by(|left, right| {
        network_sort_key(&left.network)
            .cmp(&network_sort_key(&right.network))
            .then_with(|| {
                left.network
                    .to_ascii_lowercase()
                    .cmp(&right.network.to_ascii_lowercase())
            })
    });

    matches
}

fn network_sort_key(network: &str) -> (u8, String) {
    let normalized = normalize_phrase(network);
    let priority = match normalized.as_str() {
        "mainnet" => 0,
        "erc20" => 1,
        "trc20" => 2,
        "bep20" => 3,
        _ => 10,
    };

    (priority, normalized)
}

fn parse_swap_intent(input: &str) -> Option<ParsedSwapIntent> {
    static SWAP_RE: OnceLock<Regex> = OnceLock::new();
    let regex = SWAP_RE.get_or_init(|| {
        Regex::new(r"(?i)^(?:swap\s+)?(?P<amount>\d+(?:\.\d+)?)\s+(?P<from>.+?)\s+(?:to|for)\s+(?P<to>.+)$")
            .expect("valid swap regex")
    });

    let captures = regex.captures(input.trim())?;
    Some(ParsedSwapIntent {
        amount: captures
            .name("amount")
            .and_then(|value| parse_amount(value.as_str())),
        from_phrase: captures
            .name("from")
            .map(|value| value.as_str().trim().to_string()),
        to_phrase: captures
            .name("to")
            .map(|value| value.as_str().trim().to_string()),
    })
}

fn resolve_currency_phrase(
    catalog: &[CurrencyResponse],
    phrase: &str,
) -> Result<AssetResolution, String> {
    let normalized_phrase = normalize_phrase(phrase);
    if normalized_phrase.is_empty() {
        return Ok(AssetResolution {
            selected: None,
            ambiguous_options: Vec::new(),
            error: Some("The asset description was empty.".to_string()),
        });
    }

    let network_aliases = build_network_aliases(catalog);

    if let Some((asset_phrase, explicit_network)) =
        split_explicit_network_phrase(phrase, &network_aliases)
    {
        let filtered = catalog
            .iter()
            .filter(|currency| currency.network.eq_ignore_ascii_case(&explicit_network))
            .cloned()
            .collect::<Vec<_>>();

        if !filtered.is_empty() {
            return Ok(resolve_asset_phrase(
                &filtered,
                &asset_phrase,
                phrase,
                Some(&explicit_network),
            ));
        }
    }

    let matched_networks = network_aliases
        .iter()
        .filter(|(alias, _)| normalized_phrase.contains(alias.as_str()))
        .map(|(_, canonical)| canonical.clone())
        .collect::<HashSet<_>>();

    let mut scored = score_currency_matches(catalog, &normalized_phrase, Some(&matched_networks));

    if scored.is_empty() {
        if let Some(selected) = fuzzy_match_currency(catalog, &normalized_phrase, false) {
            return Ok(AssetResolution {
                selected: Some(CurrencySelection::from(selected)),
                ambiguous_options: Vec::new(),
                error: None,
            });
        }

        return Ok(unmatched_asset_resolution(phrase));
    }

    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| {
                left.1
                    .ticker
                    .to_lowercase()
                    .cmp(&right.1.ticker.to_lowercase())
            })
            .then_with(|| {
                left.1
                    .network
                    .to_lowercase()
                    .cmp(&right.1.network.to_lowercase())
            })
    });

    let top_score = scored.first().map(|entry| entry.0).unwrap_or_default();
    let top = scored
        .into_iter()
        .filter(|entry| entry.0 == top_score)
        .map(|(_, currency)| currency)
        .collect::<Vec<_>>();

    Ok(resolve_ranked_matches(
        top,
        &normalized_phrase,
        phrase,
        None,
    ))
}

fn build_network_aliases(catalog: &[CurrencyResponse]) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    for currency in catalog {
        aliases
            .entry(normalize_phrase(&currency.network))
            .or_insert_with(|| currency.network.clone());
    }

    for (alias, canonical) in [
        ("stellar", "XLM"),
        ("xlm", "XLM"),
        ("solana", "SOL"),
        ("sol", "SOL"),
        ("erc20", "ERC20"),
        ("eth", "ERC20"),
        ("ethereum", "ERC20"),
        ("eth mainnet", "ERC20"),
        ("tron", "TRC20"),
        ("trc20", "TRC20"),
        ("bep20", "BEP20"),
        ("bsc", "BEP20"),
        ("bnb chain", "BEP20"),
        ("bnb smart chain", "BEP20"),
        ("binance smart chain", "BEP20"),
        ("avax c", "AVAXC"),
        ("avaxc", "AVAXC"),
        ("avalanche", "AVAXC"),
        ("avax", "AVAXC"),
        ("arb", "Arbitrum"),
        ("polygon", "Polygon"),
        ("matic", "Polygon"),
        ("arbitrum", "Arbitrum"),
        ("op", "Optimism"),
        ("optimism", "Optimism"),
        ("base", "Base"),
        ("mainnet", "Mainnet"),
        ("lightning", "Lightning"),
        ("omni", "OMNI"),
    ] {
        aliases
            .entry(normalize_phrase(alias))
            .or_insert_with(|| canonical.to_string());
    }

    aliases
}

fn split_explicit_network_phrase(
    phrase: &str,
    network_aliases: &HashMap<String, String>,
) -> Option<(String, String)> {
    static ASSET_ON_NETWORK_RE: OnceLock<Regex> = OnceLock::new();
    let regex = ASSET_ON_NETWORK_RE.get_or_init(|| {
        Regex::new(r"(?i)^(?P<asset>.+?)\s+on\s+(?P<network>.+)$")
            .expect("valid asset-on-network regex")
    });

    if let Some(captures) = regex.captures(phrase.trim()) {
        let asset = captures.name("asset")?.as_str().trim();
        let network = captures.name("network")?.as_str().trim();
        if let Some(canonical) = resolve_network_alias(network_aliases, network) {
            return Some((asset.to_string(), canonical));
        }
    }

    let normalized_phrase = normalize_phrase(phrase);
    if normalized_phrase.split_whitespace().count() < 2 {
        return None;
    }

    let mut aliases = network_aliases.keys().cloned().collect::<Vec<_>>();
    aliases.sort_by(|left, right| {
        right
            .split_whitespace()
            .count()
            .cmp(&left.split_whitespace().count())
            .then_with(|| right.len().cmp(&left.len()))
    });

    for alias in aliases {
        let suffix = format!(" {}", alias);
        if let Some(asset_part) = normalized_phrase.strip_suffix(&suffix) {
            let trimmed_asset = asset_part.trim();
            if trimmed_asset.is_empty() {
                continue;
            }

            if let Some(canonical) = network_aliases.get(&alias) {
                return Some((trimmed_asset.to_string(), canonical.clone()));
            }
        }
    }

    None
}

fn resolve_network_alias(
    network_aliases: &HashMap<String, String>,
    network_phrase: &str,
) -> Option<String> {
    let normalized = normalize_phrase(network_phrase);
    if normalized.is_empty() {
        return None;
    }

    if let Some(canonical) = network_aliases.get(&normalized) {
        return Some(canonical.clone());
    }

    let mut fuzzy_matches = network_aliases
        .iter()
        .filter_map(|(alias, canonical)| {
            let distance = levenshtein_distance(&normalized, alias);
            let threshold = fuzzy_threshold(alias.len());
            (distance <= threshold).then(|| (distance, alias.len(), canonical.clone()))
        })
        .collect::<Vec<_>>();

    fuzzy_matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let best = fuzzy_matches.first()?;
    if fuzzy_matches
        .get(1)
        .map(|candidate| candidate.0 == best.0 && candidate.2 != best.2)
        .unwrap_or(false)
    {
        return None;
    }

    Some(best.2.clone())
}

fn resolve_asset_phrase(
    catalog: &[CurrencyResponse],
    asset_phrase: &str,
    original_phrase: &str,
    explicit_network: Option<&str>,
) -> AssetResolution {
    let normalized_asset = normalize_phrase(asset_phrase);
    if normalized_asset.is_empty() {
        return unmatched_asset_resolution(original_phrase);
    }

    let scored = score_currency_matches(catalog, &normalized_asset, None);
    if scored.is_empty() {
        if let Some(selected) = fuzzy_match_currency(catalog, &normalized_asset, true) {
            return AssetResolution {
                selected: Some(CurrencySelection::from(selected)),
                ambiguous_options: Vec::new(),
                error: None,
            };
        }

        let message = match explicit_network {
            Some(network) => format!(
                "I could not find \"{}\" on {}. Try another asset or network.",
                asset_phrase, network
            ),
            None => format!(
                "I could not match \"{}\" to a supported asset. Try something like usdc on stellar, btc mainnet, xmr, or send a broader search term like usd or bit.",
                original_phrase
            ),
        };

        return AssetResolution {
            selected: None,
            ambiguous_options: Vec::new(),
            error: Some(message),
        };
    }

    resolve_ranked_matches(
        scored.into_iter().map(|(_, currency)| currency).collect(),
        &normalized_asset,
        original_phrase,
        explicit_network,
    )
}

fn score_currency_matches(
    catalog: &[CurrencyResponse],
    normalized_phrase: &str,
    matched_networks: Option<&HashSet<String>>,
) -> Vec<(usize, CurrencyResponse)> {
    let mut scored = Vec::new();

    for currency in catalog {
        let ticker_alias = normalize_phrase(&currency.ticker);
        let name_alias = normalize_phrase(&currency.name);
        let network_alias = normalize_phrase(&currency.network);

        let mut score = 0usize;
        if normalized_phrase == ticker_alias {
            score += 220;
        } else if normalized_phrase
            .split_whitespace()
            .any(|token| token == ticker_alias)
        {
            score += 170;
        } else if normalized_phrase.contains(&ticker_alias) {
            score += 120;
        }

        if normalized_phrase == name_alias {
            score += 210;
        } else if normalized_phrase.contains(&name_alias) {
            score += 140;
        }

        if let Some(matched_networks) = matched_networks {
            if matched_networks.contains(&currency.network)
                || normalized_phrase.contains(&network_alias)
            {
                score += 40;
            }
        }

        if score > 0 {
            scored.push((score, currency.clone()));
        }
    }

    scored
}

fn resolve_ranked_matches(
    ranked: Vec<CurrencyResponse>,
    normalized_phrase: &str,
    original_phrase: &str,
    explicit_network: Option<&str>,
) -> AssetResolution {
    if ranked.is_empty() {
        return unmatched_asset_resolution(original_phrase);
    }

    if ranked.len() == 1 {
        return AssetResolution {
            selected: ranked.into_iter().next().map(CurrencySelection::from),
            ambiguous_options: Vec::new(),
            error: None,
        };
    }

    let mut rescored = score_currency_matches(&ranked, normalized_phrase, None);
    rescored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| normalize_phrase(&left.1.ticker).cmp(&normalize_phrase(&right.1.ticker)))
            .then_with(|| {
                normalize_phrase(&left.1.network).cmp(&normalize_phrase(&right.1.network))
            })
    });

    let top_score = rescored.first().map(|entry| entry.0).unwrap_or_default();
    let top = rescored
        .into_iter()
        .filter(|entry| entry.0 == top_score)
        .map(|(_, currency)| currency)
        .collect::<Vec<_>>();

    if top.len() == 1 {
        return AssetResolution {
            selected: top.into_iter().next().map(CurrencySelection::from),
            ambiguous_options: Vec::new(),
            error: None,
        };
    }

    let unique_networks = top
        .iter()
        .map(|currency| normalize_phrase(&currency.network))
        .collect::<HashSet<_>>();

    if unique_networks.len() == 1 {
        return AssetResolution {
            selected: top.into_iter().next().map(CurrencySelection::from),
            ambiguous_options: Vec::new(),
            error: None,
        };
    }

    let exact_asset_match = top.iter().all(|currency| {
        normalize_phrase(&currency.ticker) == normalized_phrase
            || normalize_phrase(&currency.name) == normalized_phrase
    });

    if explicit_network.is_none() && exact_asset_match {
        if let Some(mainnet_currency) = top
            .iter()
            .find(|currency| currency.network.eq_ignore_ascii_case("Mainnet"))
        {
            return AssetResolution {
                selected: Some(CurrencySelection::from(mainnet_currency.clone())),
                ambiguous_options: Vec::new(),
                error: None,
            };
        }
    }

    let message = match explicit_network {
        Some(network) => format!(
            "I found multiple assets matching \"{}\" on {}. Reply with the exact ticker or full asset name.",
            original_phrase, network
        ),
        None => format!("\"{}\" matches multiple networks. Specify the network too.", original_phrase),
    };

    AssetResolution {
        selected: None,
        ambiguous_options: top
            .into_iter()
            .take(100)
            .map(CurrencySelection::from)
            .collect::<Vec<_>>(),
        error: Some(message),
    }
}

fn fuzzy_match_currency(
    catalog: &[CurrencyResponse],
    normalized_phrase: &str,
    restricted_scope: bool,
) -> Option<CurrencyResponse> {
    let mut matches = catalog
        .iter()
        .filter_map(|currency| {
            let ticker_alias = normalize_phrase(&currency.ticker);
            let name_alias = normalize_phrase(&currency.name);
            let ticker_distance = levenshtein_distance(normalized_phrase, &ticker_alias);
            let name_distance = levenshtein_distance(normalized_phrase, &name_alias);
            let (distance, prefers_ticker) = if ticker_distance <= name_distance {
                (ticker_distance, true)
            } else {
                (name_distance, false)
            };

            let threshold = if prefers_ticker {
                fuzzy_threshold(ticker_alias.len())
            } else {
                fuzzy_threshold(name_alias.len())
            };

            (distance <= threshold).then(|| {
                (
                    distance,
                    if prefers_ticker { 0usize } else { 1usize },
                    currency.clone(),
                )
            })
        })
        .collect::<Vec<_>>();

    matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let best = matches.first()?;
    if !restricted_scope && best.0 > 1 {
        return None;
    }

    if matches
        .get(1)
        .map(|candidate| {
            candidate.0 == best.0
                && normalize_phrase(&candidate.2.ticker) != normalize_phrase(&best.2.ticker)
        })
        .unwrap_or(false)
    {
        return None;
    }

    Some(best.2.clone())
}

fn unmatched_asset_resolution(phrase: &str) -> AssetResolution {
    AssetResolution {
        selected: None,
        ambiguous_options: Vec::new(),
        error: Some(format!(
            "No match for \"{}\". Try btc, xmr, usdc, or add a network like usdc on stellar.",
            phrase
        )),
    }
}

fn fuzzy_threshold(length: usize) -> usize {
    match length {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn levenshtein_distance(left: &str, right: &str) -> usize {
    if left == right {
        return 0;
    }
    if left.is_empty() {
        return right.chars().count();
    }
    if right.is_empty() {
        return left.chars().count();
    }

    let left_chars = left.chars().collect::<Vec<_>>();
    let right_chars = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right_chars.len()).collect::<Vec<_>>();

    for (left_index, left_char) in left_chars.iter().enumerate() {
        let mut current = vec![left_index + 1];
        for (right_index, right_char) in right_chars.iter().enumerate() {
            let substitution_cost = usize::from(left_char != right_char);
            let insertion = current[right_index] + 1;
            let deletion = previous[right_index + 1] + 1;
            let substitution = previous[right_index] + substitution_cost;
            current.push(insertion.min(deletion).min(substitution));
        }
        previous = current;
    }

    previous[right_chars.len()]
}

fn normalize_phrase(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character.is_ascii_whitespace() {
                character
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn trim_f64(value: f64) -> String {
    let rendered = format!("{:.8}", value);
    rendered
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn truncate_whatsapp_text(value: &str, max_chars: usize) -> String {
    let characters = value.chars().collect::<Vec<_>>();
    if characters.len() <= max_chars {
        return value.to_string();
    }

    let keep = max_chars.saturating_sub(1);
    let mut shortened = characters.into_iter().take(keep).collect::<String>();
    shortened.push('…');
    shortened
}

#[cfg(test)]
mod tests {
    use super::{
        parse_asset_selection_id, resolve_currency_phrase,
        should_restart_asset_search_from_network_choice, AssetFamilySelection,
    };
    use crate::modules::swap::schema::CurrencyResponse;

    fn currency(name: &str, ticker: &str, network: &str) -> CurrencyResponse {
        CurrencyResponse {
            name: name.to_string(),
            ticker: ticker.to_string(),
            network: network.to_string(),
            memo: false,
            extra_id_name: None,
            image: String::new(),
            minimum: 0.0,
            maximum: 0.0,
        }
    }

    fn sample_catalog() -> Vec<CurrencyResponse> {
        vec![
            currency("Ethereum", "eth", "Mainnet"),
            currency("USD Coin", "usdc", "ERC20"),
            currency("USD Coin", "usdc", "Arbitrum"),
            currency("Bitcoin", "btc", "Mainnet"),
        ]
    }

    #[test]
    fn resolves_usdc_on_eth_to_erc20_network() {
        let resolution =
            resolve_currency_phrase(&sample_catalog(), "usdc on eth").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "usdc");
        assert_eq!(selected.network, "ERC20");
    }

    #[test]
    fn resolves_usdc_with_trailing_network_alias() {
        let resolution =
            resolve_currency_phrase(&sample_catalog(), "usdc eth").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "usdc");
        assert_eq!(selected.network, "ERC20");
    }

    #[test]
    fn keeps_usdc_ambiguous_without_network() {
        let resolution = resolve_currency_phrase(&sample_catalog(), "usdc").expect("resolution");

        assert!(resolution.selected.is_none());
        assert_eq!(resolution.ambiguous_options.len(), 2);
        assert!(resolution
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("multiple networks"));
    }

    #[test]
    fn plain_eth_still_resolves_to_native_eth() {
        let resolution = resolve_currency_phrase(&sample_catalog(), "eth").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "eth");
        assert_eq!(selected.network, "Mainnet");
    }

    #[test]
    fn resolves_bitcoin_name_to_mainnet_btc() {
        let resolution = resolve_currency_phrase(&sample_catalog(), "bitcoin").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "btc");
        assert_eq!(selected.network, "Mainnet");
    }

    #[test]
    fn parses_structured_asset_selection_id() {
        let selected = parse_asset_selection_id(&sample_catalog(), "asset:usdc:erc20")
            .expect("selected asset");

        assert_eq!(selected.ticker, "usdc");
        assert_eq!(selected.network, "ERC20");
    }

    #[test]
    fn network_choice_restarts_global_search_when_user_retypes_family_ticker() {
        let family = AssetFamilySelection {
            ticker: "xlm".to_string(),
            name: "Stellar".to_string(),
        };

        assert!(should_restart_asset_search_from_network_choice(
            &family, "xlm"
        ));
        assert!(should_restart_asset_search_from_network_choice(
            &family, "stellar"
        ));
        assert!(!should_restart_asset_search_from_network_choice(
            &family, "mainnet"
        ));
    }
}
