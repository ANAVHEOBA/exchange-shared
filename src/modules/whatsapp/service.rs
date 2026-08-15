use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::modules::swap::crud::{CurrenciesResult, SwapCrud, SwapError};
use crate::modules::swap::schema::{
    CreateSwapRequest, CurrenciesQuery, CurrencyResponse, RateResponse, RateType, RatesQuery,
    ValidateAddressRequest,
};
use crate::modules::whatsapp::crud::{SessionRecord, WhatsAppCrud};
use crate::services::kimi::{KimiAmountMode, KimiConfirmation};
use crate::services::pricing::CommissionService;
use crate::services::trocador::TrocadorGateway;
use crate::services::whatsapp::{
    derive_whatsapp_client_id, InteractiveListOption, InteractiveListSection, ReplyButtonOption,
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
    AwaitingAmountMode,
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
            "awaiting_amount_mode" => Self::AwaitingAmountMode,
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
    #[serde(default)]
    privacy_rating: Option<String>,
    #[serde(default)]
    eta_minutes: Option<u32>,
    trade_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AmountInputMode {
    SourceAsset,
    Usd,
}

impl From<KimiAmountMode> for AmountInputMode {
    fn from(value: KimiAmountMode) -> Self {
        match value {
            KimiAmountMode::SourceAsset => Self::SourceAsset,
            KimiAmountMode::Usd => Self::Usd,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SwapDraft {
    from: Option<CurrencySelection>,
    to: Option<CurrencySelection>,
    pending_from_family: Option<AssetFamilySelection>,
    pending_to_family: Option<AssetFamilySelection>,
    #[serde(default)]
    pending_from_family_options: Vec<AssetFamilySelection>,
    #[serde(default)]
    pending_to_family_options: Vec<AssetFamilySelection>,
    #[serde(default)]
    pending_from_currency_options: Vec<CurrencySelection>,
    #[serde(default)]
    pending_to_currency_options: Vec<CurrencySelection>,
    amount: Option<f64>,
    amount_input_mode: Option<AmountInputMode>,
    requested_amount_usd: Option<f64>,
    quotes: Vec<QuoteChoice>,
    selected_quote: Option<QuoteChoice>,
    recipient_address: Option<String>,
    recipient_extra_id: Option<String>,
    refund_address: Option<String>,
    refund_extra_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedSwapIntent {
    amount: Option<f64>,
    amount_mode: Option<AmountInputMode>,
    from_phrase: Option<String>,
    to_phrase: Option<String>,
    recipient_address: Option<String>,
    refund_address: Option<String>,
}

#[derive(Debug, Clone)]
struct ParsedAmountInput {
    amount: f64,
    mode: AmountInputMode,
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

    /// Plain-language description of `asset_prompt` for `narrate_or`, kept in
    /// sync with it by hand since one is a fixed string and the other is an
    /// instruction for the model to rephrase.
    fn asset_prompt_situation(self) -> &'static str {
        match self {
            Self::From => {
                "Ask the user what coin they want to send, mentioning they can type a ticker or \
                 coin name, with examples like btc, xlm, or usdc on stellar."
            }
            Self::To => {
                "Ask the user what coin they want to receive, mentioning they can type a ticker \
                 or coin name, with examples like xmr, btc, or usdt on tron."
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
        }

        result
    }

    /// Last-resort notice sent when the worker gives up retrying a message
    /// event entirely. Without this, an unexpected error anywhere in the flow
    /// (anything other than the two error types `fetch_and_prompt_quotes`
    /// already turns into a reply) left the user in total silence - the
    /// worker just logs it and marks the event failed. The specific reason
    /// still isn't shown to the user, but they at least know to try again.
    pub async fn notify_processing_failed(
        &self,
        wa_id: &str,
        phone_number_id: &str,
    ) -> Result<(), String> {
        let message = self
            .narrate_or(
                "Tell the user something went wrong processing their WhatsApp message and ask them to try again. Keep it short and do not mention commands or menus.",
                "Something went wrong processing that. Send it again and I'll pick it up from there.",
            )
            .await;

        self.reply(wa_id, phone_number_id, None, &message).await
    }

    async fn process_message_event_locked(
        &self,
        phone_number_id: &str,
        wa_id: &str,
        inbound_message_id: Option<&str>,
        text: &str,
    ) -> Result<(), String> {
        let raw_trimmed = text.trim();

        self.acknowledge_inbound_message(inbound_message_id).await;

        if raw_trimmed.is_empty() {
            let message = self
                .narrate_or(
                    "The user sent an empty WhatsApp message. Ask what swap or status check they need. Do not mention commands or menus.",
                    "What do you want to swap or check?",
                )
                .await;

            return self.reply(wa_id, phone_number_id, None, &message).await;
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
            let message = self
                .narrate_or(
                    "Tell the user they can paste their Assetar swap ID if they want a status check. Keep it conversational and do not show a menu.",
                    "Paste your Assetar swap ID and I'll check it.",
                )
                .await;

            return self
                .reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                .await;
        }

        if is_cancel_request(trimmed) {
            let had_active_swap = state != ConversationState::Idle;
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

            let message = self
                .narrate_or(if had_active_swap {
                    "Tell the user the current swap setup has been cancelled and ask what they want to do next. Do not mention commands or menus."
                } else {
                    "Tell the user there is no active swap running anymore and they can message again whenever they want to start one. Keep it short and natural."
                }, if had_active_swap {
                    "Alright, I cancelled that swap setup. What do you want to do next?"
                } else {
                    "Alright. Nothing is running on my side now. Message me whenever you want to start a swap."
                })
                .await;

            return self
                .reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                .await;
        }

        if is_generic_swap_start(&lowered) {
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

            let prompt = self
                .narrate_or(
                    AssetSide::From.asset_prompt_situation(),
                    AssetSide::From.asset_prompt(),
                )
                .await;

            return self
                .reply(wa_id, phone_number_id, session_id.as_deref(), &prompt)
                .await;
        }

        if lowered == "examples" {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    "You can say: swap 100 USDT to XMR, or swap $250 of BTC to Monero. If you already have the receiving address, add it in the same message.",
                )
                .await;
        }

        if lowered == "help"
            || lowered == "menu"
            || lowered == "start"
            || matches!(lowered.as_str(), "hi" | "hello" | "hey" | "yo" | "sup")
        {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    "I'm here. Tell me the swap you want, like: swap 100 USDT to XMR. Or paste a swap ID and I'll check it.",
                )
                .await;
        }

        if let Some(swap_id) = parse_status_lookup_input(trimmed) {
            return match self
                .send_status(wa_id, phone_number_id, session_id.as_deref(), &swap_id)
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.reply_to_inbound(
                        wa_id,
                        phone_number_id,
                        session_id.as_deref(),
                        inbound_message_id,
                        &error,
                    )
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

                    let prompt = self
                        .narrate_or(
                            AssetSide::From.asset_prompt_situation(),
                            AssetSide::From.asset_prompt(),
                        )
                        .await;

                    return self
                        .reply(wa_id, phone_number_id, session_id.as_deref(), &prompt)
                        .await;
                }

                if let Some(intent) = parse_swap_intent(trimmed) {
                    return self
                        .handle_parsed_swap_intent(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &locale,
                            draft,
                            inbound_message_id,
                            trimmed,
                            intent,
                        )
                        .await;
                }

                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    "I'm here. Tell me what you want to swap, or paste a swap ID for me to check.",
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
            ConversationState::AwaitingAmountMode => {
                self.handle_amount_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                )
                .await
            }
            ConversationState::AwaitingAmount => {
                self.handle_amount_input(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &locale,
                    draft,
                    inbound_message_id,
                    trimmed,
                )
                .await
            }
            ConversationState::AwaitingQuoteSelection => {
                let choice_index = parse_quote_selection_id(trimmed)
                    .or_else(|| parse_quote_selection(trimmed))
                    .or_else(|| parse_quote_selection_by_provider(trimmed, &draft.quotes));
                let choice_index = match choice_index {
                    Some(index) => Some(index),
                    None => {
                        self.extract_quote_selection_via_kimi(trimmed, draft.quotes.len())
                            .await
                    }
                };

                let Some(choice_index) = choice_index else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Tell me the provider you want to use, or send your destination address and I’ll keep the recommended route.",
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
                let Some(target) = draft.to.as_ref().cloned() else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "I lost the destination asset for this swap. Send the swap details again.",
                        )
                        .await;
                };
                if let Some(choice_index) = parse_quote_selection(trimmed)
                    .or_else(|| parse_quote_selection_by_provider(trimmed, &draft.quotes))
                {
                    if let Some(selected_quote) = draft
                        .quotes
                        .iter()
                        .find(|quote| quote.index == choice_index)
                        .cloned()
                    {
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

                        return self
                            .reply(
                                wa_id,
                                phone_number_id,
                                session_id.as_deref(),
                                &format!(
                                    "Okay, I switched the route to {}. Send your {} on {} receiving address.",
                                    selected_quote.provider_name,
                                    target.ticker.to_uppercase(),
                                    target.network
                                ),
                            )
                            .await;
                    }
                }

                if is_show_routes_command(trimmed) && !draft.quotes.is_empty() {
                    let alternative_quotes =
                        if let Some(selected_quote) = draft.selected_quote.as_ref() {
                            draft
                                .quotes
                                .iter()
                                .filter(|quote| {
                                    !quote
                                        .provider_name
                                        .eq_ignore_ascii_case(&selected_quote.provider_name)
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        } else {
                            draft.quotes.clone()
                        };
                    let route_summary = summarize_quote_providers(&alternative_quotes);
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &format!(
                                "Best live route right now is {}. Other available providers: {}. Send your destination address when you're ready.",
                                draft
                                    .selected_quote
                                    .as_ref()
                                    .map(|quote| quote.provider_name.as_str())
                                    .unwrap_or("the recommended provider"),
                                route_summary
                            ),
                        )
                        .await;
                }

                let candidate_address = self
                    .extract_address_via_kimi(trimmed, &target.ticker, &target.network)
                    .await
                    .unwrap_or_else(|| trimmed.to_string());

                if let Err(error) = self
                    .validate_address(&target.ticker, &target.network, &candidate_address)
                    .await
                {
                    return self
                        .reply_to_inbound(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            inbound_message_id,
                            &error,
                        )
                        .await;
                }
                draft.recipient_address = Some(candidate_address);
                if draft.selected_quote.is_none() {
                    draft.selected_quote = draft.quotes.first().cloned();
                }

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
                            self.reply_to_inbound(
                                wa_id,
                                phone_number_id,
                                session_id.as_deref(),
                                inbound_message_id,
                                &error,
                            )
                            .await
                        }
                    }
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
                        self.reply_to_inbound(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            inbound_message_id,
                            &error,
                        )
                        .await
                    }
                }
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
                            "I lost the source asset for this swap. Send the swap details again.",
                        )
                        .await;
                };
                if let Err(error) = self
                    .validate_address(&source.ticker, &source.network, trimmed)
                    .await
                {
                    return self
                        .reply_to_inbound(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            inbound_message_id,
                            &format!(
                                "{} Or reply skip to continue without a refund address.",
                                error
                            ),
                        )
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
                // Deterministic only, deliberately: this is the step that
                // actually creates the swap, so it must never depend on an AI
                // interpretation of ambiguous text. Unclear input falls
                // straight to "reply confirm or cancel" below.
                let decision = parse_confirmation_decision(trimmed);

                match decision {
                    Some(KimiConfirmation::Cancel) => {
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
                                "No problem. I cancelled that swap setup. Send the next one whenever you're ready.",
                            )
                            .await;
                    }
                    Some(KimiConfirmation::Confirm) => {}
                    None => {
                        return self
                            .prompt_confirmation(
                                wa_id,
                                phone_number_id,
                                session_id.as_deref(),
                                &locale,
                                draft,
                                inbound_message_id,
                            )
                            .await;
                    }
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
                    draft
                        .selected_quote
                        .as_ref()
                        .and_then(|quote| format_eta_line(quote.eta_minutes))
                        .unwrap_or_else(|| "ETA: unavailable".to_string()),
                    format_expiry_line("Deposit window left", response.expires_at)
                        .unwrap_or_else(|| "Deposit window left: unavailable".to_string()),
                    "Paste this swap ID later if you want me to check progress.".to_string(),
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

                if let Some(qr_link) =
                    self.swap_deposit_qr_link(phone_number_id, wa_id, &response.swap_id)
                {
                    if let Err(error) = self
                        .reply_image(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            &qr_link,
                            Some("Deposit QR"),
                        )
                        .await
                    {
                        tracing::warn!(
                            "failed to send WhatsApp deposit QR for swap {}: {}",
                            response.swap_id,
                            error
                        );
                    }
                }

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
            let prompt = self
                .narrate_or(side.asset_prompt_situation(), side.asset_prompt())
                .await;

            return self
                .reply(wa_id, phone_number_id, session_id, &prompt)
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
                set_pending_family_options(&mut draft, side, Vec::new());
                set_pending_currency_options(&mut draft, side, options.clone());
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
                set_pending_family_options(&mut draft, side, families.clone());
                set_pending_currency_options(&mut draft, side, Vec::new());
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
                    options.clone(),
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
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        input: &str,
        side: AssetSide,
    ) -> Result<(), String> {
        let catalog = self.fetch_currency_catalog().await?;

        if let Some(index) = parse_quote_selection(input) {
            if let Some(selection) = pending_currency_options(&draft, side)
                .get(index - 1)
                .cloned()
            {
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

            if let Some(family) = pending_family_options(&draft, side).get(index - 1).cloned() {
                set_pending_family_options(&mut draft, side, Vec::new());
                return self
                    .handle_asset_family_choice(
                        wa_id,
                        phone_number_id,
                        session_id,
                        locale,
                        draft,
                        inbound_message_id,
                        side,
                        AssetFamilyKey {
                            ticker: normalize_phrase(&family.ticker).replace(' ', "_"),
                            name: normalize_phrase(&family.name).replace(' ', "_"),
                        },
                        &catalog,
                    )
                    .await;
            }
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

        if let Some(index) = parse_quote_selection(input) {
            if let Some(selection) = pending_currency_options(&draft, side)
                .get(index - 1)
                .cloned()
            {
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
        }

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
            set_pending_currency_options(&mut next_draft, side, Vec::new());

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
            set_pending_currency_options(&mut next_draft, side, Vec::new());
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

        let network_options = family_catalog
            .iter()
            .cloned()
            .map(CurrencySelection::from)
            .collect::<Vec<_>>();

        if let Some(selection) = match_pending_network_option(&network_options, input) {
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
                    network_options.clone(),
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
                        "I could not match that network. Reply with the network name or the number from the list.",
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
            options.clone(),
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
        options: Vec<CurrencySelection>,
    ) -> Result<(), String> {
        set_pending_family(&mut draft, side, Some(family));
        set_pending_family_options(&mut draft, side, Vec::new());
        set_pending_currency_options(&mut draft, side, options);
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
        set_pending_family_options(&mut draft, side, Vec::new());
        set_pending_currency_options(&mut draft, side, Vec::new());

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
                    self.prompt_amount(
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
                            &ConversationState::AwaitingToAssetSearch,
                            locale,
                            &draft,
                            inbound_message_id,
                        )
                        .await
                        .map_err(|error| error.to_string())?;

                    let prompt = self
                        .narrate_or(
                            AssetSide::To.asset_prompt_situation(),
                            AssetSide::To.asset_prompt(),
                        )
                        .await;

                    self.reply(wa_id, phone_number_id, session_id, &prompt)
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
                    self.prompt_amount(
                        wa_id,
                        phone_number_id,
                        session_id,
                        locale,
                        draft,
                        inbound_message_id,
                    )
                    .await
                }
            }
        }
    }

    /// Resolves a swap intent - however it was parsed (regex or Kimi) - against the currency
    /// catalog and advances the session to whichever state the resolved fields call for.
    /// Shared by the deterministic `parse_swap_intent` path and the Kimi fallback so both
    /// funnel through the exact same asset-resolution and state-transition logic.
    async fn handle_parsed_swap_intent(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        _raw_text: &str,
        intent: ParsedSwapIntent,
    ) -> Result<(), String> {
        let crud = WhatsAppCrud::new(self.state.db.clone());

        let from_phrase = meaningful_asset_phrase(intent.from_phrase.as_deref());
        let to_phrase = meaningful_asset_phrase(intent.to_phrase.as_deref());
        let recipient_address = normalize_optional_text(intent.recipient_address.as_deref());
        let refund_address = normalize_optional_text(intent.refund_address.as_deref());

        if intent.amount.is_none()
            && from_phrase.is_none()
            && to_phrase.is_none()
            && recipient_address.is_none()
            && refund_address.is_none()
        {
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    "Sure. Send the pair and amount in one message, like: swap 100 USDT to XMR.",
                )
                .await;
        }

        let catalog = self.fetch_currency_catalog().await?;
        let from_plan = match from_phrase {
            Some(value) => Some(self.resolve_asset_input(&catalog, &value).await?),
            None => None,
        };
        let to_plan = match to_phrase {
            Some(value) => Some(self.resolve_asset_input(&catalog, &value).await?),
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

        if let Some(amount) = intent.amount {
            match intent.amount_mode.unwrap_or(AmountInputMode::SourceAsset) {
                AmountInputMode::SourceAsset => {
                    draft.amount = Some(amount);
                    draft.requested_amount_usd = None;
                    draft.amount_input_mode = Some(AmountInputMode::SourceAsset);
                }
                AmountInputMode::Usd => {
                    draft.requested_amount_usd = Some(amount);
                    draft.amount_input_mode = Some(AmountInputMode::Usd);
                    if let Some(from) = draft.from.as_ref() {
                        draft.amount =
                            Some(self.resolve_source_amount_from_usd(from, amount).await?);
                    }
                }
            }
        } else if draft.amount.is_none() {
            if let (Some(from), Some(usd_amount)) =
                (draft.from.as_ref(), draft.requested_amount_usd)
            {
                draft.amount = Some(
                    self.resolve_source_amount_from_usd(from, usd_amount)
                        .await?,
                );
            }
        }

        if let Some(refund_address) = refund_address {
            if let Some(from) = draft.from.as_ref() {
                if let Err(error) = self
                    .validate_address(&from.ticker, &from.network, refund_address)
                    .await
                {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRefundAddress,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_to_inbound(
                            wa_id,
                            phone_number_id,
                            session_id,
                            inbound_message_id,
                            &format!(
                                "{} Or reply skip to continue without a refund address.",
                                error
                            ),
                        )
                        .await;
                }
            }

            draft.refund_address = Some(refund_address.to_string());
        }

        if let Some(recipient_address) = recipient_address {
            if let Some(to) = draft.to.as_ref() {
                if let Err(error) = self
                    .validate_address(&to.ticker, &to.network, recipient_address)
                    .await
                {
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRecipientAddress,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_to_inbound(
                            wa_id,
                            phone_number_id,
                            session_id,
                            inbound_message_id,
                            &error,
                        )
                        .await;
                }
            }

            draft.recipient_address = Some(recipient_address.to_string());
        }

        if let Some(plan) = from_plan {
            match plan {
                AssetInputPlan::Selected(_) => {}
                AssetInputPlan::ChooseResults { prompt, options } => {
                    set_pending_family_options(&mut draft, AssetSide::From, Vec::new());
                    set_pending_currency_options(&mut draft, AssetSide::From, options.clone());
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingFromAssetChoice,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_currency_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &prompt,
                            "Choose coin",
                            &options,
                        )
                        .await;
                }
                AssetInputPlan::ChooseAsset(families) => {
                    set_pending_family_options(&mut draft, AssetSide::From, families.clone());
                    set_pending_currency_options(&mut draft, AssetSide::From, Vec::new());
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingFromAssetChoice,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_asset_family_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &AssetSide::From.family_prompt(families.len()),
                            &families,
                        )
                        .await;
                }
                AssetInputPlan::ChooseNetwork { family, options } => {
                    self.persist_network_choice_state(
                        wa_id,
                        phone_number_id,
                        locale,
                        draft,
                        inbound_message_id,
                        AssetSide::From,
                        family.clone(),
                        options.clone(),
                    )
                    .await?;

                    return self
                        .reply_network_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &AssetSide::From.network_prompt(&family),
                            &options,
                        )
                        .await;
                }
                AssetInputPlan::Error(message) => {
                    return self
                        .reply(wa_id, phone_number_id, session_id, &message)
                        .await;
                }
            }
        }

        if let Some(plan) = to_plan {
            match plan {
                AssetInputPlan::Selected(_) => {}
                AssetInputPlan::ChooseResults { prompt, options } => {
                    set_pending_family_options(&mut draft, AssetSide::To, Vec::new());
                    set_pending_currency_options(&mut draft, AssetSide::To, options.clone());
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingToAssetChoice,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_currency_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &prompt,
                            "Choose coin",
                            &options,
                        )
                        .await;
                }
                AssetInputPlan::ChooseAsset(families) => {
                    set_pending_family_options(&mut draft, AssetSide::To, families.clone());
                    set_pending_currency_options(&mut draft, AssetSide::To, Vec::new());
                    crud.upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingToAssetChoice,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                    return self
                        .reply_asset_family_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &AssetSide::To.family_prompt(families.len()),
                            &families,
                        )
                        .await;
                }
                AssetInputPlan::ChooseNetwork { family, options } => {
                    self.persist_network_choice_state(
                        wa_id,
                        phone_number_id,
                        locale,
                        draft,
                        inbound_message_id,
                        AssetSide::To,
                        family.clone(),
                        options.clone(),
                    )
                    .await?;

                    return self
                        .reply_network_options(
                            wa_id,
                            phone_number_id,
                            session_id,
                            &AssetSide::To.network_prompt(&family),
                            &options,
                        )
                        .await;
                }
                AssetInputPlan::Error(message) => {
                    return self
                        .reply(wa_id, phone_number_id, session_id, &message)
                        .await;
                }
            }
        }

        if draft.from.is_some() && draft.to.is_some() && draft.amount.is_some() {
            return match self
                .fetch_and_prompt_quotes(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                )
                .await
            {
                Ok(()) => Ok(()),
                Err(error) => self.reply(wa_id, phone_number_id, session_id, &error).await,
            };
        }

        if draft.from.is_some() && draft.to.is_some() {
            return self
                .prompt_amount(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                )
                .await;
        }

        if draft.from.is_some() {
            crud.upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::AwaitingToAssetSearch,
                locale,
                &draft,
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

            let prompt = self
                .narrate_or(
                    AssetSide::To.asset_prompt_situation(),
                    AssetSide::To.asset_prompt(),
                )
                .await;

            return self
                .reply(wa_id, phone_number_id, session_id, &prompt)
                .await;
        }

        crud.upsert_session_state(
            wa_id,
            phone_number_id,
            &ConversationState::AwaitingFromAssetSearch,
            locale,
            &draft,
            inbound_message_id,
        )
        .await
        .map_err(|error| error.to_string())?;

        let prompt = self
            .narrate_or(
                AssetSide::From.asset_prompt_situation(),
                AssetSide::From.asset_prompt(),
            )
            .await;

        self.reply(wa_id, phone_number_id, session_id, &prompt)
            .await
    }

    async fn handle_amount_input(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
        trimmed: &str,
    ) -> Result<(), String> {
        let from = draft
            .from
            .as_ref()
            .ok_or_else(|| {
                "I lost the source asset for this swap. Send the swap details again.".to_string()
            })?
            .clone();

        let Some(parsed_input) = self
            .parse_amount_input(trimmed, &from, draft.to.as_ref())
            .await
        else {
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
            return self
                .reply(
                    wa_id,
                    phone_number_id,
                    session_id,
                    &format!(
                        "I couldn't read the amount. Send something like 0.25 {} or $100.",
                        from.ticker.to_uppercase()
                    ),
                )
                .await;
        };

        let amount = match parsed_input.mode {
            AmountInputMode::SourceAsset => {
                draft.requested_amount_usd = None;
                parsed_input.amount
            }
            AmountInputMode::Usd => {
                draft.requested_amount_usd = Some(parsed_input.amount);
                self.resolve_source_amount_from_usd(&from, parsed_input.amount)
                    .await
                    .map_err(|error| error.to_string())?
            }
        };

        draft.amount_input_mode = Some(parsed_input.mode);
        draft.amount = Some(amount);

        match self
            .fetch_and_prompt_quotes(
                wa_id,
                phone_number_id,
                session_id,
                locale,
                draft,
                inbound_message_id,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => self.reply(wa_id, phone_number_id, session_id, &error).await,
        }
    }

    async fn prompt_amount(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        locale: &str,
        mut draft: SwapDraft,
        inbound_message_id: Option<&str>,
    ) -> Result<(), String> {
        let from = draft.from.as_ref().ok_or_else(|| {
            "I lost the source asset for this swap. Send the swap details again.".to_string()
        })?;
        let destination_context = draft
            .to
            .as_ref()
            .map(|asset| format!(" for {} on {}", asset.ticker.to_uppercase(), asset.network))
            .unwrap_or_default();

        draft.amount = None;
        draft.amount_input_mode = None;
        draft.requested_amount_usd = None;

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
            &format!(
                "How much {} on {} do you want to send{}? You can write a coin amount like 0.25 or a dollar value like $100.",
                from.ticker.to_uppercase(),
                from.network,
                destination_context
            ),
        )
        .await
    }

    async fn parse_amount_input(
        &self,
        text: &str,
        from: &CurrencySelection,
        to: Option<&CurrencySelection>,
    ) -> Option<ParsedAmountInput> {
        let deterministic_mode = parse_amount_mode(text, from).or_else(|| {
            if looks_like_usd_amount(text) {
                Some(AmountInputMode::Usd)
            } else {
                None
            }
        });
        let deterministic_amount = match deterministic_mode {
            Some(AmountInputMode::Usd) => parse_usd_amount(text),
            _ => parse_amount(text).or_else(|| parse_amount_from_text(text)),
        };

        if let Some(kimi_result) = self.extract_amount_input_via_kimi(text, from, to).await {
            return Some(kimi_result);
        }

        deterministic_amount.map(|amount| ParsedAmountInput {
            amount,
            mode: deterministic_mode.unwrap_or(AmountInputMode::SourceAsset),
        })
    }

    async fn extract_amount_input_via_kimi(
        &self,
        text: &str,
        from: &CurrencySelection,
        to: Option<&CurrencySelection>,
    ) -> Option<ParsedAmountInput> {
        if is_plain_amount_input(text) {
            return None;
        }

        let kimi = self.state.kimi_client.as_ref()?;
        let destination_ticker = to.map(|asset| asset.ticker.as_str());
        let destination_network = to.map(|asset| asset.network.as_str());

        match kimi
            .extract_amount_with_mode(
                text,
                &from.ticker,
                &from.network,
                destination_ticker,
                destination_network,
            )
            .await
        {
            Ok((Some(amount), mode)) => Some(ParsedAmountInput {
                amount,
                mode: mode
                    .map(AmountInputMode::from)
                    .or_else(|| parse_amount_mode(text, from))
                    .or_else(|| {
                        if looks_like_usd_amount(text) {
                            Some(AmountInputMode::Usd)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(AmountInputMode::SourceAsset),
            }),
            Ok((None, _)) => None,
            Err(error) => {
                tracing::warn!("Kimi contextual amount extraction failed: {}", error);
                None
            }
        }
    }

    async fn extract_quote_selection_via_kimi(
        &self,
        text: &str,
        route_count: usize,
    ) -> Option<usize> {
        let kimi = self.state.kimi_client.as_ref()?;

        match kimi.choose_quote_index(text, route_count).await {
            Ok(index) => index,
            Err(error) => {
                tracing::warn!("Kimi quote selection failed: {}", error);
                None
            }
        }
    }

    async fn extract_address_via_kimi(
        &self,
        text: &str,
        ticker: &str,
        network: &str,
    ) -> Option<String> {
        let kimi = self.state.kimi_client.as_ref()?;

        match kimi.extract_address(text, ticker, network).await {
            Ok(address) => address,
            Err(error) => {
                tracing::warn!("Kimi address extraction failed: {}", error);
                None
            }
        }
    }

    /// Best-effort: rephrases a routine prompt naturally (matching the user's
    /// own language/tone), falling back to the canned copy unchanged if Kimi
    /// isn't configured or the call fails. Only ever used for the ordinary
    /// back-and-forth prompts (asset/amount questions, welcome text) - never
    /// for addresses, confirmations, or quote figures, so a fallback or a
    /// failure never blocks the flow or risks altering a fact.
    async fn narrate_or(&self, situation: &str, fallback: &str) -> String {
        let Some(kimi) = self.state.kimi_client.as_ref() else {
            return fallback.to_string();
        };

        match kimi.narrate(situation).await {
            Ok(text) => text,
            Err(error) => {
                tracing::warn!("Kimi narration failed, using fallback copy: {}", error);
                fallback.to_string()
            }
        }
    }

    async fn resolve_source_amount_from_usd(
        &self,
        from: &CurrencySelection,
        usd_amount: f64,
    ) -> Result<f64, String> {
        if usd_amount <= 0.0 {
            return Err("USD amount must be greater than zero.".to_string());
        }

        let gateway = TrocadorGateway::from_env()
            .map_err(|_| "Trocador pricing is not configured on the backend.".to_string())?;
        let commission_service = CommissionService::new();
        let probe_amounts = [1.0, 0.1, 0.01, 10.0, 100.0];
        let mut last_error = None;

        for probe_amount in probe_amounts {
            match commission_service
                .resolve_live_amount_usd(&gateway, &from.ticker, &from.network, probe_amount)
                .await
            {
                Ok(probe_usd) if probe_usd.is_finite() && probe_usd > 0.0 => {
                    let usd_per_unit = probe_usd / probe_amount;
                    if usd_per_unit.is_finite() && usd_per_unit > 0.0 {
                        return Ok((usd_amount / usd_per_unit).max(0.0));
                    }
                }
                Ok(_) => {}
                Err(error) => last_error = Some(error.to_string()),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            format!(
                "I couldn't convert ${} into {} on {} right now.",
                trim_f64(usd_amount),
                from.ticker.to_uppercase(),
                from.network
            )
        }))
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
            .ok_or_else(|| {
                "I lost the source asset for this swap. Send the swap details again.".to_string()
            })?
            .clone();
        let to = draft
            .to
            .as_ref()
            .ok_or_else(|| {
                "I lost the destination asset for this swap. Send the swap details again."
                    .to_string()
            })?
            .clone();
        let amount = draft
            .amount
            .ok_or_else(|| "I lost the amount for this swap. Send the amount again.".to_string())?;

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
            Err(SwapError::PairNotAvailable) => {
                return self
                    .reply_to_inbound(
                        wa_id,
                        phone_number_id,
                        session_id,
                        inbound_message_id,
                        NO_ROUTE_EXPLANATION,
                    )
                    .await;
            }
            Err(SwapError::AmountOutOfRange { min, max }) => {
                let message = amount_out_of_range_message(&from.ticker, amount, min, max)
                    .unwrap_or_else(|| {
                        "That amount will not work for this pair. Send another amount and I'll try again."
                            .to_string()
                    });
                return self
                    .reply_to_inbound(
                        wa_id,
                        phone_number_id,
                        session_id,
                        inbound_message_id,
                        &message,
                    )
                    .await;
            }
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
            let message = amount_out_of_range_message(
                &from.ticker,
                amount,
                rates.min_deposit,
                rates.max_deposit,
            )
            .unwrap_or_else(|| NO_ROUTE_EXPLANATION.to_string());

            return self
                .reply_to_inbound(
                    wa_id,
                    phone_number_id,
                    session_id,
                    inbound_message_id,
                    &message,
                )
                .await;
        }

        let mut sorted_rates = rates.rates.clone();
        sorted_rates.sort_by_key(|rate| rate.kyc_required);

        let visible_route_count = sorted_rates.len().min(10);

        draft.quotes = sorted_rates
            .iter()
            .take(10)
            .enumerate()
            .map(|(index, rate)| to_quote_choice(index + 1, rate, &rates.trade_id))
            .collect::<Vec<_>>();

        let request_context = if let Some(requested_usd) = draft.requested_amount_usd {
            format!(
                "Requested ${}. Using about {} {} on {}.",
                trim_f64(requested_usd),
                trim_f64(amount),
                from.ticker.to_uppercase(),
                from.network
            )
        } else {
            format!(
                "Requested {} {} on {}.",
                trim_f64(amount),
                from.ticker.to_uppercase(),
                from.network
            )
        };

        draft.selected_quote = draft.quotes.first().cloned();

        if draft.recipient_address.is_some() {
            if to.memo && draft.recipient_extra_id.is_none() {
                WhatsAppCrud::new(self.state.db.clone())
                    .upsert_session_state(
                        wa_id,
                        phone_number_id,
                        &ConversationState::AwaitingRecipientExtraId,
                        locale,
                        &draft,
                        inbound_message_id,
                    )
                    .await
                    .map_err(|error| error.to_string())?;

                return self
                    .reply(
                        wa_id,
                        phone_number_id,
                        session_id,
                        &format!(
                            "The {} destination also needs {}. Reply with it now.",
                            to.ticker.to_uppercase(),
                            to.extra_id_name
                                .clone()
                                .unwrap_or_else(|| "the extra ID".to_string())
                        ),
                    )
                    .await;
            }

            return self
                .prompt_confirmation(
                    wa_id,
                    phone_number_id,
                    session_id,
                    locale,
                    draft,
                    inbound_message_id,
                )
                .await;
        }

        WhatsAppCrud::new(self.state.db.clone())
            .upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::AwaitingRecipientAddress,
                locale,
                &draft,
                inbound_message_id,
            )
            .await
            .map_err(|error| error.to_string())?;

        if let Some(recommended) = draft.selected_quote.as_ref() {
            let body = format!(
                "{} Recommended route: {} for about {} {} on {}. Send your {} on {} receiving address.",
                request_context,
                recommended.provider_name,
                trim_f64(recommended.estimated_amount),
                to.ticker.to_uppercase(),
                to.network,
                to.ticker.to_uppercase(),
                to.network
            );

            return self.reply(wa_id, phone_number_id, session_id, &body).await;
        }

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

        let body = if rates.rates.len() > 10 {
            format!(
                "{} Found {} live routes for {} on {} -> {} on {}. Showing the top 10. Privacy-first sorting is applied, so Privacy A routes appear first.",
                request_context,
                rates.rates.len(),
                from.ticker.to_uppercase(),
                from.network,
                to.ticker.to_uppercase(),
                to.network
            )
        } else {
            format!(
                "{} Found {} live routes for {} on {} -> {} on {}. Showing all {} routes. Privacy-first sorting is applied, so Privacy A routes appear first.",
                request_context,
                rates.rates.len(),
                from.ticker.to_uppercase(),
                from.network,
                to.ticker.to_uppercase(),
                to.network,
                visible_route_count
            )
        };

        self.reply_quote_options(wa_id, phone_number_id, session_id, &body, &draft.quotes)
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

        if let Some(eta_line) = format_eta_line(quote.eta_minutes) {
            lines.push(eta_line);
        }

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

        lines.push("If this looks right, reply confirm. If not, reply cancel.".to_string());
        let body = lines.join("\n");

        self.reply_interactive_buttons_or_fallback(
            wa_id,
            phone_number_id,
            session_id,
            &body,
            vec![
                ReplyButtonOption {
                    id: build_confirmation_selection_id(KimiConfirmation::Confirm).to_string(),
                    title: "Confirm".to_string(),
                },
                ReplyButtonOption {
                    id: build_confirmation_selection_id(KimiConfirmation::Cancel).to_string(),
                    title: "Cancel".to_string(),
                },
            ],
            &body,
        )
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

        if let Some(expires_at) = status.expires_at {
            if let Some(expiry_line) = format_expiry_line("Deposit window left", expires_at) {
                lines.push(expiry_line);
            }
        }

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
            let fallback = format!(
                "That address does not look valid for {} on {}. Send another one.",
                ticker.to_uppercase(),
                network
            );
            Err(self
                .narrate_or(
                    &format!(
                        "Tell the user their address does not look valid for {} on {} and ask them to send another one. Keep it short.",
                        ticker.to_uppercase(),
                        network
                    ),
                    &fallback,
                )
                .await)
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
        let sanitized_input =
            sanitize_asset_phrase(input).unwrap_or_else(|| input.trim().to_string());
        let normalized_input = normalize_phrase(&sanitized_input);
        if normalized_input.is_empty() {
            return Ok(AssetInputPlan::Error(
                "Type a coin ticker or name.".to_string(),
            ));
        }

        let network_aliases = build_network_aliases(catalog);
        let planned = plan_asset_input(catalog, &sanitized_input)?;
        if !matches!(planned, AssetInputPlan::Error(_)) {
            return Ok(planned);
        }

        if split_explicit_network_phrase(&sanitized_input, &network_aliases).is_some() {
            return plan_asset_input(catalog, &sanitized_input);
        }

        let ranked_matches = self
            .search_currency_matches(catalog, &sanitized_input, 250)
            .await?;
        if ranked_matches.is_empty() {
            return plan_asset_input(catalog, &sanitized_input);
        }

        let exact_ranked_matches = ranked_matches
            .iter()
            .filter(|currency| {
                normalize_phrase(&currency.ticker) == normalized_input
                    || normalize_phrase(&currency.name) == normalized_input
            })
            .cloned()
            .collect::<Vec<_>>();

        let ranked_matches = if exact_ranked_matches.is_empty() {
            ranked_matches
        } else {
            exact_ranked_matches
        };

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
            format!(
                "\"{}\" matches multiple options. Choose one.",
                sanitized_input.trim()
            )
        } else if ranked_selections.len() > 10 {
            format!(
                "Top matches for \"{}\". Showing first 10. Choose one or narrow the search.",
                sanitized_input.trim()
            )
        } else {
            format!(
                "Top matches for \"{}\". Choose one.",
                sanitized_input.trim()
            )
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

    fn public_backend_base_url(&self) -> Option<String> {
        std::env::var("PUBLIC_BACKEND_URL")
            .ok()
            .or_else(|| std::env::var("RENDER_EXTERNAL_URL").ok())
            .or_else(|| std::env::var("API_BASE_URL").ok())
            .map(|value| value.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
    }

    fn swap_deposit_qr_link(
        &self,
        phone_number_id: &str,
        wa_id: &str,
        swap_id: &str,
    ) -> Option<String> {
        let base_url = self.public_backend_base_url()?;
        let client_id = derive_whatsapp_client_id(phone_number_id, wa_id);

        Some(format!(
            "{}/whatsapp/qr/{}?client_id={}",
            base_url, swap_id, client_id
        ))
    }

    async fn acknowledge_inbound_message(&self, inbound_message_id: Option<&str>) {
        let Some(message_id) = inbound_message_id else {
            return;
        };

        let Some(service) = self.state.whatsapp_service.as_ref() else {
            return;
        };

        if let Err(error) = service.mark_message_read(message_id).await {
            tracing::warn!(
                "failed to mark WhatsApp message {} as read: {}",
                message_id,
                error
            );
            return;
        }

        if let Err(error) = service.send_typing_indicator(message_id).await {
            tracing::warn!(
                "failed to send WhatsApp typing indicator for {}: {}",
                message_id,
                error
            );
        }
    }

    async fn reply(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
    ) -> Result<(), String> {
        let crud = WhatsAppCrud::new(self.state.db.clone());
        let inferred_inbound_message_id = crud
            .get_last_inbound_message_id(wa_id, phone_number_id)
            .await
            .map_err(|error| error.to_string())?;

        self.send_reply(
            wa_id,
            phone_number_id,
            session_id,
            inferred_inbound_message_id.as_deref(),
            body,
        )
        .await
    }

    async fn send_reply(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        inbound_message_id: Option<&str>,
        body: &str,
    ) -> Result<(), String> {
        let service = self
            .state
            .whatsapp_service
            .as_ref()
            .ok_or_else(|| "WhatsApp is not configured".to_string())?;

        let crud = WhatsAppCrud::new(self.state.db.clone());
        if let Some(inbound_message_id) = inbound_message_id {
            let idempotency_key =
                whatsapp_outbound_idempotency_key(phone_number_id, wa_id, inbound_message_id, body);

            let reservation = crud
                .record_outbound_message_once(
                    session_id,
                    wa_id,
                    phone_number_id,
                    "text",
                    body,
                    &idempotency_key,
                )
                .await
                .map_err(|error| error.to_string())?;

            if !reservation.should_send {
                tracing::info!(
                    "Skipping duplicate WhatsApp reply for inbound message {}",
                    inbound_message_id
                );
                return Ok(());
            }

            return match service.send_text_message(wa_id, body).await {
                Ok(response) => {
                    let provider_message_id =
                        response.messages.first().map(|message| message.id.as_str());
                    crud.mark_outbound_sent(&reservation.id, provider_message_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }
                Err(error) => {
                    let _ = crud
                        .mark_outbound_failed(&reservation.id, &error.to_string())
                        .await;
                    Err(error.to_string())
                }
            };
        }

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

    async fn reply_to_inbound(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        inbound_message_id: Option<&str>,
        body: &str,
    ) -> Result<(), String> {
        self.send_reply(wa_id, phone_number_id, session_id, inbound_message_id, body)
            .await
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

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let inferred_inbound_message_id = crud
            .get_last_inbound_message_id(wa_id, phone_number_id)
            .await
            .map_err(|error| error.to_string())?;
        let body_for_audit = serde_json::json!({
            "image_link": image_link,
            "caption": caption,
        })
        .to_string();

        if let Some(inbound_message_id) = inferred_inbound_message_id.as_deref() {
            let idempotency_key = whatsapp_outbound_idempotency_key(
                phone_number_id,
                wa_id,
                inbound_message_id,
                &body_for_audit,
            );
            let reservation = crud
                .record_outbound_message_once(
                    session_id,
                    wa_id,
                    phone_number_id,
                    "image",
                    &body_for_audit,
                    &idempotency_key,
                )
                .await
                .map_err(|error| error.to_string())?;

            if !reservation.should_send {
                tracing::info!(
                    "Skipping duplicate WhatsApp image reply for inbound message {}",
                    inbound_message_id
                );
                return Ok(());
            }

            return match service.send_image_message(wa_id, image_link, caption).await {
                Ok(response) => {
                    let provider_message_id =
                        response.messages.first().map(|message| message.id.as_str());
                    crud.mark_outbound_sent(&reservation.id, provider_message_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }
                Err(error) => {
                    let _ = crud
                        .mark_outbound_failed(&reservation.id, &error.to_string())
                        .await;
                    Err(error.to_string())
                }
            };
        }

        let outbound_id = crud
            .record_outbound_message(session_id, wa_id, phone_number_id, "image", &body_for_audit)
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

    async fn reply_interactive_list_or_fallback(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        button_label: &str,
        section_title: Option<&str>,
        options: Vec<InteractiveListOption>,
        fallback_body: &str,
    ) -> Result<(), String> {
        if options.is_empty() {
            return self
                .reply(wa_id, phone_number_id, session_id, fallback_body)
                .await;
        }

        let Some(service) = self.state.whatsapp_service.as_ref() else {
            return self
                .reply(wa_id, phone_number_id, session_id, fallback_body)
                .await;
        };

        let options = options
            .into_iter()
            .take(10)
            .map(|option| InteractiveListOption {
                id: truncate_whatsapp_text(&option.id, 200),
                title: truncate_whatsapp_text(&option.title, 24),
                description: option
                    .description
                    .map(|value| truncate_whatsapp_text(&value, 72)),
            })
            .collect::<Vec<_>>();

        let payload_body = serde_json::json!({
            "body": body,
            "button": button_label,
            "section_title": section_title,
            "rows": options.iter().map(|option| serde_json::json!({
                "id": option.id.as_str(),
                "title": option.title.as_str(),
                "description": option.description.as_deref(),
            })).collect::<Vec<_>>(),
        })
        .to_string();

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let inferred_inbound_message_id = crud
            .get_last_inbound_message_id(wa_id, phone_number_id)
            .await
            .map_err(|error| error.to_string())?;
        let sections = vec![InteractiveListSection {
            title: section_title.map(str::to_string),
            rows: options,
        }];

        if let Some(inbound_message_id) = inferred_inbound_message_id.as_deref() {
            let idempotency_key = whatsapp_outbound_idempotency_key(
                phone_number_id,
                wa_id,
                inbound_message_id,
                &payload_body,
            );
            let reservation = crud
                .record_outbound_message_once(
                    session_id,
                    wa_id,
                    phone_number_id,
                    "interactive_list",
                    &payload_body,
                    &idempotency_key,
                )
                .await
                .map_err(|error| error.to_string())?;

            if !reservation.should_send {
                tracing::info!(
                    "Skipping duplicate WhatsApp interactive list for inbound message {}",
                    inbound_message_id
                );
                return Ok(());
            }

            return match service
                .send_interactive_list_message(wa_id, body, button_label, sections)
                .await
            {
                Ok(response) => {
                    let provider_message_id =
                        response.messages.first().map(|message| message.id.as_str());
                    crud.mark_outbound_sent(&reservation.id, provider_message_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }
                Err(error) => {
                    let _ = crud
                        .mark_outbound_failed(&reservation.id, &error.to_string())
                        .await;
                    tracing::warn!(
                        "WhatsApp interactive list failed for {} on {}: {}. Falling back to text.",
                        wa_id,
                        phone_number_id,
                        error
                    );
                    self.reply(wa_id, phone_number_id, session_id, fallback_body)
                        .await
                }
            };
        }

        let outbound_id = crud
            .record_outbound_message(
                session_id,
                wa_id,
                phone_number_id,
                "interactive_list",
                &payload_body,
            )
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
                tracing::warn!(
                    "WhatsApp interactive list failed for {} on {}: {}. Falling back to text.",
                    wa_id,
                    phone_number_id,
                    error
                );
                self.reply(wa_id, phone_number_id, session_id, fallback_body)
                    .await
            }
        }
    }

    async fn reply_interactive_buttons_or_fallback(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        buttons: Vec<ReplyButtonOption>,
        fallback_body: &str,
    ) -> Result<(), String> {
        if buttons.is_empty() {
            return self
                .reply(wa_id, phone_number_id, session_id, fallback_body)
                .await;
        }

        let Some(service) = self.state.whatsapp_service.as_ref() else {
            return self
                .reply(wa_id, phone_number_id, session_id, fallback_body)
                .await;
        };

        let buttons = buttons
            .into_iter()
            .take(3)
            .map(|button| ReplyButtonOption {
                id: truncate_whatsapp_text(&button.id, 200),
                title: truncate_whatsapp_text(&button.title, 20),
            })
            .collect::<Vec<_>>();

        let payload_body = serde_json::json!({
            "body": body,
            "buttons": buttons.iter().map(|button| serde_json::json!({
                "id": button.id.as_str(),
                "title": button.title.as_str(),
            })).collect::<Vec<_>>(),
        })
        .to_string();

        let crud = WhatsAppCrud::new(self.state.db.clone());
        let inferred_inbound_message_id = crud
            .get_last_inbound_message_id(wa_id, phone_number_id)
            .await
            .map_err(|error| error.to_string())?;

        if let Some(inbound_message_id) = inferred_inbound_message_id.as_deref() {
            let idempotency_key = whatsapp_outbound_idempotency_key(
                phone_number_id,
                wa_id,
                inbound_message_id,
                &payload_body,
            );
            let reservation = crud
                .record_outbound_message_once(
                    session_id,
                    wa_id,
                    phone_number_id,
                    "interactive_button",
                    &payload_body,
                    &idempotency_key,
                )
                .await
                .map_err(|error| error.to_string())?;

            if !reservation.should_send {
                tracing::info!(
                    "Skipping duplicate WhatsApp interactive button reply for inbound message {}",
                    inbound_message_id
                );
                return Ok(());
            }

            return match service
                .send_interactive_button_message(wa_id, body, buttons, None)
                .await
            {
                Ok(response) => {
                    let provider_message_id =
                        response.messages.first().map(|message| message.id.as_str());
                    crud.mark_outbound_sent(&reservation.id, provider_message_id)
                        .await
                        .map_err(|error| error.to_string())?;
                    Ok(())
                }
                Err(error) => {
                    let _ = crud
                        .mark_outbound_failed(&reservation.id, &error.to_string())
                        .await;
                    tracing::warn!(
                        "WhatsApp interactive buttons failed for {} on {}: {}. Falling back to text.",
                        wa_id,
                        phone_number_id,
                        error
                    );
                    self.reply(wa_id, phone_number_id, session_id, fallback_body)
                        .await
                }
            };
        }

        let outbound_id = crud
            .record_outbound_message(
                session_id,
                wa_id,
                phone_number_id,
                "interactive_button",
                &payload_body,
            )
            .await
            .map_err(|error| error.to_string())?;

        match service
            .send_interactive_button_message(wa_id, body, buttons, None)
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
                tracing::warn!(
                    "WhatsApp interactive buttons failed for {} on {}: {}. Falling back to text.",
                    wa_id,
                    phone_number_id,
                    error
                );
                self.reply(wa_id, phone_number_id, session_id, fallback_body)
                    .await
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
        let fallback_options = families
            .iter()
            .map(|family| format!("{} ({})", family.name, family.ticker.to_uppercase()))
            .collect::<Vec<_>>();
        let interactive_rows = families
            .iter()
            .map(|family| InteractiveListOption {
                id: build_family_selection_id(family),
                title: truncate_whatsapp_text(&family.name, 24),
                description: Some(truncate_whatsapp_text(&family.ticker.to_uppercase(), 72)),
            })
            .collect::<Vec<_>>();
        let fallback = format_choice_prompt(body, &fallback_options, "Tell me which one you want.");

        if families.len() <= 3 {
            let buttons = families
                .iter()
                .map(|family| ReplyButtonOption {
                    id: build_family_selection_id(family),
                    title: truncate_whatsapp_text(&family.ticker.to_uppercase(), 20),
                })
                .collect::<Vec<_>>();

            return self
                .reply_interactive_buttons_or_fallback(
                    wa_id,
                    phone_number_id,
                    session_id,
                    body,
                    buttons,
                    &fallback,
                )
                .await;
        }

        self.reply_interactive_list_or_fallback(
            wa_id,
            phone_number_id,
            session_id,
            body,
            "Choose asset",
            Some("Matching assets"),
            interactive_rows,
            &fallback,
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
        let fallback_rows = options
            .iter()
            .map(|option| option.network.clone())
            .collect::<Vec<_>>();
        let interactive_rows = options
            .iter()
            .map(|option| InteractiveListOption {
                id: build_asset_selection_id(option),
                title: truncate_whatsapp_text(&option.network, 24),
                description: Some(truncate_whatsapp_text(
                    &format!("{} ({})", option.name, option.ticker.to_uppercase()),
                    72,
                )),
            })
            .collect::<Vec<_>>();
        let fallback = format_choice_prompt(body, &fallback_rows, "Tell me the network you want.");

        if options.len() <= 3 {
            let buttons = options
                .iter()
                .map(|option| ReplyButtonOption {
                    id: build_asset_selection_id(option),
                    title: truncate_whatsapp_text(&option.network, 20),
                })
                .collect::<Vec<_>>();

            return self
                .reply_interactive_buttons_or_fallback(
                    wa_id,
                    phone_number_id,
                    session_id,
                    body,
                    buttons,
                    &fallback,
                )
                .await;
        }

        self.reply_interactive_list_or_fallback(
            wa_id,
            phone_number_id,
            session_id,
            body,
            "Choose network",
            Some("Available networks"),
            interactive_rows,
            &fallback,
        )
        .await
    }

    async fn reply_currency_options(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        _button_label: &str,
        options: &[CurrencySelection],
    ) -> Result<(), String> {
        let fallback_rows = options
            .iter()
            .map(|option| format!("{} on {}", option.ticker.to_uppercase(), option.network))
            .collect::<Vec<_>>();
        let interactive_rows = options
            .iter()
            .map(|option| InteractiveListOption {
                id: build_asset_selection_id(option),
                title: truncate_whatsapp_text(
                    &format!("{} on {}", option.ticker.to_uppercase(), option.network),
                    24,
                ),
                description: Some(truncate_whatsapp_text(&option.name, 72)),
            })
            .collect::<Vec<_>>();
        let fallback =
            format_choice_prompt(body, &fallback_rows, "Tell me the exact one you want.");

        if options.len() <= 3 {
            let buttons = options
                .iter()
                .map(|option| ReplyButtonOption {
                    id: build_asset_selection_id(option),
                    title: truncate_whatsapp_text(&option.ticker.to_uppercase(), 20),
                })
                .collect::<Vec<_>>();

            return self
                .reply_interactive_buttons_or_fallback(
                    wa_id,
                    phone_number_id,
                    session_id,
                    body,
                    buttons,
                    &fallback,
                )
                .await;
        }

        self.reply_interactive_list_or_fallback(
            wa_id,
            phone_number_id,
            session_id,
            body,
            "Choose coin",
            Some("Matching coins"),
            interactive_rows,
            &fallback,
        )
        .await
    }

    async fn reply_quote_options(
        &self,
        wa_id: &str,
        phone_number_id: &str,
        session_id: Option<&str>,
        body: &str,
        quotes: &[QuoteChoice],
    ) -> Result<(), String> {
        let fallback_rows = quotes
            .iter()
            .map(|quote| {
                format!(
                    "{} - {}",
                    quote.provider_name,
                    format_quote_list_description(quote)
                )
            })
            .collect::<Vec<_>>();
        let interactive_rows = quotes
            .iter()
            .map(|quote| InteractiveListOption {
                id: build_quote_selection_id(quote.index),
                title: truncate_whatsapp_text(&quote.provider_name, 24),
                description: Some(truncate_whatsapp_text(
                    &format_quote_list_description(quote),
                    72,
                )),
            })
            .collect::<Vec<_>>();
        let fallback = format_choice_prompt(
            body,
            &fallback_rows,
            "If you want a different route, tell me the provider name.",
        );

        if quotes.len() <= 3 {
            let buttons = quotes
                .iter()
                .map(|quote| ReplyButtonOption {
                    id: build_quote_selection_id(quote.index),
                    title: truncate_whatsapp_text(&quote.provider_name, 20),
                })
                .collect::<Vec<_>>();

            return self
                .reply_interactive_buttons_or_fallback(
                    wa_id,
                    phone_number_id,
                    session_id,
                    body,
                    buttons,
                    &fallback,
                )
                .await;
        }

        self.reply_interactive_list_or_fallback(
            wa_id,
            phone_number_id,
            session_id,
            body,
            "View routes",
            Some("Live routes"),
            interactive_rows,
            &fallback,
        )
        .await
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
        privacy_rating: rate
            .privacy_rating
            .clone()
            .or_else(|| rate.kyc_rating.clone()),
        eta_minutes: rate.eta_minutes,
        trade_id: trade_id.to_string(),
    }
}

fn quote_privacy_label(quote: &QuoteChoice) -> String {
    if let Some(rating) = quote
        .privacy_rating
        .as_deref()
        .map(str::trim)
        .filter(|rating| !rating.is_empty())
    {
        return format!("Privacy {}", rating.to_ascii_uppercase());
    }

    if quote.kyc_required {
        "Privacy ?".to_string()
    } else {
        "Privacy A".to_string()
    }
}

fn format_quote_list_description(quote: &QuoteChoice) -> String {
    let mut parts = vec![
        format!("Recv {}", trim_f64(quote.estimated_amount)),
        quote.rate_type.as_db_str().to_string(),
        quote_privacy_label(quote),
    ];

    if let Some(eta) = format_eta_short(quote.eta_minutes) {
        parts.push(eta);
    }

    parts.join(" | ")
}

fn format_eta_line(eta_minutes: Option<u32>) -> Option<String> {
    eta_minutes.map(|minutes| format!("ETA: {}", humanize_minutes(minutes)))
}

fn format_eta_short(eta_minutes: Option<u32>) -> Option<String> {
    eta_minutes.map(|minutes| humanize_minutes(minutes).replace(' ', ""))
}

fn format_expiry_line(label: &str, expires_at: DateTime<Utc>) -> Option<String> {
    let minutes_remaining = expires_at
        .signed_duration_since(Utc::now())
        .num_minutes()
        .max(0) as u32;

    if minutes_remaining == 0 {
        Some(format!("{}: under 1 min", label))
    } else {
        Some(format!(
            "{}: {}",
            label,
            humanize_minutes(minutes_remaining)
        ))
    }
}

fn humanize_minutes(minutes: u32) -> String {
    if minutes <= 1 {
        return "~1 min".to_string();
    }

    if minutes < 60 {
        return format!("~{} min", minutes);
    }

    let hours = minutes / 60;
    let remainder = minutes % 60;

    if remainder == 0 {
        format!("~{} hr", hours)
    } else {
        format!("~{} hr {} min", hours, remainder)
    }
}

/// Same reasoning Trocador's own site gives for a genuinely unavailable pair
/// (as opposed to an amount that's simply out of bounds) - used instead of a
/// bare "pair not available" so users understand why and what to try next.
const NO_ROUTE_EXPLANATION: &str =
    "I couldn't get a live route for that pair right now. Try another amount or a different pair.";

/// Explains why no routes came back when Trocador's response tells us the
/// requested amount fell outside the pair's min/max deposit bounds, instead of
/// showing a generic "no routes available" message that hides the real reason.
fn amount_out_of_range_message(
    from_ticker: &str,
    amount: f64,
    min_deposit: Option<f64>,
    max_deposit: Option<f64>,
) -> Option<String> {
    let ticker = from_ticker.to_uppercase();

    if let Some(min_deposit) = min_deposit.filter(|value| *value > 0.0) {
        if amount < min_deposit {
            return Some(format!(
                "That amount is below the minimum for this pair. Minimum is {} {}. Reply with a new amount to try again.",
                trim_f64(min_deposit),
                ticker
            ));
        }
    }

    if let Some(max_deposit) = max_deposit.filter(|value| *value > 0.0) {
        if amount > max_deposit {
            return Some(format!(
                "That amount is above the maximum for this pair. Maximum is {} {}. Reply with a new amount to try again.",
                trim_f64(max_deposit),
                ticker
            ));
        }
    }

    None
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

fn parse_status_lookup_input(input: &str) -> Option<String> {
    static UUID_RE: OnceLock<Regex> = OnceLock::new();
    let regex = UUID_RE.get_or_init(|| {
        Regex::new(r"(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
            .expect("valid uuid regex")
    });

    parse_status_command(input).or_else(|| {
        regex
            .find(input.trim())
            .map(|matched| matched.as_str().to_string())
    })
}

fn is_cancel_request(input: &str) -> bool {
    let normalized = normalize_phrase(input);
    if normalized.is_empty() {
        return false;
    }

    if matches!(
        normalized.as_str(),
        "cancel"
            | "restart"
            | "reset"
            | "stop"
            | "abort"
            | "not now"
            | "nevermind"
            | "never mind"
            | "forget it"
            | "forget about it"
            | "leave it"
            | "drop it"
    ) {
        return true;
    }

    if normalized.contains("dont cancel") || normalized.contains("do not cancel") {
        return false;
    }

    let has_cancel_signal = normalized.contains("cancel")
        || normalized.contains("restart")
        || normalized.contains("reset")
        || normalized.contains("abort")
        || normalized.contains("never mind")
        || normalized.contains("forget it")
        || normalized.contains("forget about it")
        || normalized.contains("stop this")
        || normalized.contains("end this")
        || normalized.contains("close this")
        || normalized.contains("leave this")
        || normalized.contains("leave am")
        || normalized.contains("drop this")
        || normalized.contains("change my mind");

    if !has_cancel_signal {
        return false;
    }

    normalized.contains("conversation")
        || normalized.contains("swap")
        || normalized.contains("trade")
        || normalized.contains("setup")
        || normalized.contains("request")
        || normalized.contains("this")
        || normalized.contains("am")
        || normalized.contains("it")
        || normalized.contains("that")
        || normalized == "stop"
        || normalized == "abort"
}

fn normalize_optional_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn sanitize_asset_phrase(value: &str) -> Option<String> {
    let mut normalized = normalize_phrase(value);
    if normalized.is_empty() {
        return None;
    }

    for prefix in [
        "no i said ",
        "i said ",
        "i said i want to buy ",
        "i said i want to buy some ",
        "i said i want to send ",
        "i said i need ",
        "i said i need some ",
        "i said i want ",
        "i need to buy some ",
        "i need to buy ",
        "i need to get some ",
        "i need to get ",
        "i need to receive ",
        "i need some ",
        "i need ",
        "i want to buy some ",
        "i want to buy ",
        "i want to get ",
        "i want to get some ",
        "i want to receive ",
        "i want to send ",
        "i want some ",
        "i want ",
        "need some ",
        "need ",
        "im buying ",
        "i am buying ",
        "im sending ",
        "i am sending ",
        "buy ",
        "get ",
        "receive ",
        "sending ",
        "send ",
    ] {
        if let Some(stripped) = normalized.strip_prefix(prefix) {
            normalized = stripped.trim().to_string();
            break;
        }
    }

    loop {
        let next = normalized.split_whitespace().collect::<Vec<_>>();
        if next.len() <= 1 {
            break;
        }

        let first = next[0];
        if matches!(first, "some" | "a" | "an" | "the" | "just") {
            normalized = next[1..].join(" ");
            continue;
        }

        break;
    }

    if let Some((before_not, _)) = normalized.split_once(" not ") {
        let trimmed_before_not = before_not.trim();
        if !trimmed_before_not.is_empty() {
            normalized = trimmed_before_not.to_string();
        }
    }

    loop {
        let next = normalized.split_whitespace().collect::<Vec<_>>();
        if next.is_empty() {
            return None;
        }

        let last = *next.last().unwrap_or(&"");
        if matches!(
            last,
            "man" | "bro" | "bros" | "bruh" | "baba" | "please" | "pls" | "abeg" | "nah"
        ) {
            normalized = next[..next.len() - 1].join(" ");
            continue;
        }
        break;
    }

    if normalized.is_empty() {
        return None;
    }

    Some(normalized)
}

fn meaningful_asset_phrase(value: Option<&str>) -> Option<String> {
    let value = normalize_optional_text(value)?;
    let sanitized = sanitize_asset_phrase(value)?;

    if matches!(
        sanitized.as_str(),
        "crypto"
            | "cryptocurrency"
            | "coin"
            | "coins"
            | "token"
            | "tokens"
            | "some crypto"
            | "some cryptocurrency"
            | "some coin"
            | "some coins"
            | "some token"
            | "some tokens"
            | "any crypto"
            | "any cryptocurrency"
            | "any coin"
            | "any coins"
            | "any token"
            | "any tokens"
            | "i want to buy"
            | "i want to send"
            | "i want"
            | "i need to buy"
            | "i need to get"
            | "i need to receive"
            | "i need"
            | "buy"
            | "need"
            | "send"
            | "receive"
            | "get"
            | "swap"
            | "trade"
            | "convert"
            | "change"
    ) {
        return None;
    }

    Some(sanitized)
}

#[cfg(test)]
fn looks_like_asset_correction(value: &str) -> bool {
    let normalized = normalize_phrase(value);
    if normalized.is_empty() {
        return false;
    }

    let tokens = normalized.split_whitespace().collect::<Vec<_>>();
    if tokens.len() > 6 {
        return false;
    }

    !tokens.iter().any(|token| {
        matches!(
            *token,
            "check"
                | "request"
                | "requests"
                | "properly"
                | "working"
                | "wrong"
                | "error"
                | "issue"
                | "problem"
                | "because"
                | "why"
        )
    })
}

fn is_generic_swap_start(input: &str) -> bool {
    let normalized = normalize_phrase(input);
    if matches!(
        normalized.as_str(),
        "swap"
            | "start swap"
            | "i want to swap"
            | "i want swap"
            | "i want to make a swap"
            | "i want to swap crypto"
            | "i want to swap some crypto"
            | "want to swap crypto"
            | "want to swap some crypto"
            | "swap crypto"
            | "swap some crypto"
            | "swap coins"
            | "swap tokens"
    ) {
        return true;
    }

    normalized.contains("swap")
        && (normalized.contains("some crypto")
            || normalized.contains("any crypto")
            || normalized.ends_with(" crypto")
            || normalized.ends_with(" coins")
            || normalized.ends_with(" tokens"))
        && !normalized
            .chars()
            .any(|character| character.is_ascii_digit())
        && !normalized.contains(" to ")
        && !normalized.contains(" for ")
}

fn is_show_routes_command(input: &str) -> bool {
    matches!(
        normalize_phrase(input).as_str(),
        "routes"
            | "show routes"
            | "show providers"
            | "providers"
            | "compare"
            | "compare providers"
            | "quotes"
            | "show quotes"
            | "choose provider"
            | "pick provider"
    )
}

fn parse_quote_selection(input: &str) -> Option<usize> {
    let trimmed = input.trim();
    if let Ok(index) = trimmed.parse::<usize>() {
        return Some(index);
    }

    match normalize_phrase(trimmed).as_str() {
        "first"
        | "first one"
        | "one"
        | "1st"
        | "route one"
        | "routeone"
        | "option one"
        | "optionone"
        | "top"
        | "best"
        | "recommended"
        | "use first"
        | "usefirst"
        | "use top"
        | "usetop"
        | "use best"
        | "usebest"
        | "the recommended one"
        | "therecommendedone" => Some(1),
        "second" | "second one" | "two" | "2nd" | "route two" | "routetwo" | "option two"
        | "optiontwo" => Some(2),
        "third" | "third one" | "three" | "3rd" | "route three" | "routethree" | "option three"
        | "optionthree" => Some(3),
        "fourth" | "fourth one" | "four" | "4th" | "route four" | "routefour" | "option four"
        | "optionfour" => Some(4),
        "fifth" | "fifth one" | "five" | "5th" | "route five" | "routefive" | "option five"
        | "optionfive" => Some(5),
        _ => None,
    }
}

fn parse_quote_selection_id(input: &str) -> Option<usize> {
    input
        .trim()
        .to_ascii_lowercase()
        .strip_prefix("quote:")
        .and_then(|value| value.parse::<usize>().ok())
}

fn build_quote_selection_id(index: usize) -> String {
    format!("quote:{}", index)
}

fn build_confirmation_selection_id(decision: KimiConfirmation) -> &'static str {
    match decision {
        KimiConfirmation::Confirm => "confirm:confirm",
        KimiConfirmation::Cancel => "confirm:cancel",
    }
}

fn parse_quote_selection_by_provider(input: &str, quotes: &[QuoteChoice]) -> Option<usize> {
    let normalized = normalize_phrase(input);
    if normalized.is_empty() {
        return None;
    }

    quotes.iter().find_map(|quote| {
        let provider = normalize_phrase(&quote.provider);
        let provider_name = normalize_phrase(&quote.provider_name);

        let matches_provider =
            !provider.is_empty() && (normalized == provider || normalized.contains(&provider));
        let matches_provider_name = !provider_name.is_empty()
            && (normalized == provider_name || normalized.contains(&provider_name));

        (matches_provider || matches_provider_name).then_some(quote.index)
    })
}

fn parse_confirmation_decision(input: &str) -> Option<KimiConfirmation> {
    if is_cancel_request(input) {
        return Some(KimiConfirmation::Cancel);
    }

    match normalize_phrase(input).as_str() {
        "confirm confirm" | "confirmconfirm" => Some(KimiConfirmation::Confirm),
        "confirm cancel" | "confirmcancel" => Some(KimiConfirmation::Cancel),
        "confirm" | "yes" | "yes please" | "yesplease" | "y" | "yeah" | "yep" | "sure" | "ok"
        | "okay" | "proceed" | "goahead" | "createit" | "doit" | "sendit" | "letsgo"
        | "continue" | "looks good" | "looksgood" | "all good" | "allgood" | "thats fine"
        | "thatsfine" => Some(KimiConfirmation::Confirm),
        "no" | "n" | "nope" | "nah" | "cancelit" | "notnow" | "dont" | "donot" | "dont do it"
        | "dontdoit" | "wait" => Some(KimiConfirmation::Cancel),
        _ => None,
    }
}

fn parse_amount_mode(input: &str, from: &CurrencySelection) -> Option<AmountInputMode> {
    let normalized = normalize_phrase(input);
    let from_ticker = normalize_phrase(&from.ticker);
    let from_network = normalize_phrase(&from.network);

    match normalized.as_str() {
        "from" | "source" | "coin" | "token" | "asset" | "fromamount" | "sourceamount" => {
            Some(AmountInputMode::SourceAsset)
        }
        "usd" | "dollar" | "dollars" | "cash" | "$" => Some(AmountInputMode::Usd),
        _ if normalized == from_ticker
            || normalized == format!("from{}", from_ticker)
            || normalized == format!("{}amount", from_ticker)
            || normalized == from_network =>
        {
            Some(AmountInputMode::SourceAsset)
        }
        _ => None,
    }
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

fn parse_amount_from_text(input: &str) -> Option<f64> {
    static AMOUNT_IN_TEXT_RE: OnceLock<Regex> = OnceLock::new();
    let regex = AMOUNT_IN_TEXT_RE.get_or_init(|| {
        Regex::new(r"\d+(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?").expect("valid amount regex")
    });

    regex.find(input).and_then(|matched| {
        let value = matched.as_str().replace(',', "");
        value.parse::<f64>().ok().filter(|amount| *amount > 0.0)
    })
}

fn is_plain_amount_input(input: &str) -> bool {
    parse_amount(input).is_some()
}

fn looks_like_usd_amount(input: &str) -> bool {
    if input.contains('$') {
        return true;
    }

    normalize_phrase(input).split_whitespace().any(|token| {
        matches!(
            token,
            "usd" | "dollar" | "dollars" | "buck" | "bucks" | "cash"
        )
    })
}

fn parse_usd_amount(input: &str) -> Option<f64> {
    if !looks_like_usd_amount(input) {
        return parse_amount(input);
    }

    parse_amount_from_text(input)
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

fn pending_family_options(draft: &SwapDraft, side: AssetSide) -> &[AssetFamilySelection] {
    match side {
        AssetSide::From => &draft.pending_from_family_options,
        AssetSide::To => &draft.pending_to_family_options,
    }
}

fn pending_currency_options(draft: &SwapDraft, side: AssetSide) -> &[CurrencySelection] {
    match side {
        AssetSide::From => &draft.pending_from_currency_options,
        AssetSide::To => &draft.pending_to_currency_options,
    }
}

fn set_pending_family(draft: &mut SwapDraft, side: AssetSide, value: Option<AssetFamilySelection>) {
    match side {
        AssetSide::From => draft.pending_from_family = value,
        AssetSide::To => draft.pending_to_family = value,
    }
}

fn set_pending_family_options(
    draft: &mut SwapDraft,
    side: AssetSide,
    value: Vec<AssetFamilySelection>,
) {
    match side {
        AssetSide::From => draft.pending_from_family_options = value,
        AssetSide::To => draft.pending_to_family_options = value,
    }
}

fn set_pending_currency_options(
    draft: &mut SwapDraft,
    side: AssetSide,
    value: Vec<CurrencySelection>,
) {
    match side {
        AssetSide::From => draft.pending_from_currency_options = value,
        AssetSide::To => draft.pending_to_currency_options = value,
    }
}

fn format_choice_prompt(body: &str, options: &[String], hint: &str) -> String {
    if options.is_empty() {
        return body.to_string();
    }

    let choices = options
        .iter()
        .map(|option| format!("- {}", option))
        .collect::<Vec<_>>()
        .join("\n");

    format!("{}\n\n{}\n\n{}", body, choices, hint)
}

fn summarize_quote_providers(quotes: &[QuoteChoice]) -> String {
    let mut providers = quotes
        .iter()
        .map(|quote| quote.provider_name.trim())
        .filter(|name| !name.is_empty())
        .fold(Vec::<String>::new(), |mut acc, name| {
            if !acc
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(name))
            {
                acc.push(name.to_string());
            }
            acc
        });

    if providers.is_empty() {
        return "no other providers right now".to_string();
    }

    if providers.len() == 1 {
        return providers.remove(0);
    }

    if providers.len() == 2 {
        return format!("{} and {}", providers[0], providers[1]);
    }

    let last = providers.pop().unwrap_or_default();
    format!("{}, and {}", providers.join(", "), last)
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
            name: display_asset_family_name(&option.name),
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
        let name = normalized_asset_family_name(&currency.name).replace(' ', "_");

        (ticker == family_key.ticker && name == family_key.name).then(|| AssetFamilySelection {
            ticker: currency.ticker.clone(),
            name: display_asset_family_name(&currency.name),
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
                && normalized_asset_family_name(&currency.name)
                    == normalized_asset_family_name(&family.name)
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

fn display_asset_family_name(name: &str) -> String {
    let mut current = name.trim().to_string();

    loop {
        let trimmed = current.trim_end();
        if !trimmed.ends_with(')') {
            break;
        }

        let Some(open_index) = trimmed.rfind('(') else {
            break;
        };

        if open_index == 0 {
            break;
        }

        let inner = trimmed[open_index + 1..trimmed.len() - 1].trim();
        if inner.is_empty() {
            break;
        }

        current = trimmed[..open_index].trim_end().to_string();
    }

    if current.is_empty() {
        name.trim().to_string()
    } else {
        current
    }
}

fn normalized_asset_family_name(name: &str) -> String {
    normalize_phrase(&display_asset_family_name(name))
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
        amount_mode: Some(AmountInputMode::SourceAsset),
        from_phrase: captures
            .name("from")
            .map(|value| value.as_str().trim().to_string()),
        to_phrase: captures
            .name("to")
            .map(|value| value.as_str().trim().to_string()),
        recipient_address: None,
        refund_address: None,
    })
}

#[cfg(test)]
fn parse_partial_swap_intent(input: &str) -> Option<ParsedSwapIntent> {
    let normalized = normalize_phrase(input);
    if normalized.is_empty()
        || is_generic_swap_start(input)
        || is_cancel_request(input)
        || parse_status_lookup_input(input).is_some()
    {
        return None;
    }

    let has_swap_signal = normalized.contains("swap")
        || normalized.contains("trade")
        || normalized.contains("convert")
        || normalized.contains("change")
        || normalized.contains("buy")
        || normalized.contains("receive")
        || normalized.contains("get")
        || normalized.contains("need")
        || normalized.contains("want")
        || normalized.contains("send")
        || normalized.contains("sell")
        || normalized.contains("cash out")
        || normalized.contains("cashout");

    if !has_swap_signal {
        return None;
    }

    let phrase = meaningful_asset_phrase(Some(input))?;
    Some(ParsedSwapIntent {
        amount: None,
        amount_mode: None,
        from_phrase: Some(phrase),
        to_phrase: None,
        recipient_address: None,
        refund_address: None,
    })
}

#[cfg(test)]
fn rebalance_intent_asset_sides(raw_text: &str, mut intent: ParsedSwapIntent) -> ParsedSwapIntent {
    let normalized = normalize_phrase(raw_text);
    if normalized.is_empty() {
        return intent;
    }

    let prefers_destination = message_prefers_destination_asset(&normalized);
    let prefers_source = message_prefers_source_asset(&normalized);

    if intent.to_phrase.is_none()
        && intent.from_phrase.is_some()
        && prefers_destination
        && !prefers_source
    {
        intent.to_phrase = intent.from_phrase.take();
    } else if intent.from_phrase.is_none()
        && intent.to_phrase.is_some()
        && prefers_source
        && !prefers_destination
    {
        intent.from_phrase = intent.to_phrase.take();
    }

    intent
}

#[cfg(test)]
fn message_prefers_destination_asset(normalized_text: &str) -> bool {
    normalized_text.contains("buy")
        || normalized_text.contains("receive")
        || normalized_text.contains("get")
        || normalized_text.contains("need")
        || normalized_text.contains("want")
        || normalized_text.contains("cash out")
        || normalized_text.contains("cashout")
}

#[cfg(test)]
fn message_prefers_source_asset(normalized_text: &str) -> bool {
    normalized_text.contains("send")
        || normalized_text.contains("selling")
        || normalized_text.contains("sell")
        || normalized_text.contains("swap from")
        || normalized_text.contains("from ")
        || normalized_text.contains("use ")
        || normalized_text.starts_with("use ")
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

fn build_scoped_network_aliases(options: &[CurrencySelection]) -> HashMap<String, String> {
    let mut aliases = HashMap::new();
    let available_networks = options
        .iter()
        .map(|option| option.network.clone())
        .collect::<Vec<_>>();

    for network in &available_networks {
        aliases
            .entry(normalize_phrase(network))
            .or_insert_with(|| network.clone());
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
        if available_networks
            .iter()
            .any(|network| network.eq_ignore_ascii_case(canonical))
        {
            aliases
                .entry(normalize_phrase(alias))
                .or_insert_with(|| canonical.to_string());
        }
    }

    aliases
}

fn normalized_phrase_contains_alias(normalized_phrase: &str, alias: &str) -> bool {
    normalized_phrase == alias
        || normalized_phrase.starts_with(&format!("{} ", alias))
        || normalized_phrase.ends_with(&format!(" {}", alias))
        || normalized_phrase.contains(&format!(" {} ", alias))
}

fn match_pending_network_option(
    options: &[CurrencySelection],
    input: &str,
) -> Option<CurrencySelection> {
    let sanitized = sanitize_asset_phrase(input).unwrap_or_else(|| input.trim().to_string());
    let normalized_input = normalize_phrase(&sanitized);
    if normalized_input.is_empty() || options.is_empty() {
        return None;
    }

    let aliases = build_scoped_network_aliases(options);
    let mut matched_networks = aliases
        .iter()
        .filter_map(|(alias, canonical)| {
            normalized_phrase_contains_alias(&normalized_input, alias).then_some(canonical.clone())
        })
        .collect::<Vec<_>>();

    matched_networks.sort();
    matched_networks.dedup();

    if matched_networks.len() != 1 {
        return None;
    }

    let canonical = matched_networks.pop()?;
    let mut matches = options
        .iter()
        .filter(|option| option.network.eq_ignore_ascii_case(&canonical))
        .cloned()
        .collect::<Vec<_>>();

    if matches.len() == 1 {
        return matches.pop();
    }

    None
}

fn split_explicit_network_phrase(
    phrase: &str,
    network_aliases: &HashMap<String, String>,
) -> Option<(String, String)> {
    static ASSET_ON_NETWORK_RE: OnceLock<Regex> = OnceLock::new();
    let regex = ASSET_ON_NETWORK_RE.get_or_init(|| {
        Regex::new(r"(?i)^(?P<asset>.+?)\s+(?:on|in)\s+(?P<network>.+)$")
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
        if let Some(mainnet_currency) = top.iter().find(|currency| {
            currency.network.eq_ignore_ascii_case("Mainnet")
                || currency.network.eq_ignore_ascii_case("MAINNET")
                || normalize_phrase(&currency.name)
                    .split_whitespace()
                    .any(|token| token == "mainnet")
        }) {
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

fn whatsapp_outbound_idempotency_key(
    phone_number_id: &str,
    wa_id: &str,
    inbound_message_id: &str,
    body: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(phone_number_id.as_bytes());
    hasher.update(b":");
    hasher.update(wa_id.as_bytes());
    hasher.update(b":");
    hasher.update(inbound_message_id.as_bytes());
    hasher.update(b":");
    hasher.update(body.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::{
        amount_out_of_range_message, is_cancel_request, is_generic_swap_start,
        is_plain_amount_input, looks_like_asset_correction, match_pending_network_option,
        meaningful_asset_phrase, parse_amount_from_text, parse_amount_mode,
        parse_asset_selection_id, parse_confirmation_decision, parse_partial_swap_intent,
        parse_quote_selection, parse_usd_amount, plan_asset_input, rebalance_intent_asset_sides,
        resolve_currency_phrase, sanitize_asset_phrase,
        should_restart_asset_search_from_network_choice, AmountInputMode, AssetFamilySelection,
        AssetInputPlan, CurrencySelection, ParsedSwapIntent,
    };
    use crate::modules::swap::schema::CurrencyResponse;
    use crate::services::kimi::KimiConfirmation;

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
            currency("Ethereum (Mainnet)", "eth", "ERC20"),
            currency("USDC (ERC20)", "usdc", "ERC20"),
            currency("USDC (Arbitrum One)", "usdc", "Arbitrum"),
            currency("Bitcoin (Mainnet)", "btc", "Mainnet"),
        ]
    }

    fn stellar_like_catalog() -> Vec<CurrencyResponse> {
        vec![
            currency("Stellar", "xlm", "Mainnet"),
            currency("Stellar", "xlm", "BEP20"),
            currency("Wirex Token", "wxt", "XLM"),
            currency("USD Coin", "usdc", "XLM"),
            currency("MANTRA", "man", "Mainnet"),
        ]
    }

    fn selection(name: &str, ticker: &str, network: &str) -> CurrencySelection {
        CurrencySelection {
            ticker: ticker.to_string(),
            name: name.to_string(),
            network: network.to_string(),
            memo: false,
            extra_id_name: None,
        }
    }

    #[test]
    fn generic_swap_start_catches_vague_crypto_requests_only() {
        assert!(is_generic_swap_start("swap"));
        assert!(is_generic_swap_start("i want to swap some crypto man"));
        assert!(is_generic_swap_start("swap tokens"));

        assert!(!is_generic_swap_start("swap 100 usdt to xmr"));
        assert!(!is_generic_swap_start("swap btc for xmr"));
    }

    #[test]
    fn meaningful_asset_phrase_ignores_generic_crypto_words() {
        assert_eq!(meaningful_asset_phrase(Some("crypto")), None);
        assert_eq!(meaningful_asset_phrase(Some("some crypto")), None);
        assert_eq!(meaningful_asset_phrase(Some("any token")), None);

        assert_eq!(
            meaningful_asset_phrase(Some("usdt")),
            Some("usdt".to_string())
        );
        assert_eq!(
            meaningful_asset_phrase(Some("xmr mainnet")),
            Some("xmr mainnet".to_string())
        );
    }

    #[test]
    fn sanitizes_casual_asset_phrases() {
        assert_eq!(
            sanitize_asset_phrase("i said i want to buy btc"),
            Some("btc".to_string())
        );
        assert_eq!(
            sanitize_asset_phrase("i need some eth on base"),
            Some("eth on base".to_string())
        );
        assert_eq!(
            sanitize_asset_phrase("i am sending xlm on mainnet man"),
            Some("xlm on mainnet".to_string())
        );
        assert_eq!(
            sanitize_asset_phrase("i want to buy some eth in mainnet"),
            Some("eth in mainnet".to_string())
        );
        assert_eq!(
            sanitize_asset_phrase("i said xlm on mainnet not on man"),
            Some("xlm on mainnet".to_string())
        );
    }

    #[test]
    fn rebalances_buy_intent_into_destination_side() {
        let intent = ParsedSwapIntent {
            amount: None,
            amount_mode: None,
            from_phrase: Some("eth in mainnet".to_string()),
            to_phrase: None,
            recipient_address: None,
            refund_address: None,
        };

        let rebalanced = rebalance_intent_asset_sides("i want to buy some eth in mainnet", intent);

        assert_eq!(rebalanced.from_phrase, None);
        assert_eq!(rebalanced.to_phrase, Some("eth in mainnet".to_string()));
    }

    #[test]
    fn rebalances_need_intent_into_destination_side() {
        let intent = ParsedSwapIntent {
            amount: None,
            amount_mode: None,
            from_phrase: Some("eth on base".to_string()),
            to_phrase: None,
            recipient_address: None,
            refund_address: None,
        };

        let rebalanced = rebalance_intent_asset_sides("i need some eth on base", intent);

        assert_eq!(rebalanced.from_phrase, None);
        assert_eq!(rebalanced.to_phrase, Some("eth on base".to_string()));
    }

    #[test]
    fn parses_partial_swap_intent_for_destination_asset_requests() {
        let intent = parse_partial_swap_intent("i need some eth on base")
            .expect("partial destination intent");
        let rebalanced = rebalance_intent_asset_sides("i need some eth on base", intent);

        assert_eq!(rebalanced.from_phrase, None);
        assert_eq!(rebalanced.to_phrase, Some("eth on base".to_string()));
    }

    #[test]
    fn parses_partial_swap_intent_for_source_asset_requests() {
        let intent = parse_partial_swap_intent("i want to send xlm on mainnet man")
            .expect("partial source intent");
        let rebalanced = rebalance_intent_asset_sides("i want to send xlm on mainnet man", intent);

        assert_eq!(rebalanced.from_phrase, Some("xlm on mainnet".to_string()));
        assert_eq!(rebalanced.to_phrase, None);
    }

    #[test]
    fn long_complaints_do_not_trigger_asset_correction() {
        assert!(!looks_like_asset_correction(
            "it will work man check very well the request are not being sent properly"
        ));
        assert!(looks_like_asset_correction("xlm on mainnet"));
    }

    #[test]
    fn parses_confirmation_button_ids() {
        assert_eq!(
            parse_confirmation_decision("confirm:confirm"),
            Some(KimiConfirmation::Confirm)
        );
        assert_eq!(
            parse_confirmation_decision("confirm:cancel"),
            Some(KimiConfirmation::Cancel)
        );
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
        assert_eq!(selected.network, "ERC20");
    }

    #[test]
    fn resolves_bitcoin_name_to_mainnet_btc() {
        let resolution = resolve_currency_phrase(&sample_catalog(), "bitcoin").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "btc");
        assert_eq!(selected.network, "Mainnet");
    }

    #[test]
    fn plain_xlm_prefers_native_stellar_not_tokens_on_xlm_network() {
        let resolution =
            resolve_currency_phrase(&stellar_like_catalog(), "xlm").expect("resolution");
        let selected = resolution.selected.expect("selected asset");

        assert_eq!(selected.ticker, "xlm");
        assert_eq!(selected.network, "Mainnet");
    }

    #[test]
    fn matches_network_choice_from_freeform_phrase() {
        let options = vec![
            selection("Ethereum", "eth", "Arbitrum"),
            selection("Ethereum", "eth", "Base"),
            selection("Ethereum", "eth", "Mainnet"),
        ];

        let selected =
            match_pending_network_option(&options, "Ethereum mainnet eth, that what I need here")
                .expect("network selection");

        assert_eq!(selected.ticker, "eth");
        assert_eq!(selected.network, "Mainnet");
    }

    #[test]
    fn decorated_network_names_collapse_into_one_family_choice() {
        let plan = plan_asset_input(&sample_catalog(), "usdc").expect("plan");

        match plan {
            AssetInputPlan::ChooseNetwork { family, options } => {
                assert_eq!(family.ticker, "usdc");
                assert_eq!(family.name, "USDC");
                assert_eq!(options.len(), 2);
                assert!(options.iter().any(|option| option.network == "ERC20"));
                assert!(options.iter().any(|option| option.network == "Arbitrum"));
            }
            other => panic!("expected network choice, got {:?}", other),
        }
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

    #[test]
    fn parses_usd_amount_inputs() {
        assert_eq!(parse_usd_amount("$1000"), Some(1000.0));
        assert_eq!(parse_usd_amount("1,250 usd"), Some(1250.0));
        assert_eq!(parse_usd_amount("USD 50"), Some(50.0));
    }

    #[test]
    fn parses_natural_amount_phrases() {
        assert_eq!(parse_amount_from_text("I want 10 eth"), Some(10.0));
        assert_eq!(parse_amount_from_text("na 10 eth me want"), Some(10.0));
        assert_eq!(parse_amount_from_text("I am sending 0.1 BTC"), Some(0.1));
    }

    #[test]
    fn only_bare_numeric_values_skip_contextual_kimi_amount_parser() {
        assert!(is_plain_amount_input("10"));
        assert!(is_plain_amount_input("0.25"));
        assert!(!is_plain_amount_input("I want 10 eth"));
        assert!(!is_plain_amount_input("$100"));
    }

    #[test]
    fn parses_amount_mode_from_source_aliases() {
        let from = selection("Bitcoin", "btc", "Mainnet");

        assert_eq!(
            parse_amount_mode("from", &from),
            Some(AmountInputMode::SourceAsset)
        );
        assert_eq!(
            parse_amount_mode("btc", &from),
            Some(AmountInputMode::SourceAsset)
        );
        assert_eq!(parse_amount_mode("usd", &from), Some(AmountInputMode::Usd));
    }

    #[test]
    fn parses_natural_quote_selection_aliases() {
        assert_eq!(parse_quote_selection("first one"), Some(1));
        assert_eq!(parse_quote_selection("use best"), Some(1));
        assert_eq!(parse_quote_selection("second"), Some(2));
        assert_eq!(parse_quote_selection("route five"), Some(5));
    }

    #[test]
    fn parses_confirmation_and_cancel_aliases() {
        assert_eq!(
            parse_confirmation_decision("looks good"),
            Some(KimiConfirmation::Confirm)
        );
        assert_eq!(
            parse_confirmation_decision("yes please"),
            Some(KimiConfirmation::Confirm)
        );
        assert_eq!(
            parse_confirmation_decision("never mind"),
            Some(KimiConfirmation::Cancel)
        );
        assert_eq!(
            parse_confirmation_decision("wait"),
            Some(KimiConfirmation::Cancel)
        );
        assert_eq!(parse_confirmation_decision("what is the rate again?"), None);
    }

    #[test]
    fn detects_freeform_cancel_requests() {
        assert!(is_cancel_request("baba i wan cancel this conversation"));
        assert!(is_cancel_request("never mind this swap"));
        assert!(is_cancel_request("forget about it"));
        assert!(!is_cancel_request("do not cancel this"));
        assert!(!is_cancel_request("what happens if i cancel later?"));
    }

    #[test]
    fn flags_amount_below_minimum_deposit() {
        let message = amount_out_of_range_message("xlm", 24.0, Some(21.902860 * 2.0), None)
            .expect("amount below minimum should produce a message");

        assert!(message.contains("below the minimum"));
        assert!(message.contains("XLM"));
    }

    #[test]
    fn flags_amount_above_maximum_deposit() {
        let message = amount_out_of_range_message("xlm", 5000.0, None, Some(1000.0))
            .expect("amount above maximum should produce a message");

        assert!(message.contains("above the maximum"));
    }

    #[test]
    fn no_message_when_amount_is_within_bounds() {
        assert_eq!(
            amount_out_of_range_message("xlm", 50.0, Some(20.0), Some(1000.0)),
            None
        );
    }

    #[test]
    fn no_message_when_bounds_are_unknown() {
        assert_eq!(amount_out_of_range_message("xlm", 50.0, None, None), None);
    }
}
