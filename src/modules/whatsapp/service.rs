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
use crate::services::whatsapp::derive_whatsapp_client_id;
use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ConversationState {
    Idle,
    AwaitingFromAsset,
    AwaitingToAsset,
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
            "awaiting_from_asset" => Self::AwaitingFromAsset,
            "awaiting_to_asset" => Self::AwaitingToAsset,
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

#[derive(Debug)]
struct AssetResolution {
    selected: Option<CurrencySelection>,
    ambiguous_options: Vec<CurrencySelection>,
    error: Option<String>,
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
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return self
                .reply(wa_id, phone_number_id, None, "Send a text message like `swap 100 usdc on stellar to bitcoin`, `swap`, or `status <swap_id>`.")
                .await;
        }

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
                    "Swap flow reset. Send `swap` to start again or `swap 100 usdc on stellar to bitcoin`.",
                )
                .await;
        }

        if lowered == "help" || lowered == "menu" || lowered == "start" {
            crud.upsert_session_state(
                wa_id,
                phone_number_id,
                &ConversationState::Idle,
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
                    &Self::help_message(),
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
                        &ConversationState::AwaitingFromAsset,
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
                            "What are you sending? Example: `usdc on stellar`, `btc mainnet`, or `eth on arbitrum`.",
                        )
                        .await;
                }

                if let Some(intent) = parse_swap_intent(trimmed) {
                    if let Some(amount) = intent.amount {
                        draft.amount = Some(amount);
                    }

                    let catalog = self.fetch_currency_catalog().await?;

                    if let Some(from_phrase) = intent.from_phrase {
                        match resolve_currency_phrase(&catalog, &from_phrase)? {
                            AssetResolution {
                                selected: Some(selection),
                                ..
                            } => draft.from = Some(selection),
                            AssetResolution {
                                ambiguous_options,
                                error,
                                ..
                            } => {
                                let message = error.unwrap_or_else(|| {
                                    format_ambiguity_message(
                                        "sending",
                                        &ambiguous_options,
                                        "Reply with the asset and network more precisely.",
                                    )
                                });
                                return self
                                    .reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                                    .await;
                            }
                        }
                    }

                    if let Some(to_phrase) = intent.to_phrase {
                        match resolve_currency_phrase(&catalog, &to_phrase)? {
                            AssetResolution {
                                selected: Some(selection),
                                ..
                            } => draft.to = Some(selection),
                            AssetResolution {
                                ambiguous_options,
                                error,
                                ..
                            } => {
                                let message = error.unwrap_or_else(|| {
                                    format_ambiguity_message(
                                        "receiving",
                                        &ambiguous_options,
                                        "Reply with the asset and network more precisely.",
                                    )
                                });
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
            ConversationState::AwaitingFromAsset => {
                let catalog = self.fetch_currency_catalog().await?;
                match resolve_currency_phrase(&catalog, trimmed)? {
                    AssetResolution {
                        selected: Some(selection),
                        ..
                    } => {
                        draft.from = Some(selection);
                        crud.upsert_session_state(
                            wa_id,
                            phone_number_id,
                            &ConversationState::AwaitingToAsset,
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
                            "What do you want to receive? Example: `bitcoin mainnet`, `xmr`, or `usdt on trc20`.",
                        )
                        .await
                    }
                    AssetResolution {
                        ambiguous_options,
                        error,
                        ..
                    } => {
                        let message = error.unwrap_or_else(|| {
                            format_ambiguity_message(
                                "sending",
                                &ambiguous_options,
                                "Reply with the exact asset and network you are sending.",
                            )
                        });
                        self.reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                            .await
                    }
                }
            }
            ConversationState::AwaitingToAsset => {
                let catalog = self.fetch_currency_catalog().await?;
                match resolve_currency_phrase(&catalog, trimmed)? {
                    AssetResolution {
                        selected: Some(selection),
                        ..
                    } => {
                        draft.to = Some(selection);
                        crud.upsert_session_state(
                            wa_id,
                            phone_number_id,
                            &ConversationState::AwaitingAmount,
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
                            "How much do you want to send? Reply with only the amount, for example `100` or `0.25`.",
                        )
                        .await
                    }
                    AssetResolution {
                        ambiguous_options,
                        error,
                        ..
                    } => {
                        let message = error.unwrap_or_else(|| {
                            format_ambiguity_message(
                                "receiving",
                                &ambiguous_options,
                                "Reply with the exact asset and network you want to receive.",
                            )
                        });
                        self.reply(wa_id, phone_number_id, session_id.as_deref(), &message)
                            .await
                    }
                }
            }
            ConversationState::AwaitingAmount => {
                let Some(amount) = parse_amount(trimmed) else {
                    return self
                        .reply(
                            wa_id,
                            phone_number_id,
                            session_id.as_deref(),
                            "Amount not recognized. Reply with a number like `100` or `0.25`.",
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
                            "Reply with the quote number you want, for example `1`.",
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
                            "Destination asset missing. Send `swap` to restart.",
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
                        "Optional but recommended: send a refund address for the asset you are sending, or reply `skip`.",
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
                    "Optional but recommended: send a refund address for the asset you are sending, or reply `skip`.",
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
                            "Source asset missing. Send `swap` to restart.",
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
                            "Reply `confirm` to create the swap, or `cancel` to reset.",
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

                let mut lines = vec![
                    format!("Swap created: {}", response.swap_id),
                    format!(
                        "Send exactly {} {} on {}",
                        trim_f64(response.deposit_amount),
                        response.from.to_uppercase(),
                        response.network_from
                    ),
                    format!("Deposit address: {}", response.deposit_address),
                ];

                if let Some(extra_id) = response.deposit_extra_id.as_ref() {
                    lines.push(format!("Deposit extra ID: {}", extra_id));
                }

                lines.push(format!(
                    "Expected receive: {} {}",
                    trim_f64(response.estimated_receive),
                    response.to.to_uppercase()
                ));
                lines.push(format!("Provider: {}", response.provider));
                lines.push("Check progress any time with `status <swap_id>`.".to_string());

                self.reply(
                    wa_id,
                    phone_number_id,
                    session_id.as_deref(),
                    &lines.join("\n"),
                )
                .await
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
            .ok_or_else(|| "Source asset missing. Send `swap` to restart.".to_string())?;
        let to = draft
            .to
            .as_ref()
            .ok_or_else(|| "Destination asset missing. Send `swap` to restart.".to_string())?;
        let amount = draft
            .amount
            .ok_or_else(|| "Amount missing. Send `swap` to restart.".to_string())?;

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

        lines.push("Reply `confirm` to create the swap, or `cancel` to abort.".to_string());

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
            .map_err(|error| format!("I could not find swap `{}`: {}", swap_id, error))?;

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

    fn help_message() -> String {
        [
            "Commands:",
            "- `swap 100 usdc on stellar to bitcoin`",
            "- `swap` for a guided flow",
            "- `status <swap_id>` to check a swap",
            "- `cancel` to reset the current flow",
        ]
        .join("\n")
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
    let matched_networks = network_aliases
        .iter()
        .filter(|(alias, _)| normalized_phrase.contains(alias.as_str()))
        .map(|(_, canonical)| canonical.clone())
        .collect::<HashSet<_>>();

    let mut scored = Vec::new();
    for currency in catalog {
        let ticker_alias = normalize_phrase(&currency.ticker);
        let name_alias = normalize_phrase(&currency.name);
        let network_alias = normalize_phrase(&currency.network);

        let mut score = 0usize;
        if normalized_phrase == ticker_alias {
            score += 120;
        } else if normalized_phrase
            .split_whitespace()
            .any(|token| token == ticker_alias)
        {
            score += 100;
        } else if normalized_phrase.contains(&ticker_alias) {
            score += 80;
        }

        if normalized_phrase == name_alias {
            score += 120;
        } else if normalized_phrase.contains(&name_alias) {
            score += 90;
        }

        if matched_networks.contains(&currency.network)
            || normalized_phrase.contains(&network_alias)
        {
            score += 40;
        }

        if score > 0 {
            scored.push((score, currency.clone()));
        }
    }

    if scored.is_empty() {
        return Ok(AssetResolution {
            selected: None,
            ambiguous_options: Vec::new(),
            error: Some(format!(
                "I could not match `{}` to a supported asset. Try something like `usdc on stellar`, `btc mainnet`, or `xmr`.",
                phrase
            )),
        });
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

    if top.len() == 1 {
        return Ok(AssetResolution {
            selected: top.into_iter().next().map(CurrencySelection::from),
            ambiguous_options: Vec::new(),
            error: None,
        });
    }

    let unique_networks = top
        .iter()
        .map(|currency| currency.network.to_lowercase())
        .collect::<HashSet<_>>();

    if unique_networks.len() == 1 {
        return Ok(AssetResolution {
            selected: top.into_iter().next().map(CurrencySelection::from),
            ambiguous_options: Vec::new(),
            error: None,
        });
    }

    Ok(AssetResolution {
        selected: None,
        ambiguous_options: top
            .into_iter()
            .take(6)
            .map(CurrencySelection::from)
            .collect::<Vec<_>>(),
        error: Some(format!(
            "`{}` matches multiple networks. Specify the network too.",
            phrase
        )),
    })
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
        ("ethereum", "ERC20"),
        ("eth mainnet", "ERC20"),
        ("tron", "TRC20"),
        ("trc20", "TRC20"),
        ("bep20", "BEP20"),
        ("bsc", "BEP20"),
        ("binance smart chain", "BEP20"),
        ("avax c", "AVAXC"),
        ("avaxc", "AVAXC"),
        ("avalanche", "AVAXC"),
        ("polygon", "Polygon"),
        ("arbitrum", "Arbitrum"),
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

fn format_ambiguity_message(side: &str, options: &[CurrencySelection], suffix: &str) -> String {
    if options.is_empty() {
        return format!("I need a clearer {} asset. {}", side, suffix);
    }

    let mut lines = vec![format!("I found multiple {} options:", side)];
    for option in options.iter().take(6) {
        lines.push(format!(
            "- {} ({}) on {}",
            option.name,
            option.ticker.to_uppercase(),
            option.network
        ));
    }
    lines.push(suffix.to_string());
    lines.join("\n")
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
