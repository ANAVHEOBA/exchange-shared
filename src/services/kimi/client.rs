use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const DEFAULT_MOONSHOT_API_BASE_URL: &str = "https://api.moonshot.ai/v1";
const DEFAULT_KIMI_MODEL: &str = "kimi-k2.7-code-highspeed";
/// K2.6/K2.5 only accept 0.6 in non-thinking mode. K2.7 Code and K3 have
/// different fixed temperature constraints, so the request builder omits the
/// temperature field for those models.
const KIMI_NON_THINKING_TEMPERATURE: f32 = 0.6;

/// Shared grounding prepended to every Kimi call so the model always knows
/// what Assetar is and how it should sound, instead of relying on a bare
/// per-call instruction with no context about the product.
const PROJECT_CONTEXT: &str = "\
You work for Assetar, a non-custodial cryptocurrency swap service reachable over WhatsApp. Assetar \
aggregates live rates from many exchange providers (via Trocador) so users can compare routes and \
swap directly with the best one - no account needed, no KYC for most routes, and Assetar never \
holds or touches user funds directly.

The swap flow always follows the same shape: pick the coin and network to send, pick the coin and \
network to receive, enter an amount, compare quotes from different providers, provide a receiving \
address (and a refund address if needed), then confirm before the swap is created.

Match the user's own language and tone - if they write in pidgin, Spanish, or anything else, reply \
naturally in kind. Keep it short, warm, and human: the way a helpful person texting back would \
sound, not a corporate support script. Never say \"As an AI\" or use stiff phrases like \"I'd be \
happy to help\".";

const SWAP_INTENT_INSTRUCTIONS: &str = "\
You must always respond by calling exactly one of the two tools you're given:

1. If the user is expressing intent to swap crypto, even vaguely (\"swap some usdt for monero\", \
\"change my btc to xmr\", \"100 usdc to bitcoin\"), call extract_swap_request with only the values \
they actually stated. Never guess a value they did not say - leave it out instead. Generic words \
like \"crypto\", \"coin\", \"token\", \"some crypto\", or \"any coin\" are not asset names; leave \
from_asset and to_asset empty for those. If the user says they want to buy, get, receive, or cash \
out into an asset, treat that asset as to_asset unless they explicitly say it is what they are sending. \
If the user says they want to send, sell, swap from, or use an asset, treat that asset as from_asset \
unless they explicitly say it is what they want to receive. Ignore casual filler words like man, bro, \
baba, abeg, please, or pls when extracting asset names and networks.
2. For anything else - greetings, thanks, confusion, small talk, questions you can't safely answer \
- call send_friendly_reply with a short, human reply. Never invent swap rates, addresses, amounts, \
or swap status; if asked about those, say you'll need to start or check a swap first.";

const SWAP_CONTEXT_INSTRUCTIONS: &str = "\
You may also be given current swap context. Use that context to interpret short follow-up replies. \
Examples:
- If the context says the source or destination asset family is USDC and the user replies \"arbitrum\", \
  return the full asset phrase as \"usdc on arbitrum\" in the correct field.
- If the context shows the swap pair is already known and the user replies \"$100\", set amount=100 and amount_mode=usd.
- If the context shows the pair is known and the user pastes one address-like value, set the recipient or refund address \
  that best matches the context.
When context shows an active swap is already in progress, your job is to help finish it in as few messages as possible. \
If the user sends small talk, hesitation, or confusion, reply naturally and gently bring them back to the single missing piece \
shown in the context. \
If the user wants to stop, reset, cancel, abandon, or leave the current swap, use send_friendly_reply with a short acknowledgement \
that the current setup is being dropped.
Only use the provided context to resolve terse replies. Never invent values that are not grounded in the latest message \
plus the given context.";

const AMOUNT_INSTRUCTIONS: &str = "\
The user is replying to a prompt asking how much they want to swap. Their message may be messy \
(\"just send 100 bucks\", \"0.25 pls\", \"around 50\"). If you can confidently identify the single \
numeric amount they mean, call extract_amount with that number. If there is no clear number, or \
the message is about something else entirely, do not call any tool. Never guess a number that \
isn't clearly implied by the message.";

const AMOUNT_MODE_INSTRUCTIONS: &str = "\
The user is replying to a prompt asking whether they want to enter the send amount in the source \
coin or in USD. Call choose_amount_mode only when the user's wording clearly chooses one. If they \
mention dollars, usd, bucks, or use a dollar sign, choose usd. If they mention the source ticker, \
source coin, token amount, or coin amount, choose source_asset. If the message is unclear, do not \
call any tool.";

const QUOTE_SELECTION_INSTRUCTIONS: &str = "\
The user is replying to a list of numbered exchange routes. Call select_quote only when the user \
clearly selects a route. If they say first, top, recommended, best, cheapest, most private, or use \
route 1 language, choose index 1. If they give a number or ordinal, use that route number. If the \
message is unclear or not a route selection, do not call any tool. Never invent route numbers.";

const ADDRESS_EXTRACTION_INSTRUCTIONS: &str = "\
The user is replying with a cryptocurrency receiving or refund address. Call extract_address only \
when the message clearly contains one address-like value. It may be surrounded by words like \
\"send to\" or \"use this\". Never invent or repair an address. If the message is unclear or has \
multiple possible addresses, do not call any tool.";

const NARRATE_INSTRUCTIONS: &str = "\
You'll be given a short description of a fact or step to convey to the user right now. Rephrase it \
into a single short WhatsApp message in your own natural words. Never invent, alter, round, or omit \
any number, ticker, network name, or address mentioned in the description - repeat those exactly \
as given. Don't add extra options, steps, or questions beyond what's described. Reply with the \
message text only - no preamble, no quotes around it, no meta commentary.";

/// Client for Moonshot's Kimi chat completions API.
/// Used as an optional pre-processor and phrasing layer in front of the
/// deterministic WhatsApp swap flow - it never decides an amount, address, or
/// network, only extracts/rephrases what the flow already has.
pub struct KimiClient {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
}

#[derive(Debug)]
pub enum KimiError {
    HttpError(String),
    ParseError(String),
    ApiError(String),
}

impl std::fmt::Display for KimiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KimiError::HttpError(e) => write!(f, "Kimi HTTP error: {}", e),
            KimiError::ParseError(e) => write!(f, "Kimi parse error: {}", e),
            KimiError::ApiError(e) => write!(f, "Kimi API error: {}", e),
        }
    }
}

impl std::error::Error for KimiError {}

/// Structured result of classifying one inbound WhatsApp message.
#[derive(Debug, Clone)]
pub enum KimiIntent {
    /// The user appears to want a swap; fields are only populated when stated.
    SwapRequest {
        amount: Option<f64>,
        amount_mode: Option<KimiAmountMode>,
        from_asset: Option<String>,
        to_asset: Option<String>,
        recipient_address: Option<String>,
        refund_address: Option<String>,
    },
    /// Anything else - a ready-to-send, human-toned reply.
    FriendlyReply(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KimiAmountMode {
    SourceAsset,
    Usd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KimiConfirmation {
    Confirm,
    Cancel,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ThinkingConfig<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
}

#[derive(Serialize)]
struct ToolFunctionDef<'a> {
    name: &'a str,
    description: &'a str,
    parameters: Value,
}

#[derive(Serialize)]
struct ToolDef<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    function: ToolFunctionDef<'a>,
}

#[derive(Serialize)]
struct ChatCompletionRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    thinking: Option<ThinkingConfig<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ToolDef<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    #[serde(default)]
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
}

#[derive(Deserialize)]
struct ChatResponseMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<ToolCall>,
}

#[derive(Deserialize)]
struct ToolCall {
    function: ToolCallFunction,
}

#[derive(Deserialize)]
struct ToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Deserialize, Default)]
struct SwapRequestArgs {
    #[serde(default)]
    amount: Option<f64>,
    #[serde(default)]
    amount_mode: Option<String>,
    #[serde(default)]
    from_asset: Option<String>,
    #[serde(default)]
    to_asset: Option<String>,
    #[serde(default)]
    recipient_address: Option<String>,
    #[serde(default)]
    refund_address: Option<String>,
}

#[derive(Deserialize)]
struct FriendlyReplyArgs {
    message: String,
}

#[derive(Deserialize)]
struct AmountArgs {
    amount: f64,
}

#[derive(Deserialize)]
struct AmountModeArgs {
    mode: String,
}

#[derive(Deserialize)]
struct QuoteSelectionArgs {
    index: usize,
}

#[derive(Deserialize)]
struct AddressArgs {
    address: String,
}

impl KimiClient {
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Returns `None` when no Kimi/Moonshot key is configured, matching the
    /// other optional integrations in this codebase (Redis, email, WhatsApp).
    pub fn from_env() -> Option<Self> {
        if std::env::var("KIMI_ENABLED")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no"
                )
            })
            .unwrap_or(false)
        {
            return None;
        }

        let api_key = std::env::var("KIMI_API_KEY")
            .or_else(|_| std::env::var("MOONSHOT_API_KEY"))
            .ok()?;
        let base_url = std::env::var("KIMI_API_BASE_URL")
            .unwrap_or_else(|_| DEFAULT_MOONSHOT_API_BASE_URL.to_string());
        let model = std::env::var("KIMI_MODEL").unwrap_or_else(|_| DEFAULT_KIMI_MODEL.to_string());
        let timeout_seconds = std::env::var("KIMI_TIMEOUT_SECONDS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(8)
            .clamp(1, 30);
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds))
            .build()
            .ok()?;

        Some(Self {
            client,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
        })
    }

    pub async fn classify_swap_message(&self, user_text: &str) -> Result<KimiIntent, KimiError> {
        self.classify_swap_message_internal(user_text, SWAP_INTENT_INSTRUCTIONS)
            .await
    }

    pub async fn classify_swap_message_with_context(
        &self,
        user_text: &str,
        context: &str,
    ) -> Result<KimiIntent, KimiError> {
        let instructions = format!(
            "{}\n\n{}",
            SWAP_INTENT_INSTRUCTIONS, SWAP_CONTEXT_INSTRUCTIONS
        );
        let prompt = format!(
            "Current swap context:\n{}\n\nLatest user message:\n{}",
            context, user_text
        );

        self.classify_swap_message_internal(&prompt, &instructions)
            .await
    }

    async fn classify_swap_message_internal(
        &self,
        user_text: &str,
        instructions: &str,
    ) -> Result<KimiIntent, KimiError> {
        let tools = vec![
            ToolDef {
                kind: "function",
                function: ToolFunctionDef {
                    name: "extract_swap_request",
                    description: "Record a crypto swap the user wants to make, using only values they stated.",
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "amount": {
                                "type": "number",
                                "description": "The numeric amount if the user stated one."
                            },
                            "amount_mode": {
                                "type": "string",
                                "enum": ["source_asset", "usd"],
                                "description": "usd when the amount is clearly a dollar value like $100 or 100 USD; otherwise source_asset when it is a crypto amount."
                            },
                            "from_asset": {
                                "type": "string",
                                "description": "The coin/network the user is sending, as they described it. Do not fill this with generic words like crypto, coin, token, or some crypto."
                            },
                            "to_asset": {
                                "type": "string",
                                "description": "The coin/network the user wants to receive, as they described it. Do not fill this with generic words like crypto, coin, token, or some crypto."
                            },
                            "recipient_address": {
                                "type": "string",
                                "description": "The destination/receiving/payout address, if the user clearly gave one."
                            },
                            "refund_address": {
                                "type": "string",
                                "description": "The refund address, if the user clearly gave one."
                            }
                        }
                    }),
                },
            },
            ToolDef {
                kind: "function",
                function: ToolFunctionDef {
                    name: "send_friendly_reply",
                    description: "Send a short, warm, human-sounding WhatsApp reply for anything that isn't a swap request.",
                    parameters: serde_json::json!({
                        "type": "object",
                        "properties": {
                            "message": {
                                "type": "string",
                                "description": "The reply text: at most two short sentences, at most one emoji."
                            }
                        },
                        "required": ["message"]
                    }),
                },
            },
        ];

        let system_prompt = Self::system_prompt(instructions);
        let message = self
            .send_chat_completion(&system_prompt, user_text, Some(tools))
            .await?;

        if let Some(call) = message.tool_calls.into_iter().next() {
            return match call.function.name.as_str() {
                "extract_swap_request" => {
                    let args: SwapRequestArgs =
                        serde_json::from_str(&call.function.arguments).unwrap_or_default();

                    Ok(KimiIntent::SwapRequest {
                        amount: args.amount,
                        amount_mode: parse_amount_mode_arg(args.amount_mode.as_deref()),
                        from_asset: args.from_asset,
                        to_asset: args.to_asset,
                        recipient_address: args.recipient_address,
                        refund_address: args.refund_address,
                    })
                }
                "send_friendly_reply" => {
                    let args: FriendlyReplyArgs = serde_json::from_str(&call.function.arguments)
                        .map_err(|e| {
                            KimiError::ParseError(format!(
                                "Invalid send_friendly_reply arguments: {}",
                                e
                            ))
                        })?;

                    Ok(KimiIntent::FriendlyReply(args.message))
                }
                other => Err(KimiError::ParseError(format!(
                    "Unexpected tool call: {}",
                    other
                ))),
            };
        }

        if let Some(content) = message.content.filter(|c| !c.trim().is_empty()) {
            return Ok(KimiIntent::FriendlyReply(content));
        }

        Err(KimiError::ParseError(
            "Kimi returned an empty response".to_string(),
        ))
    }

    /// Best-effort extraction of a single numeric amount out of messy free text
    /// (e.g. "just send 100 bucks"). Returns `Ok(None)` when Kimi isn't confident
    /// enough to call the tool, rather than guessing.
    pub async fn extract_amount(&self, user_text: &str) -> Result<Option<f64>, KimiError> {
        let tools = vec![ToolDef {
            kind: "function",
            function: ToolFunctionDef {
                name: "extract_amount",
                description: "Record the single numeric amount the user stated.",
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "amount": {
                            "type": "number",
                            "description": "The numeric amount the user meant, with no currency words or symbols."
                        }
                    },
                    "required": ["amount"]
                }),
            },
        }];

        let system_prompt = Self::system_prompt(AMOUNT_INSTRUCTIONS);
        let message = self
            .send_chat_completion(&system_prompt, user_text, Some(tools))
            .await?;

        let Some(call) = message.tool_calls.into_iter().next() else {
            return Ok(None);
        };

        if call.function.name != "extract_amount" {
            return Ok(None);
        }

        let args: AmountArgs = serde_json::from_str(&call.function.arguments).map_err(|e| {
            KimiError::ParseError(format!("Invalid extract_amount arguments: {}", e))
        })?;

        if args.amount > 0.0 {
            Ok(Some(args.amount))
        } else {
            Ok(None)
        }
    }

    /// Interprets a user's natural-language choice between entering the amount
    /// in the source asset or in USD. This never extracts the amount itself;
    /// callers still parse/validate the numeric amount separately.
    pub async fn choose_amount_mode(
        &self,
        user_text: &str,
        source_ticker: &str,
        source_network: &str,
    ) -> Result<Option<KimiAmountMode>, KimiError> {
        let tools = vec![ToolDef {
            kind: "function",
            function: ToolFunctionDef {
                name: "choose_amount_mode",
                description:
                    "Choose whether the user wants to enter the amount in the source asset or USD.",
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "mode": {
                            "type": "string",
                            "enum": ["source_asset", "usd"],
                            "description": "source_asset when the user wants to enter the coin amount; usd when they want to enter a dollar value."
                        }
                    },
                    "required": ["mode"]
                }),
            },
        }];

        let system_prompt = Self::system_prompt(AMOUNT_MODE_INSTRUCTIONS);
        let prompt = format!(
            "Source asset: {} on {}\nUser message: {}",
            source_ticker.to_uppercase(),
            source_network,
            user_text
        );
        let message = self
            .send_chat_completion(&system_prompt, &prompt, Some(tools))
            .await?;

        let Some(call) = message.tool_calls.into_iter().next() else {
            return Ok(None);
        };

        if call.function.name != "choose_amount_mode" {
            return Ok(None);
        }

        let args: AmountModeArgs = serde_json::from_str(&call.function.arguments).map_err(|e| {
            KimiError::ParseError(format!("Invalid choose_amount_mode arguments: {}", e))
        })?;

        match args.mode.trim() {
            "source_asset" => Ok(Some(KimiAmountMode::SourceAsset)),
            "usd" => Ok(Some(KimiAmountMode::Usd)),
            _ => Ok(None),
        }
    }

    /// Interprets natural route selection wording like "first one", "best",
    /// or "route 2". The selected number is bounded by the caller's route
    /// count before being returned.
    pub async fn choose_quote_index(
        &self,
        user_text: &str,
        route_count: usize,
    ) -> Result<Option<usize>, KimiError> {
        if route_count == 0 {
            return Ok(None);
        }

        let tools = vec![ToolDef {
            kind: "function",
            function: ToolFunctionDef {
                name: "select_quote",
                description: "Select one numbered exchange route from the displayed quote list.",
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "index": {
                            "type": "integer",
                            "minimum": 1,
                            "description": "The route number the user selected."
                        }
                    },
                    "required": ["index"]
                }),
            },
        }];

        let system_prompt = Self::system_prompt(QUOTE_SELECTION_INSTRUCTIONS);
        let prompt = format!(
            "Available route count: {}\nUser message: {}",
            route_count, user_text
        );
        let message = self
            .send_chat_completion(&system_prompt, &prompt, Some(tools))
            .await?;

        let Some(call) = message.tool_calls.into_iter().next() else {
            return Ok(None);
        };

        if call.function.name != "select_quote" {
            return Ok(None);
        }

        let args: QuoteSelectionArgs = serde_json::from_str(&call.function.arguments)
            .map_err(|e| KimiError::ParseError(format!("Invalid select_quote arguments: {}", e)))?;

        if (1..=route_count).contains(&args.index) {
            Ok(Some(args.index))
        } else {
            Ok(None)
        }
    }

    /// Best-effort extraction of a single address out of a conversational
    /// message like "use this one: 4A...". The backend still validates it for
    /// the expected asset/network before accepting it.
    pub async fn extract_address(
        &self,
        user_text: &str,
        ticker: &str,
        network: &str,
    ) -> Result<Option<String>, KimiError> {
        let tools = vec![ToolDef {
            kind: "function",
            function: ToolFunctionDef {
                name: "extract_address",
                description: "Extract exactly one cryptocurrency address from the user's message.",
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "address": {
                            "type": "string",
                            "description": "The address exactly as the user wrote it."
                        }
                    },
                    "required": ["address"]
                }),
            },
        }];

        let system_prompt = Self::system_prompt(ADDRESS_EXTRACTION_INSTRUCTIONS);
        let prompt = format!(
            "Expected asset: {} on {}\nUser message: {}",
            ticker.to_uppercase(),
            network,
            user_text
        );
        let message = self
            .send_chat_completion(&system_prompt, &prompt, Some(tools))
            .await?;

        let Some(call) = message.tool_calls.into_iter().next() else {
            return Ok(None);
        };

        if call.function.name != "extract_address" {
            return Ok(None);
        }

        let args: AddressArgs = serde_json::from_str(&call.function.arguments).map_err(|e| {
            KimiError::ParseError(format!("Invalid extract_address arguments: {}", e))
        })?;
        let address = args.address.trim();

        if address.is_empty() {
            Ok(None)
        } else {
            Ok(Some(address.to_string()))
        }
    }

    /// Rephrases a plain description of a step/fact into a short, natural
    /// WhatsApp message that matches the user's own language/tone. This is a
    /// pure phrasing pass with no tool calls and no external I/O beyond the
    /// one Kimi request - callers must describe any number, ticker, network,
    /// or address exactly as it should appear, since the model is instructed
    /// to repeat those verbatim rather than invent or restate them loosely.
    /// Not used for the address-entry, confirmation, or quote-list steps -
    /// those stay fully templated.
    pub async fn narrate(&self, situation: &str) -> Result<String, KimiError> {
        let system_prompt = Self::system_prompt(NARRATE_INSTRUCTIONS);
        let message = self
            .send_chat_completion(&system_prompt, situation, None)
            .await?;

        message
            .content
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| KimiError::ParseError("Kimi returned an empty response".to_string()))
    }

    fn system_prompt(instructions: &str) -> String {
        format!("{}\n\n{}", PROJECT_CONTEXT, instructions)
    }

    fn request_overrides(&self) -> (Option<ThinkingConfig<'static>>, Option<f32>) {
        if self.model.starts_with("kimi-k2.6") || self.model.starts_with("kimi-k2.5") {
            return (
                Some(ThinkingConfig { kind: "disabled" }),
                Some(KIMI_NON_THINKING_TEMPERATURE),
            );
        }

        (None, None)
    }

    async fn send_chat_completion(
        &self,
        system_prompt: &str,
        user_text: &str,
        tools: Option<Vec<ToolDef<'_>>>,
    ) -> Result<ChatResponseMessage, KimiError> {
        let tool_choice = tools.is_some().then_some("auto");
        let (thinking, temperature) = self.request_overrides();
        let request = ChatCompletionRequest {
            model: &self.model,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: system_prompt,
                },
                ChatMessage {
                    role: "user",
                    content: user_text,
                },
            ],
            thinking,
            tools,
            tool_choice,
            // Kimi rejects unsupported fixed values outright, so model-specific
            // fields are omitted unless the chosen model accepts them.
            temperature,
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| KimiError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(KimiError::ApiError(format!(
                "Kimi API returned status {}: {}",
                status, body
            )));
        }

        let parsed: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|e| KimiError::ParseError(format!("Invalid Kimi response: {}", e)))?;

        parsed
            .choices
            .into_iter()
            .next()
            .map(|choice| choice.message)
            .ok_or_else(|| KimiError::ParseError("Kimi returned no choices".to_string()))
    }
}

fn parse_amount_mode_arg(value: Option<&str>) -> Option<KimiAmountMode> {
    match value.map(str::trim) {
        Some("source_asset") => Some(KimiAmountMode::SourceAsset),
        Some("usd") => Some(KimiAmountMode::Usd),
        _ => None,
    }
}
