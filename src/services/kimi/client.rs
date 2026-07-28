use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MOONSHOT_API_BASE_URL: &str = "https://api.moonshot.ai/v1";
const KIMI_MODEL: &str = "kimi-k2.6";

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
they actually stated. Never guess a value they did not say - leave it out instead.
2. For anything else - greetings, thanks, confusion, small talk, questions you can't safely answer \
- call send_friendly_reply with a short, human reply. Never invent swap rates, addresses, amounts, \
or swap status; if asked about those, say you'll need to start or check a swap first.";

const AMOUNT_INSTRUCTIONS: &str = "\
The user is replying to a prompt asking how much they want to swap. Their message may be messy \
(\"just send 100 bucks\", \"0.25 pls\", \"around 50\"). If you can confidently identify the single \
numeric amount they mean, call extract_amount with that number. If there is no clear number, or \
the message is about something else entirely, do not call any tool. Never guess a number that \
isn't clearly implied by the message.";

const NARRATE_INSTRUCTIONS: &str = "\
You'll be given a short description of a fact or step to convey to the user right now. Rephrase it \
into a single short WhatsApp message in your own natural words. Never invent, alter, round, or omit \
any number, ticker, network name, or address mentioned in the description - repeat those exactly \
as given. Don't add extra options, steps, or questions beyond what's described. Reply with the \
message text only - no preamble, no quotes around it, no meta commentary.";

/// Client for Moonshot's Kimi K2.6 chat completions API.
/// Used as an optional pre-processor and phrasing layer in front of the
/// deterministic WhatsApp swap flow - it never decides an amount, address, or
/// network, only extracts/rephrases what the flow already has.
pub struct KimiClient {
    client: Client,
    api_key: String,
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
        from_asset: Option<String>,
        to_asset: Option<String>,
    },
    /// Anything else - a ready-to-send, human-toned reply.
    FriendlyReply(String),
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
    thinking: ThinkingConfig<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ToolDef<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'a str>,
    temperature: f32,
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
    from_asset: Option<String>,
    #[serde(default)]
    to_asset: Option<String>,
}

#[derive(Deserialize)]
struct FriendlyReplyArgs {
    message: String,
}

#[derive(Deserialize)]
struct AmountArgs {
    amount: f64,
}

impl KimiClient {
    /// Returns `None` when `KIMI_API_KEY` is not configured, matching the other
    /// optional integrations in this codebase (Redis, email, WhatsApp).
    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("KIMI_API_KEY").ok()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(8))
            .build()
            .ok()?;

        Some(Self { client, api_key })
    }

    pub async fn classify_swap_message(&self, user_text: &str) -> Result<KimiIntent, KimiError> {
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
                                "description": "Amount of the source asset, if the user stated one."
                            },
                            "from_asset": {
                                "type": "string",
                                "description": "The coin/network the user is sending, as they described it."
                            },
                            "to_asset": {
                                "type": "string",
                                "description": "The coin/network the user wants to receive, as they described it."
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

        let system_prompt = Self::system_prompt(SWAP_INTENT_INSTRUCTIONS);
        let message = self
            .send_chat_completion(&system_prompt, user_text, Some(tools), 0.4)
            .await?;

        if let Some(call) = message.tool_calls.into_iter().next() {
            return match call.function.name.as_str() {
                "extract_swap_request" => {
                    let args: SwapRequestArgs =
                        serde_json::from_str(&call.function.arguments).unwrap_or_default();

                    Ok(KimiIntent::SwapRequest {
                        amount: args.amount,
                        from_asset: args.from_asset,
                        to_asset: args.to_asset,
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
            .send_chat_completion(&system_prompt, user_text, Some(tools), 0.4)
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
            .send_chat_completion(&system_prompt, situation, None, 0.7)
            .await?;

        message
            .content
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| KimiError::ParseError("Kimi returned an empty response".to_string()))
    }

    fn system_prompt(instructions: &str) -> String {
        format!("{}\n\n{}", PROJECT_CONTEXT, instructions)
    }

    async fn send_chat_completion(
        &self,
        system_prompt: &str,
        user_text: &str,
        tools: Option<Vec<ToolDef<'_>>>,
        temperature: f32,
    ) -> Result<ChatResponseMessage, KimiError> {
        let tool_choice = tools.is_some().then_some("auto");
        let request = ChatCompletionRequest {
            model: KIMI_MODEL,
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
            thinking: ThinkingConfig { kind: "disabled" },
            tools,
            tool_choice,
            temperature,
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", MOONSHOT_API_BASE_URL))
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
