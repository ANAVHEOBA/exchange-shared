use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MOONSHOT_API_BASE_URL: &str = "https://api.moonshot.ai/v1";
const KIMI_MODEL: &str = "kimi-k2.6";

const SYSTEM_PROMPT: &str = r#"You are the WhatsApp assistant for Assetar, a non-custodial crypto swap service.
Talk like a real person texting, not a support bot: short, warm, casual. Use at most one emoji per
message, and often none at all. Never say "As an AI" or "I'd be happy to help".

You must always respond by calling exactly one of the two tools you're given:

1. If the user is expressing intent to swap crypto, even vaguely ("swap some usdt for monero",
   "change my btc to xmr", "100 usdc to bitcoin"), call extract_swap_request with only the values
   they actually stated. Never guess a value they did not say - leave it out instead.
2. For anything else - greetings, thanks, confusion, small talk, questions you can't safely answer -
   call send_friendly_reply with a short, human reply. Never invent swap rates, addresses, amounts,
   or swap status; if asked about those, say you'll need to start or check a swap first.
"#;

/// Client for Moonshot's Kimi K2.6 chat completions API.
/// Used as an optional pre-processor in front of the deterministic WhatsApp swap flow.
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
    tools: Vec<ToolDef<'a>>,
    tool_choice: &'a str,
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
        let request = ChatCompletionRequest {
            model: KIMI_MODEL,
            messages: vec![
                ChatMessage {
                    role: "system",
                    content: SYSTEM_PROMPT,
                },
                ChatMessage {
                    role: "user",
                    content: user_text,
                },
            ],
            thinking: ThinkingConfig { kind: "disabled" },
            tools: vec![
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
            ],
            tool_choice: "auto",
            temperature: 0.4,
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

        let message = parsed
            .choices
            .into_iter()
            .next()
            .map(|choice| choice.message)
            .ok_or_else(|| KimiError::ParseError("Kimi returned no choices".to_string()))?;

        if let Some(call) = message.tool_calls.into_iter().next() {
            return match call.function.name.as_str() {
                "extract_swap_request" => {
                    let args: SwapRequestArgs = serde_json::from_str(&call.function.arguments)
                        .unwrap_or_default();

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
                other => Err(KimiError::ParseError(format!("Unexpected tool call: {}", other))),
            };
        }

        if let Some(content) = message.content.filter(|c| !c.trim().is_empty()) {
            return Ok(KimiIntent::FriendlyReply(content));
        }

        Err(KimiError::ParseError(
            "Kimi returned an empty response".to_string(),
        ))
    }
}
