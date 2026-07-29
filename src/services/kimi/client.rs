use reqwest::Client;
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_MOONSHOT_API_BASE_URL: &str = "https://api.moonshot.ai/v1";
const DEFAULT_KIMI_MODEL: &str = "kimi-k2.7-code";

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
Decide whether the user is describing a crypto swap or just needs a normal chat reply.

If the user is expressing intent to swap crypto, even vaguely (\"swap some usdt for monero\", \
\"change my btc to xmr\", \"100 usdc to bitcoin\", \"i need eth on base\", \"i want btc on mainnet\"), \
return kind=swap_request with only the values they actually stated. Never guess a value they did not \
say - leave it null instead. Generic words like crypto, coin, token, quantifiers like some, a, or \
an, and phrases like some crypto or any coin are not asset names; leave from_asset and to_asset null \
for those. If the user says they want to buy, get, receive, cash out into, need, or want a specific \
asset/network, treat that asset as to_asset unless they explicitly say it is what they are sending. \
If the user says they want to send, sell, swap from, or use an asset, treat that asset as from_asset \
unless they explicitly say it is what they want to receive. Ignore casual filler words like man, bro, \
baba, abeg, please, or pls when extracting asset names and networks.

For anything else - greetings, thanks, confusion, small talk, questions you can't safely answer - \
return kind=friendly_reply with a short, human reply. Never invent swap rates, addresses, amounts, \
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
If the user wants to stop, reset, cancel, abandon, or leave the current swap, return kind=friendly_reply with a short acknowledgement \
that the current setup is being dropped.
Only use the provided context to resolve terse replies. Never invent values that are not grounded in the latest message \
plus the given context.";

const AMOUNT_INSTRUCTIONS: &str = "\
The user is replying to a prompt asking how much they want to swap. Their message may be messy \
(\"just send 100 bucks\", \"0.25 pls\", \"around 50\"). If you can confidently identify the single \
numeric amount they mean, return it. If there is no clear number, or the message is about something \
else entirely, return amount=null. Never guess a number that isn't clearly implied by the message.";

const AMOUNT_MODE_INSTRUCTIONS: &str = "\
The user is replying to a prompt asking whether they want to enter the send amount in the source \
coin or in USD. Return mode=usd only when the user's wording clearly chooses USD. If they mention \
dollars, usd, bucks, or use a dollar sign, choose usd. If they mention the source ticker, source \
coin, token amount, or coin amount, choose source_asset. If the message is unclear, return mode=null.";

const QUOTE_SELECTION_INSTRUCTIONS: &str = "\
The user is replying to a list of numbered exchange routes. Return the selected route index only when \
the user clearly selects a route. If they say first, top, recommended, best, cheapest, most private, \
or use route 1 language, choose index 1. If they give a number or ordinal, use that route number. \
If the message is unclear or not a route selection, return index=null. Never invent route numbers.";

const ADDRESS_EXTRACTION_INSTRUCTIONS: &str = "\
The user is replying with a cryptocurrency receiving or refund address. Return address only when the \
message clearly contains one address-like value. It may be surrounded by words like \"send to\" or \
\"use this\". Never invent or repair an address. If the message is unclear or has multiple possible \
addresses, return address=null.";

const NARRATE_INSTRUCTIONS: &str = "\
You'll be given a short description of a fact or step to convey to the user right now. Rephrase it \
into a single short WhatsApp message in your own natural words. Never invent, alter, round, or omit \
any number, ticker, network name, or address mentioned in the description - repeat those exactly \
as given. Don't add extra options, steps, or questions beyond what's described. Reply with the \
message text only - no preamble, no quotes around it, no meta commentary.";

const STRUCTURED_OUTPUT_INSTRUCTIONS: &str = "\
You must produce structured output that matches the requested schema exactly. \
Do not include explanation, markdown, or extra keys outside the schema.";

pub struct KimiClient {
    http_client: Client,
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

#[derive(Debug, Clone)]
pub enum KimiIntent {
    SwapRequest {
        amount: Option<f64>,
        amount_mode: Option<KimiAmountMode>,
        from_asset: Option<String>,
        to_asset: Option<String>,
        recipient_address: Option<String>,
        refund_address: Option<String>,
    },
    FriendlyReply(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KimiAmountMode {
    SourceAsset,
    Usd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KimiConfirmation {
    Confirm,
    Cancel,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum StructuredIntentKind {
    SwapRequest,
    FriendlyReply,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredIntentResponse {
    kind: StructuredIntentKind,
    #[serde(default)]
    amount: Option<f64>,
    #[serde(default)]
    amount_mode: Option<KimiAmountMode>,
    #[serde(default)]
    from_asset: Option<String>,
    #[serde(default)]
    to_asset: Option<String>,
    #[serde(default)]
    recipient_address: Option<String>,
    #[serde(default)]
    refund_address: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredAmountResponse {
    #[serde(default)]
    amount: Option<f64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredAmountModeResponse {
    #[serde(default)]
    mode: Option<KimiAmountMode>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredQuoteSelectionResponse {
    #[serde(default)]
    index: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredAddressResponse {
    #[serde(default)]
    address: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct StructuredNarrationResponse {
    message: String,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    #[serde(default)]
    choices: Vec<ChatCompletionChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionMessage,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionMessage {
    #[serde(default)]
    content: String,
    #[serde(default)]
    reasoning_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionErrorEnvelope {
    error: ChatCompletionErrorBody,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionErrorBody {
    message: String,
    #[serde(default)]
    r#type: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct KimiRequestProfile {
    temperature: Option<f64>,
    thinking_type: Option<&'static str>,
    max_tokens: u32,
}

impl KimiClient {
    pub fn model(&self) -> &str {
        &self.model
    }

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

        Some(Self {
            http_client: Client::new(),
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
        let response = self
            .typed_prompt::<StructuredIntentResponse>(instructions, user_text, "structured intent")
            .await?;

        match response.kind {
            StructuredIntentKind::SwapRequest => Ok(KimiIntent::SwapRequest {
                amount: response.amount,
                amount_mode: response.amount_mode,
                from_asset: response.from_asset,
                to_asset: response.to_asset,
                recipient_address: response.recipient_address,
                refund_address: response.refund_address,
            }),
            StructuredIntentKind::FriendlyReply => Ok(KimiIntent::FriendlyReply(
                response
                    .message
                    .filter(|message| !message.trim().is_empty())
                    .unwrap_or_else(|| {
                        "Tell me what you want to swap, or paste a swap ID for me to check."
                            .to_string()
                    }),
            )),
        }
    }

    pub async fn extract_amount(&self, user_text: &str) -> Result<Option<f64>, KimiError> {
        let response = self
            .typed_prompt::<StructuredAmountResponse>(
                AMOUNT_INSTRUCTIONS,
                user_text,
                "amount extraction",
            )
            .await?;

        Ok(response.amount.filter(|amount| *amount > 0.0))
    }

    pub async fn choose_amount_mode(
        &self,
        user_text: &str,
        source_ticker: &str,
        source_network: &str,
    ) -> Result<Option<KimiAmountMode>, KimiError> {
        let prompt = format!(
            "Source asset: {} on {}\nUser message: {}",
            source_ticker.to_uppercase(),
            source_network,
            user_text
        );
        let response = self
            .typed_prompt::<StructuredAmountModeResponse>(
                AMOUNT_MODE_INSTRUCTIONS,
                &prompt,
                "amount mode",
            )
            .await?;

        Ok(response.mode)
    }

    pub async fn choose_quote_index(
        &self,
        user_text: &str,
        route_count: usize,
    ) -> Result<Option<usize>, KimiError> {
        if route_count == 0 {
            return Ok(None);
        }

        let prompt = format!(
            "Available route count: {}\nUser message: {}",
            route_count, user_text
        );
        let response = self
            .typed_prompt::<StructuredQuoteSelectionResponse>(
                QUOTE_SELECTION_INSTRUCTIONS,
                &prompt,
                "quote selection",
            )
            .await?;

        Ok(response
            .index
            .filter(|index| (1..=route_count).contains(index)))
    }

    pub async fn extract_address(
        &self,
        user_text: &str,
        ticker: &str,
        network: &str,
    ) -> Result<Option<String>, KimiError> {
        let prompt = format!(
            "Expected asset: {} on {}\nUser message: {}",
            ticker.to_uppercase(),
            network,
            user_text
        );
        let response = self
            .typed_prompt::<StructuredAddressResponse>(
                ADDRESS_EXTRACTION_INSTRUCTIONS,
                &prompt,
                "address extraction",
            )
            .await?;

        Ok(response
            .address
            .map(|address| address.trim().to_string())
            .filter(|address| !address.is_empty()))
    }

    pub async fn narrate(&self, situation: &str) -> Result<String, KimiError> {
        let response = self
            .typed_prompt::<StructuredNarrationResponse>(
                NARRATE_INSTRUCTIONS,
                situation,
                "narration",
            )
            .await?;

        let message = response.message.trim();
        if message.is_empty() {
            Err(KimiError::ParseError(
                "Kimi returned an empty narration".to_string(),
            ))
        } else {
            Ok(message.to_string())
        }
    }

    fn system_prompt(instructions: &str) -> String {
        format!(
            "{}\n\n{}\n\n{}",
            PROJECT_CONTEXT, instructions, STRUCTURED_OUTPUT_INSTRUCTIONS
        )
    }

    fn request_profile(&self) -> KimiRequestProfile {
        let normalized_model = self.model.to_ascii_lowercase();

        if normalized_model.starts_with("kimi-k2.7-code") {
            return KimiRequestProfile {
                temperature: Some(1.0),
                thinking_type: Some("enabled"),
                max_tokens: 256,
            };
        }

        if normalized_model == "kimi-k2.6" {
            return KimiRequestProfile {
                temperature: Some(0.6),
                thinking_type: None,
                max_tokens: 256,
            };
        }

        KimiRequestProfile {
            temperature: None,
            thinking_type: None,
            max_tokens: 256,
        }
    }

    async fn typed_prompt<T>(
        &self,
        instructions: &str,
        user_text: &str,
        label: &str,
    ) -> Result<T, KimiError>
    where
        T: JsonSchema + DeserializeOwned + Send + 'static,
    {
        let profile = self.request_profile();
        let mut payload = json!({
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": Self::system_prompt(instructions),
                },
                {
                    "role": "user",
                    "content": user_text,
                }
            ],
            "response_format": {
                "type": "json_object"
            },
            "max_tokens": profile.max_tokens,
        });

        if let Some(temperature) = profile.temperature {
            payload["temperature"] = json!(temperature);
        }

        if let Some(thinking_type) = profile.thinking_type {
            payload["thinking"] = json!({ "type": thinking_type });
        }

        let response = self
            .http_client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&payload)
            .send()
            .await
            .map_err(|error| KimiError::HttpError(error.to_string()))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| KimiError::HttpError(error.to_string()))?;

        if !status.is_success() {
            return Err(KimiError::ApiError(parse_api_error_message(&body)));
        }

        let envelope: ChatCompletionResponse = serde_json::from_str(&body).map_err(|error| {
            KimiError::ParseError(format!("Invalid Kimi {} envelope: {}", label, error))
        })?;

        let message = envelope
            .choices
            .first()
            .map(|choice| &choice.message)
            .ok_or_else(|| {
                KimiError::ParseError(format!("Kimi returned no choices for {}", label))
            })?;

        let content = normalize_json_content(&message.content);
        if content.is_empty() {
            let reasoning = message.reasoning_content.as_deref().unwrap_or_default();
            return Err(KimiError::ParseError(format!(
                "Kimi returned empty {} content{}",
                label,
                if reasoning.is_empty() {
                    String::new()
                } else {
                    format!(" (reasoning present: {} chars)", reasoning.chars().count())
                }
            )));
        }

        serde_json::from_str::<T>(&content).map_err(|error| {
            KimiError::ParseError(format!(
                "Invalid Kimi {} response: {}. Raw content: {}",
                label, error, content
            ))
        })
    }
}

fn normalize_json_content(content: &str) -> String {
    let trimmed = content.trim();
    if trimmed.starts_with("```") {
        return trimmed
            .trim_start_matches("```json")
            .trim_start_matches("```JSON")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim()
            .to_string();
    }

    trimmed.to_string()
}

fn parse_api_error_message(body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<ChatCompletionErrorEnvelope>(body) {
        if let Some(kind) = parsed.error.r#type.filter(|value| !value.trim().is_empty()) {
            return format!("{} ({})", parsed.error.message, kind);
        }

        return parsed.error.message;
    }

    if let Ok(parsed) = serde_json::from_str::<Value>(body) {
        if let Some(message) = parsed
            .get("errorMessage")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            return message.to_string();
        }
    }

    body.to_string()
}
