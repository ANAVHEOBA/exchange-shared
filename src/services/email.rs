use std::{env, time::Duration};

use reqwest::Client;
use serde::Serialize;

const BREVO_API_BASE_URL: &str = "https://api.brevo.com/v3";

enum EmailProvider {
    Brevo { api_key: String, client: Client },
    Smtp {
        host: String,
        port: u16,
        username: String,
        password: String,
    },
}

pub struct EmailService {
    provider: EmailProvider,
    from_email: String,
    from_name: String,
    app_url: String,
}

#[derive(Debug)]
pub enum EmailError {
    ConfigError(String),
    SendError(String),
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmailError::ConfigError(e) => write!(f, "Email config error: {}", e),
            EmailError::SendError(e) => write!(f, "Email send error: {}", e),
        }
    }
}

impl std::error::Error for EmailError {}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrevoSendEmailRequest<'a> {
    sender: BrevoSender<'a>,
    to: Vec<BrevoRecipient<'a>>,
    subject: &'a str,
    html_content: &'a str,
    text_content: &'a str,
}

#[derive(Serialize)]
struct BrevoSender<'a> {
    email: &'a str,
    name: &'a str,
}

#[derive(Serialize)]
struct BrevoRecipient<'a> {
    email: &'a str,
    name: &'a str,
}

impl EmailService {
    pub fn from_env() -> Result<Self, EmailError> {
        let from_email = env::var("EMAIL_FROM_EMAIL")
            .or_else(|_| env::var("SMTP_FROM_EMAIL"))
            .unwrap_or_else(|_| env::var("SMTP_USERNAME").unwrap_or_default());
        let from_name = env::var("EMAIL_FROM_NAME")
            .or_else(|_| env::var("SMTP_FROM_NAME"))
            .unwrap_or_else(|_| "Assetar".to_string());
        let app_url = env::var("APP_URL")
            .or_else(|_| env::var("BASE_URL"))
            .unwrap_or_else(|_| "http://localhost:5173".to_string());

        let provider = if let Ok(api_key) = env::var("BREVO_API_KEY") {
            let client = Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| {
                    EmailError::ConfigError(format!("Failed to build Brevo HTTP client: {}", e))
                })?;

            EmailProvider::Brevo { api_key, client }
        } else {
            let host = env::var("SMTP_HOST").unwrap_or_else(|_| "smtp.gmail.com".to_string());
            let port = env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .map_err(|e| EmailError::ConfigError(format!("Invalid SMTP_PORT: {}", e)))?;
            let username = env::var("SMTP_USERNAME")
                .map_err(|_| EmailError::ConfigError("SMTP_USERNAME not set".to_string()))?;
            let password = env::var("SMTP_PASSWORD")
                .map_err(|_| EmailError::ConfigError("SMTP_PASSWORD not set".to_string()))?;

            EmailProvider::Smtp {
                host,
                port,
                username,
                password,
            }
        };

        Ok(Self {
            provider,
            from_email,
            from_name,
            app_url,
        })
    }

    pub fn provider_name(&self) -> &'static str {
        match &self.provider {
            EmailProvider::Brevo { .. } => "brevo",
            EmailProvider::Smtp { .. } => "smtp",
        }
    }

    pub async fn send_verification_email(
        &self,
        to_email: &str,
        username: &str,
        token: &str,
    ) -> Result<(), EmailError> {
        let verification_link =
            format!("{}/activate/{}", self.app_url.trim_end_matches('/'), token);

        let subject = "Confirm your e-mail";
        let html_body = format!(
            r#"
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body>
    <p>Hi, <strong>{}</strong>!</p>
    <p>To activate your account, follow the link below:</p>
    <p><a href="{}">Activate Account</a></p>
    <p>If clicking the link above doesn't work, please copy and paste the URL below in a new browser window instead:</p>
    <p>{}</p>
    <p>This link is valid for 24 hours. If not activated within this time limit you will need to register again.</p>
    <p>MANAGE NOTIFICATIONS</p>
</body>
</html>
            "#,
            username, verification_link, verification_link
        );

        let text_body = format!(
            "Hi, {}!\n\nTo activate your account, follow the link below:\n\n{}\n\nIf clicking the link above doesn't work, please copy and paste the URL below in a new browser window instead:\n\n{}\n\nThis link is valid for 24 hours. If not activated within this time limit you will need to register again.\n\nMANAGE NOTIFICATIONS",
            username, verification_link, verification_link
        );

        self.send_email(to_email, username, subject, &html_body, &text_body)
            .await
    }

    pub async fn send_password_reset_email(
        &self,
        to_email: &str,
        username: &str,
        token: &str,
    ) -> Result<(), EmailError> {
        let reset_link = format!("{}/reset-password/{}", self.app_url.trim_end_matches('/'), token);
        let subject = "Reset your password";
        let html_body = format!(
            r#"
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body>
    <p>Hi, <strong>{}</strong>!</p>
    <p>You requested a password reset. Use the link below to continue:</p>
    <p><a href="{}">Reset Password</a></p>
    <p>If clicking the link above doesn't work, copy and paste this URL into your browser:</p>
    <p>{}</p>
    <p>This link is valid for 1 hour.</p>
</body>
</html>
            "#,
            username, reset_link, reset_link
        );

        let text_body = format!(
            "Hi, {}!\n\nYou requested a password reset. Use the link below to continue:\n\n{}\n\nIf clicking the link above doesn't work, copy and paste this URL into your browser:\n\n{}\n\nThis link is valid for 1 hour.",
            username, reset_link, reset_link
        );

        self.send_email(to_email, username, subject, &html_body, &text_body)
            .await
    }

    async fn send_email(
        &self,
        to: &str,
        to_name: &str,
        subject: &str,
        html_body: &str,
        text_body: &str,
    ) -> Result<(), EmailError> {
        match &self.provider {
            EmailProvider::Brevo { api_key, client } => {
                self.send_email_via_brevo(
                    client, api_key, to, to_name, subject, html_body, text_body,
                )
                .await
            }
            EmailProvider::Smtp {
                host,
                port,
                username,
                password,
            } => {
                self.send_email_via_smtp(
                    host, *port, username, password, to, subject, html_body, text_body,
                )
                .await
            }
        }
    }

    async fn send_email_via_brevo(
        &self,
        client: &Client,
        api_key: &str,
        to: &str,
        to_name: &str,
        subject: &str,
        html_body: &str,
        text_body: &str,
    ) -> Result<(), EmailError> {
        let payload = BrevoSendEmailRequest {
            sender: BrevoSender {
                email: &self.from_email,
                name: &self.from_name,
            },
            to: vec![BrevoRecipient {
                email: to,
                name: to_name,
            }],
            subject,
            html_content: html_body,
            text_content: text_body,
        };

        let response = client
            .post(format!("{}/smtp/email", BREVO_API_BASE_URL))
            .header("accept", "application/json")
            .header("api-key", api_key)
            .json(&payload)
            .send()
            .await
            .map_err(|e| EmailError::SendError(format!("Brevo request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(EmailError::SendError(format!(
                "Brevo send failed with status {}: {}",
                status, body
            )));
        }

        tracing::info!("✉️  Verification email sent to: {} via Brevo", to);
        Ok(())
    }

    async fn send_email_via_smtp(
        &self,
        smtp_host: &str,
        smtp_port: u16,
        smtp_username: &str,
        smtp_password: &str,
        to: &str,
        subject: &str,
        html_body: &str,
        text_body: &str,
    ) -> Result<(), EmailError> {
        use lettre::{
            message::{header::ContentType, Mailbox, MultiPart},
            transport::smtp::authentication::Credentials,
            AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
        };

        let from_mailbox: Mailbox = format!("{} <{}>", self.from_name, self.from_email)
            .parse()
            .map_err(|e| EmailError::ConfigError(format!("Invalid from email: {}", e)))?;

        let to_mailbox: Mailbox = to
            .parse()
            .map_err(|e| EmailError::SendError(format!("Invalid to email: {}", e)))?;

        let email = Message::builder()
            .from(from_mailbox)
            .to(to_mailbox)
            .subject(subject)
            .multipart(
                MultiPart::alternative()
                    .singlepart(
                        lettre::message::SinglePart::builder()
                            .header(ContentType::TEXT_PLAIN)
                            .body(text_body.to_string()),
                    )
                    .singlepart(
                        lettre::message::SinglePart::builder()
                            .header(ContentType::TEXT_HTML)
                            .body(html_body.to_string()),
                    ),
            )
            .map_err(|e| EmailError::SendError(format!("Failed to build email: {}", e)))?;

        let creds = Credentials::new(smtp_username.to_string(), smtp_password.to_string());

        let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(smtp_host)
            .map_err(|e| EmailError::ConfigError(format!("SMTP relay error: {}", e)))?
            .port(smtp_port)
            .credentials(creds)
            .build();

        mailer
            .send(email)
            .await
            .map_err(|e| EmailError::SendError(format!("Failed to send email: {}", e)))?;

        tracing::info!("✉️  Verification email sent to: {} via SMTP", to);
        Ok(())
    }
}
