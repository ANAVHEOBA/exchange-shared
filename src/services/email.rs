use std::env;

pub struct EmailService {
    smtp_host: String,
    smtp_port: u16,
    smtp_username: String,
    smtp_password: String,
    from_email: String,
    from_name: String,
    base_url: String,
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

impl EmailService {
    pub fn from_env() -> Result<Self, EmailError> {
        Ok(Self {
            smtp_host: env::var("SMTP_HOST")
                .unwrap_or_else(|_| "smtp.gmail.com".to_string()),
            smtp_port: env::var("SMTP_PORT")
                .unwrap_or_else(|_| "587".to_string())
                .parse()
                .map_err(|e| EmailError::ConfigError(format!("Invalid SMTP_PORT: {}", e)))?,
            smtp_username: env::var("SMTP_USERNAME")
                .map_err(|_| EmailError::ConfigError("SMTP_USERNAME not set".to_string()))?,
            smtp_password: env::var("SMTP_PASSWORD")
                .map_err(|_| EmailError::ConfigError("SMTP_PASSWORD not set".to_string()))?,
            from_email: env::var("SMTP_FROM_EMAIL")
                .unwrap_or_else(|_| env::var("SMTP_USERNAME").unwrap_or_default()),
            from_name: env::var("SMTP_FROM_NAME")
                .unwrap_or_else(|_| "Trocador".to_string()),
            base_url: env::var("BASE_URL")
                .unwrap_or_else(|_| "http://localhost:3000".to_string()),
        })
    }

    pub async fn send_verification_email(
        &self,
        to_email: &str,
        username: &str,
        token: &str,
    ) -> Result<(), EmailError> {
        let verification_link = format!("{}/auth/verify-email?token={}", self.base_url, token);
        
        let subject = "Confirm your e-mail";
        let html_body = format!(
            r#"
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f5f5f5;">
    <div style="background: white; padding: 40px; border-radius: 8px;">
        <p style="font-size: 16px; margin-bottom: 20px;">Hi, <strong>{}</strong>!</p>
        
        <p style="font-size: 16px; margin-bottom: 30px;">
            To activate your account on <strong>trocador.app</strong>, follow the link below:
        </p>
        
        <div style="text-align: center; margin: 30px 0;">
            <a href="{}" style="background: #4CAF50; color: white; padding: 12px 30px; text-decoration: none; border-radius: 4px; font-size: 16px; display: inline-block;">
                Activate Account
            </a>
        </div>
        
        <p style="font-size: 14px; color: #666; margin-top: 30px;">
            If clicking the link above doesn't work, please copy and paste the URL below into your web browser instead:
        </p>
        <p style="font-size: 13px; color: #0066cc; word-break: break-all; background: #f5f5f5; padding: 10px; border-radius: 4px;">
            {}
        </p>
        
        <p style="font-size: 14px; color: #666; margin-top: 30px;">
            This link is valid for 24 hours. If not activated within this time limit you will need to register again.
        </p>
        
        <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 30px 0;">
        
        <p style="font-size: 12px; color: #999;">
            MANAGE NOTIFICATIONS
        </p>
    </div>
</body>
</html>
            "#,
            username, verification_link, verification_link
        );

        let text_body = format!(
            "Hi, {}!\n\nTo activate your account on trocador.app, follow the link below:\n\n{}\n\nIf clicking the link above doesn't work, please copy and paste the URL into your web browser instead.\n\nThis link is valid for 24 hours. If not activated within this time limit you will need to register again.",
            username, verification_link
        );

        self.send_email(to_email, subject, &html_body, &text_body).await
    }

    async fn send_email(
        &self,
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

        let creds = Credentials::new(self.smtp_username.clone(), self.smtp_password.clone());

        let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp_host)
            .map_err(|e| EmailError::ConfigError(format!("SMTP relay error: {}", e)))?
            .port(self.smtp_port)
            .credentials(creds)
            .build();

        mailer
            .send(email)
            .await
            .map_err(|e| EmailError::SendError(format!("Failed to send email: {}", e)))?;

        tracing::info!("✉️  Verification email sent to: {}", to);
        Ok(())
    }
}
