use std::env;

#[derive(Debug, Clone)]
pub struct WhatsAppConfig {
    pub app_id: String,
    pub app_secret: String,
    pub business_account_id: String,
    pub phone_number_id: String,
    pub access_token: String,
    pub verify_token: String,
    pub graph_version: String,
    pub public_base_url: Option<String>,
}

impl WhatsAppConfig {
    pub fn from_env() -> Result<Option<Self>, String> {
        dotenvy::dotenv().ok();

        let keys = [
            "META_APP_ID",
            "META_APP_SECRET",
            "WHATSAPP_BUSINESS_ACCOUNT_ID",
            "WHATSAPP_PHONE_NUMBER_ID",
            "WHATSAPP_ACCESS_TOKEN",
            "WHATSAPP_VERIFY_TOKEN",
        ];

        let missing = keys
            .iter()
            .filter(|key| env::var(key).unwrap_or_default().trim().is_empty())
            .map(|key| (*key).to_string())
            .collect::<Vec<_>>();

        if missing.len() == keys.len() {
            return Ok(None);
        }

        if !missing.is_empty() {
            return Err(format!(
                "partial WhatsApp configuration detected; missing: {}",
                missing.join(", ")
            ));
        }

        let graph_version = env::var("WHATSAPP_GRAPH_VERSION")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "v21.0".to_string());

        let public_base_url = env::var("PUBLIC_BACKEND_URL")
            .ok()
            .or_else(|| env::var("RENDER_EXTERNAL_URL").ok())
            .or_else(|| env::var("API_BASE_URL").ok())
            .map(|value| value.trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty());

        Ok(Some(Self {
            app_id: env::var("META_APP_ID").unwrap_or_default(),
            app_secret: env::var("META_APP_SECRET").unwrap_or_default(),
            business_account_id: env::var("WHATSAPP_BUSINESS_ACCOUNT_ID").unwrap_or_default(),
            phone_number_id: env::var("WHATSAPP_PHONE_NUMBER_ID").unwrap_or_default(),
            access_token: env::var("WHATSAPP_ACCESS_TOKEN").unwrap_or_default(),
            verify_token: env::var("WHATSAPP_VERIFY_TOKEN").unwrap_or_default(),
            graph_version,
            public_base_url,
        }))
    }

    pub fn messages_endpoint(&self) -> String {
        format!(
            "https://graph.facebook.com/{}/{}/messages",
            self.graph_version, self.phone_number_id
        )
    }

    pub fn webhook_path(&self) -> &'static str {
        "/whatsapp/webhook"
    }

    pub fn webhook_url(&self) -> Option<String> {
        self.public_base_url
            .as_ref()
            .map(|base| format!("{}{}", base, self.webhook_path()))
    }
}
