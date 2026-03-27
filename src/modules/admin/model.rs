pub const DEFAULT_ADMIN_ID: &str = "assetar-admin";
pub const DEFAULT_ADMIN_EMAIL: &str = "bezaleeldennis@assetar.com";
pub const DEFAULT_ADMIN_PASSWORD: &str = "assetarexchange";

#[derive(Debug, Clone)]
pub struct AdminAccount {
    pub id: String,
    pub email: String,
    pub password: String,
}

impl AdminAccount {
    pub fn from_env() -> Self {
        Self {
            id: std::env::var("ADMIN_ID").unwrap_or_else(|_| DEFAULT_ADMIN_ID.to_string()),
            email: std::env::var("ADMIN_EMAIL").unwrap_or_else(|_| DEFAULT_ADMIN_EMAIL.to_string()),
            password: std::env::var("ADMIN_PASSWORD")
                .unwrap_or_else(|_| DEFAULT_ADMIN_PASSWORD.to_string()),
        }
    }

    pub fn matches_credentials(&self, email: &str, password: &str) -> bool {
        self.email.eq_ignore_ascii_case(email.trim()) && self.password == password
    }
}
