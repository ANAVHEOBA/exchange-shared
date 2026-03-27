use axum::{
    body::Body,
    extract::FromRequestParts,
    http::{
        header::{COOKIE, SET_COOKIE},
        request::Parts,
        HeaderName, HeaderValue, Request, StatusCode,
    },
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

const CLIENT_ID_COOKIE_NAME: &str = "assetar_client_id";
const CLIENT_ID_HEADER_NAME: &str = "x-client-id";
const CLIENT_ID_COOKIE_MAX_AGE_SECONDS: u64 = 60 * 60 * 24 * 365;

#[derive(Debug, Clone)]
pub struct AnonymousClientId(pub String);

impl AnonymousClientId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<S> FromRequestParts<S> for AnonymousClientId
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<AnonymousClientId>().cloned().ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Anonymous client identity missing from request",
        ))
    }
}

pub async fn attach_client_identity(mut request: Request<Body>, next: Next) -> Response {
    let existing_client_id = request
        .headers()
        .get(CLIENT_ID_HEADER_NAME)
        .and_then(|value| value.to_str().ok())
        .and_then(normalize_client_id)
        .or_else(|| {
            request
                .headers()
                .get(COOKIE)
                .and_then(|value| value.to_str().ok())
                .and_then(extract_client_id_from_cookie_header)
        });

    let client_id = existing_client_id.unwrap_or_else(generate_client_id);

    request
        .extensions_mut()
        .insert(AnonymousClientId(client_id.clone()));

    let mut response = next.run(request).await;
    let headers = response.headers_mut();

    if let Ok(value) = HeaderValue::from_str(&client_id) {
        headers.insert(HeaderName::from_static(CLIENT_ID_HEADER_NAME), value);
    }

    let cookie_value = format!(
        "{}={}; Path=/; Max-Age={}; HttpOnly; SameSite=Lax",
        CLIENT_ID_COOKIE_NAME, client_id, CLIENT_ID_COOKIE_MAX_AGE_SECONDS
    );
    if let Ok(value) = HeaderValue::from_str(&cookie_value) {
        headers.append(SET_COOKIE, value);
    }

    response
}

fn extract_client_id_from_cookie_header(header_value: &str) -> Option<String> {
    header_value.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        if name == CLIENT_ID_COOKIE_NAME {
            normalize_client_id(value)
        } else {
            None
        }
    })
}

fn normalize_client_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    Uuid::parse_str(trimmed).ok().map(|id| id.to_string())
}

fn generate_client_id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::{extract_client_id_from_cookie_header, normalize_client_id};

    #[test]
    fn extracts_client_id_from_cookie_header() {
        let client_id = "cf38d48d-f4d4-45f8-b6cc-5f7cb85a87ae";
        let cookie_header = format!("foo=bar; assetar_client_id={}; hello=world", client_id);

        assert_eq!(
            extract_client_id_from_cookie_header(&cookie_header).as_deref(),
            Some(client_id)
        );
    }

    #[test]
    fn rejects_invalid_client_ids() {
        assert!(normalize_client_id("not-a-uuid").is_none());
    }
}
