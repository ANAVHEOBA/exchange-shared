use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use std::time::Instant;

use super::MetricsRegistry;

/// Middleware to collect HTTP request metrics
pub async fn metrics_middleware(
    State(metrics): State<Arc<MetricsRegistry>>,
    req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = normalize_path(req.uri().path());
    
    // Process request
    let response = next.run(req).await;
    
    // Record metrics
    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    
    // HTTP request counter
    metrics
        .http_requests_total
        .with_label_values(&[&method, &path, &status])
        .inc();
    
    // HTTP request duration
    metrics
        .http_request_duration_seconds
        .with_label_values(&[&method, &path])
        .observe(duration);
    
    response
}

/// Normalize path to reduce cardinality
/// Converts /api/swap/123 -> /api/swap/:id
fn normalize_path(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').collect();
    let mut normalized = Vec::new();
    
    for segment in segments {
        if segment.is_empty() {
            continue;
        }
        
        // Check if segment looks like an ID (UUID, number, hash)
        if is_id_like(segment) {
            normalized.push(":id");
        } else {
            normalized.push(segment);
        }
    }
    
    format!("/{}", normalized.join("/"))
}

/// Check if a segment looks like an ID
fn is_id_like(segment: &str) -> bool {
    // UUID pattern
    if segment.len() == 36 && segment.chars().filter(|c| *c == '-').count() == 4 {
        return true;
    }
    
    // All digits (numeric ID)
    if segment.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    
    // Hex hash (40+ chars)
    if segment.len() >= 40 && segment.chars().all(|c| c.is_ascii_hexdigit()) {
        return true;
    }
    
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/api/swap"), "/api/swap");
        assert_eq!(normalize_path("/api/swap/123"), "/api/swap/:id");
        assert_eq!(
            normalize_path("/api/swap/550e8400-e29b-41d4-a716-446655440000"),
            "/api/swap/:id"
        );
        assert_eq!(
            normalize_path("/api/swap/0x1234567890abcdef1234567890abcdef12345678"),
            "/api/swap/:id"
        );
        assert_eq!(normalize_path("/api/swap/create"), "/api/swap/create");
    }

    #[test]
    fn test_is_id_like() {
        assert!(is_id_like("123"));
        assert!(is_id_like("550e8400-e29b-41d4-a716-446655440000"));
        assert!(is_id_like("0x1234567890abcdef1234567890abcdef12345678"));
        assert!(!is_id_like("create"));
        assert!(!is_id_like("status"));
    }
}
