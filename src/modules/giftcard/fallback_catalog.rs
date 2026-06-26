use regex::Regex;
use std::sync::OnceLock;

use super::schema::GiftCardProductResponse;

static GB_CATALOG: OnceLock<Vec<GiftCardProductResponse>> = OnceLock::new();

pub fn fallback_catalog(country: Option<&str>) -> Vec<GiftCardProductResponse> {
    let normalized = country.map(|value| value.trim().to_ascii_uppercase());

    match normalized.as_deref() {
        Some("GB") | Some("UK") => gb_catalog().clone(),
        _ => Vec::new(),
    }
}

fn gb_catalog() -> &'static Vec<GiftCardProductResponse> {
    GB_CATALOG.get_or_init(parse_gb_catalog)
}

fn parse_gb_catalog() -> Vec<GiftCardProductResponse> {
    let product_regex = Regex::new(
        r#"(?s)<a href="/en/giftcard/All%20Gift%20Cards/(?P<id>\d+)"[^>]*class="link product_option"[^>]*>\s*<img src="(?P<img>[^"]+)"[^>]*>\s*<div[^>]*>(?P<name>[^<]+)</div>\s*<div[^>]*>(?P<range>[^<]+)</div>"#,
    )
    .expect("giftcard product regex must compile");

    let mut cards = Vec::new();

    for capture in product_regex.captures_iter(include_str!("../../../flow.html")) {
        let Some(product_id) = capture.name("id").map(|value| value.as_str().trim()) else {
            continue;
        };
        let Some(image_url) = capture.name("img").map(|value| value.as_str().trim()) else {
            continue;
        };
        let Some(name) = capture.name("name").map(|value| value.as_str().trim()) else {
            continue;
        };
        let Some(range_label) = capture.name("range").map(|value| value.as_str().trim()) else {
            continue;
        };

        let values = parse_numeric_values(range_label);
        let min_amount = values.first().copied();
        let max_amount = values.last().copied();
        let denominations = if values.len() == 1 {
            Some(values)
        } else {
            None
        };

        cards.push(GiftCardProductResponse {
            product_id: product_id.to_string(),
            name: name.to_string(),
            category: Some("All Gift Cards".to_string()),
            description: None,
            terms_and_conditions: None,
            how_to_use: None,
            expiry_and_validity: None,
            card_image_url: Some(image_url.to_string()),
            country: Some("GB".to_string()),
            min_amount,
            max_amount,
            denominations,
        });
    }

    cards
}

fn parse_numeric_values(label: &str) -> Vec<f64> {
    let numeric_regex =
        Regex::new(r#"\d+(?:,\d{3})*(?:\.\d+)?"#).expect("giftcard numeric regex must compile");

    numeric_regex
        .find_iter(label)
        .filter_map(|value| value.as_str().replace(',', "").parse::<f64>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::fallback_catalog;

    #[test]
    fn gb_fallback_catalog_contains_products_and_images() {
        let cards = fallback_catalog(Some("GB"));
        assert!(!cards.is_empty(), "expected fallback giftcard catalog");

        let airbnb = cards
            .iter()
            .find(|card| card.product_id == "8215")
            .expect("expected Airbnb card in fallback catalog");

        assert_eq!(airbnb.name, "Airbnb");
        assert_eq!(airbnb.min_amount, Some(50.0));
        assert_eq!(airbnb.max_amount, Some(100.0));
        assert_eq!(
            airbnb.card_image_url.as_deref(),
            Some("https://gift.runa.io/static/product_assets/AIRBNB-GB/AIRBNB-GB-card.png")
        );
    }
}
