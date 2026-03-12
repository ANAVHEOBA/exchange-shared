use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::process::Command;

fn parse_env_file(path: &str) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let content = fs::read_to_string(path)?;
    let mut vars = HashMap::new();

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        let clean_key = key.trim().to_string();
        let clean_value = value.trim().trim_matches('"').to_string();
        vars.insert(clean_key, clean_value);
    }

    Ok(vars)
}

fn require_var(vars: &HashMap<String, String>, key: &str) -> Result<String, Box<dyn Error>> {
    match vars.get(key).cloned() {
        Some(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(format!("Missing or empty env var: {}", key).into()),
    }
}

fn is_numeric(s: &str) -> bool {
    s.chars().all(|c| c.is_ascii_digit())
}

fn graph_get(url: &str, token: &str) -> Result<(u16, String), Box<dyn Error>> {
    let output = Command::new("curl")
        .arg("-sS")
        .arg("-X")
        .arg("GET")
        .arg(url)
        .arg("-H")
        .arg(format!("Authorization: Bearer {}", token))
        .arg("-w")
        .arg("\nHTTP_STATUS:%{http_code}")
        .output()?;

    if !output.status.success() {
        return Err("curl command failed".into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let marker = "\nHTTP_STATUS:";

    if let Some(pos) = stdout.rfind(marker) {
        let body = stdout[..pos].trim().to_string();
        let status_raw = stdout[pos + marker.len()..].trim();
        let status = status_raw.parse::<u16>()?;
        return Ok((status, body));
    }

    Err("Failed to parse HTTP status from curl output".into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let vars = parse_env_file(".env")?;

    let waba_id = require_var(&vars, "WHATSAPP_BUSINESS_ACCOUNT_ID")?;
    let phone_number_id = require_var(&vars, "WHATSAPP_PHONE_NUMBER_ID")?;
    let access_token = require_var(&vars, "WHATSAPP_ACCESS_TOKEN")?;

    if !is_numeric(&waba_id) {
        return Err("WHATSAPP_BUSINESS_ACCOUNT_ID must be numeric".into());
    }
    if !is_numeric(&phone_number_id) {
        return Err("WHATSAPP_PHONE_NUMBER_ID must be numeric".into());
    }
    if access_token.len() < 40 {
        return Err("WHATSAPP_ACCESS_TOKEN looks too short".into());
    }

    let graph_version = "v21.0";

    let phone_url = format!(
        "https://graph.facebook.com/{}/{}?fields=id,display_phone_number,verified_name",
        graph_version, phone_number_id
    );

    let waba_url = format!(
        "https://graph.facebook.com/{}/{}/phone_numbers?fields=id,display_phone_number,verified_name",
        graph_version, waba_id
    );

    println!("Checking WhatsApp env vars...");
    println!("- WHATSAPP_BUSINESS_ACCOUNT_ID: {}", waba_id);
    println!("- WHATSAPP_PHONE_NUMBER_ID: {}", phone_number_id);
    println!("- WHATSAPP_ACCESS_TOKEN: present ({} chars)", access_token.len());

    println!("\nCalling Graph API for phone number...");
    let (phone_status, phone_body) = graph_get(&phone_url, &access_token)?;
    println!("Phone endpoint status: {}", phone_status);
    println!("Phone endpoint response: {}", phone_body);

    println!("\nCalling Graph API for business account...");
    let (waba_status, waba_body) = graph_get(&waba_url, &access_token)?;
    println!("Business endpoint status: {}", waba_status);
    println!("Business endpoint response: {}", waba_body);

    if phone_status == 200 && waba_status == 200 {
        println!("\nSUCCESS: WhatsApp variables are valid and API access works.");
        Ok(())
    } else {
        Err(format!(
            "One or more API checks failed (phone_status={}, business_status={})",
            phone_status, waba_status
        )
        .into())
    }
}
