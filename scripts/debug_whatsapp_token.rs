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

        vars.insert(
            key.trim().to_string(),
            value.trim().trim_matches('"').to_string(),
        );
    }

    Ok(vars)
}

fn require_var(vars: &HashMap<String, String>, key: &str) -> Result<String, Box<dyn Error>> {
    match vars.get(key).cloned() {
        Some(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(format!("Missing or empty env var: {}", key).into()),
    }
}

fn graph_get(url: &str) -> Result<(u16, String), Box<dyn Error>> {
    let output = Command::new("curl")
        .arg("-sS")
        .arg("-X")
        .arg("GET")
        .arg(url)
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

    Err("Failed to parse HTTP status from output".into())
}

fn get_json_bool(body: &str, key: &str) -> Option<bool> {
    let needle = format!("\"{}\":", key);
    let start = body.find(&needle)? + needle.len();
    let tail = body[start..].trim_start();
    if tail.starts_with("true") {
        Some(true)
    } else if tail.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn get_json_i64(body: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{}\":", key);
    let start = body.find(&needle)? + needle.len();
    let tail = body[start..].trim_start();
    let mut end = 0usize;
    for (i, ch) in tail.char_indices() {
        if !(ch.is_ascii_digit() || ch == '-') {
            break;
        }
        end = i + ch.len_utf8();
    }
    if end == 0 {
        return None;
    }
    tail[..end].parse::<i64>().ok()
}

fn get_json_string(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\":\"", key);
    let start = body.find(&needle)? + needle.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn main() -> Result<(), Box<dyn Error>> {
    let vars = parse_env_file(".env")?;

    let app_id = require_var(&vars, "META_APP_ID")?;
    let app_secret = require_var(&vars, "META_APP_SECRET")?;
    let token = require_var(&vars, "WHATSAPP_ACCESS_TOKEN")?;

    let app_access_token = format!("{}|{}", app_id, app_secret);
    let url = format!(
        "https://graph.facebook.com/v21.0/debug_token?input_token={}&access_token={}",
        token, app_access_token
    );

    println!("Debugging WhatsApp token via Meta /debug_token...");
    println!("- META_APP_ID: {}", app_id);
    println!("- META_APP_SECRET: present");
    println!("- WHATSAPP_ACCESS_TOKEN: present ({} chars)", token.len());

    let (status, body) = graph_get(&url)?;
    println!("HTTP status: {}", status);
    println!("Raw response: {}", body);

    if status != 200 {
        return Err("Meta debug_token request failed".into());
    }

    let is_valid = get_json_bool(&body, "is_valid").unwrap_or(false);
    let expires_at = get_json_i64(&body, "expires_at").unwrap_or(0);
    let issued_at = get_json_i64(&body, "issued_at").unwrap_or(0);
    let token_type = get_json_string(&body, "type").unwrap_or_else(|| "unknown".to_string());
    let app_name = get_json_string(&body, "application").unwrap_or_else(|| "unknown".to_string());

    println!("\nParsed token info:");
    println!("- is_valid: {}", is_valid);
    println!("- type: {}", token_type);
    println!("- application: {}", app_name);

    if issued_at > 0 {
        println!("- issued_at (unix): {}", issued_at);
    } else {
        println!("- issued_at: not provided");
    }

    if expires_at == 0 {
        println!("- expires_at: 0 (Meta reports this token as non-expiring or no expiry info)");
    } else {
        println!("- expires_at (unix): {}", expires_at);
    }

    Ok(())
}
