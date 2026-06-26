#!/usr/bin/env python3

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
WHATSAPP_ROUTE_FILES = [
    ROOT / "src/modules/whatsapp/controller.rs",
    ROOT / "src/modules/whatsapp/routes.rs",
    ROOT / "src/services/whatsapp/config.rs",
]

REQUIRED_SCOPES = {
    "whatsapp_business_management",
    "whatsapp_business_messaging",
}


@dataclass
class CheckResult:
    level: str
    name: str
    detail: str


def load_env() -> dict[str, str]:
    values: dict[str, str] = {}

    if ENV_PATH.exists():
        for raw_line in ENV_PATH.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"')

    for key, value in os.environ.items():
        if value.strip():
            values[key] = value.strip()

    return values


def require_var(values: dict[str, str], key: str) -> str:
    value = values.get(key, "").strip().strip('"')
    if not value:
        raise RuntimeError(f"Missing or empty env var: {key}")
    return value


def optional_var(values: dict[str, str], key: str) -> Optional[str]:
    value = values.get(key, "").strip().strip('"')
    return value or None


def is_numeric(value: str) -> bool:
    return value.isdigit()


def looks_like_hex_secret(value: str) -> bool:
    return len(value) >= 32 and all(ch in "0123456789abcdefABCDEF" for ch in value)


def looks_like_graph_version(value: str) -> bool:
    if not value.startswith("v"):
        return False
    body = value[1:]
    return bool(body) and all(ch.isdigit() or ch == "." for ch in body)


def mask_token(value: str) -> str:
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}...{value[-4:]}"


def format_unix_timestamp(value: int) -> str:
    if value <= 0:
        return "not provided"
    dt = datetime.fromtimestamp(value, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def is_test_phone_number(display_phone: str, verified_name: str) -> bool:
    normalized_phone = display_phone.strip()
    normalized_name = verified_name.strip().lower()
    return normalized_phone.startswith("+1 555") or "test" in normalized_name


def graph_get(
    url: str,
    bearer_token: Optional[str] = None,
    params: Optional[dict[str, str]] = None,
) -> tuple[int, dict]:
    if params:
        query = urllib.parse.urlencode(params)
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{query}"

    request = urllib.request.Request(url, method="GET")
    if bearer_token:
        request.add_header("Authorization", f"Bearer {bearer_token}")

    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"raw": body}
        return exc.code, parsed


def print_results(results: list[CheckResult]) -> None:
    for result in results:
        print(f"[{result.level}] {result.name}: {result.detail}")

    passes = sum(1 for result in results if result.level == "PASS")
    warns = sum(1 for result in results if result.level == "WARN")
    fails = sum(1 for result in results if result.level == "FAIL")
    print()
    print(f"Summary: {passes} passed, {warns} warned, {fails} failed")


def main() -> int:
    try:
        values = load_env()
        meta_app_id = require_var(values, "META_APP_ID")
        meta_app_secret = require_var(values, "META_APP_SECRET")
        waba_id = require_var(values, "WHATSAPP_BUSINESS_ACCOUNT_ID")
        phone_number_id = require_var(values, "WHATSAPP_PHONE_NUMBER_ID")
        access_token = require_var(values, "WHATSAPP_ACCESS_TOKEN")
        verify_token = require_var(values, "WHATSAPP_VERIFY_TOKEN")
        graph_version = optional_var(values, "WHATSAPP_GRAPH_VERSION") or "v21.0"
        public_base_url = (
            optional_var(values, "PUBLIC_BACKEND_URL")
            or optional_var(values, "RENDER_EXTERNAL_URL")
            or optional_var(values, "API_BASE_URL")
        )
    except RuntimeError as exc:
        print(f"[FAIL] Env loading: {exc}")
        return 1

    results: list[CheckResult] = []

    results.append(
        CheckResult(
            "PASS" if is_numeric(meta_app_id) else "FAIL",
            "META_APP_ID format",
            meta_app_id if is_numeric(meta_app_id) else "META_APP_ID must be numeric",
        )
    )
    results.append(
        CheckResult(
            "PASS" if looks_like_hex_secret(meta_app_secret) else "FAIL",
            "META_APP_SECRET format",
            "shape looks valid" if looks_like_hex_secret(meta_app_secret) else "META_APP_SECRET does not look like a valid app secret",
        )
    )
    results.append(
        CheckResult(
            "PASS" if is_numeric(waba_id) else "FAIL",
            "WHATSAPP_BUSINESS_ACCOUNT_ID format",
            waba_id if is_numeric(waba_id) else "WABA id must be numeric",
        )
    )
    results.append(
        CheckResult(
            "PASS" if is_numeric(phone_number_id) else "FAIL",
            "WHATSAPP_PHONE_NUMBER_ID format",
            phone_number_id if is_numeric(phone_number_id) else "phone number id must be numeric",
        )
    )
    results.append(
        CheckResult(
            "PASS" if len(access_token) >= 40 else "FAIL",
            "WHATSAPP_ACCESS_TOKEN format",
            f"present ({mask_token(access_token)})" if len(access_token) >= 40 else "access token looks too short",
        )
    )
    results.append(
        CheckResult(
            "PASS" if len(verify_token) >= 24 else "WARN",
            "WHATSAPP_VERIFY_TOKEN strength",
            f"{len(verify_token)} characters" if len(verify_token) >= 24 else "verify token is short; use a longer random string",
        )
    )
    results.append(
        CheckResult(
            "PASS" if looks_like_graph_version(graph_version) else "FAIL",
            "WHATSAPP_GRAPH_VERSION format",
            graph_version if looks_like_graph_version(graph_version) else f"invalid graph version: {graph_version}",
        )
    )

    if public_base_url and public_base_url.startswith("https://"):
        results.append(CheckResult("PASS", "Webhook callback base URL", f"public HTTPS base detected: {public_base_url}"))
    elif public_base_url:
        results.append(
            CheckResult(
                "WARN",
                "Webhook callback base URL",
                f"current base URL is not HTTPS ({public_base_url}). Meta webhook verification needs a public HTTPS callback.",
            )
        )
    else:
        results.append(
            CheckResult(
                "WARN",
                "Webhook callback base URL",
                "PUBLIC_BACKEND_URL, API_BASE_URL, or RENDER_EXTERNAL_URL is not set",
            )
        )

    webhook_url = None
    if public_base_url:
        webhook_url = f"{public_base_url.rstrip('/')}/whatsapp/webhook"

    if all(path.exists() for path in WHATSAPP_ROUTE_FILES):
        detail = "backend webhook implementation is present in this repo"
        if webhook_url:
            detail = f"{detail}; expected callback URL: {webhook_url}"
        results.append(CheckResult("PASS", "Backend webhook implementation", detail))
    else:
        results.append(
            CheckResult(
                "WARN",
                "Backend webhook implementation",
                "WhatsApp webhook files are missing from this checkout.",
            )
        )

    if any(result.level == "FAIL" for result in results):
        print_results(results)
        return 1

    debug_url = f"https://graph.facebook.com/{graph_version}/debug_token"
    app_access_token = f"{meta_app_id}|{meta_app_secret}"

    try:
        debug_status, debug_json = graph_get(
            debug_url,
            params={"input_token": access_token, "access_token": app_access_token},
        )
    except Exception as exc:
        results.append(CheckResult("FAIL", "Meta debug_token", f"request failed: {exc}"))
        print_results(results)
        return 1

    if debug_status != 200:
        results.append(CheckResult("FAIL", "Meta debug_token", f"HTTP {debug_status}: {debug_json}"))
        print_results(results)
        return 1

    token_data = debug_json.get("data", {})
    is_valid = bool(token_data.get("is_valid"))
    token_app_id = str(token_data.get("app_id", "unknown"))
    token_type = str(token_data.get("type", "unknown"))
    expires_at = int(token_data.get("expires_at", 0) or 0)
    scopes = set(token_data.get("scopes", []) or [])

    if not is_valid:
        results.append(CheckResult("FAIL", "Meta debug_token", f"token is invalid: {debug_json}"))
        print_results(results)
        return 1

    results.append(
        CheckResult(
            "PASS" if token_app_id == meta_app_id else "FAIL",
            "Token app binding",
            f"token belongs to app {token_app_id} ({token_type})"
            if token_app_id == meta_app_id
            else f"token belongs to app {token_app_id} instead of configured app {meta_app_id}",
        )
    )
    if token_type.upper() != "SYSTEM_USER":
        results.append(
            CheckResult(
                "WARN",
                "Token type",
                f"Meta reports token type {token_type}. Use a long-lived SYSTEM_USER token for production.",
            )
        )
    else:
        results.append(CheckResult("PASS", "Token type", token_type))

    if expires_at > 0:
        seconds_remaining = expires_at - int(datetime.now(tz=timezone.utc).timestamp())
        if seconds_remaining <= 0:
            results.append(CheckResult("FAIL", "Token expiry", f"token expired at {format_unix_timestamp(expires_at)}"))
        elif seconds_remaining < 7 * 24 * 60 * 60:
            results.append(
                CheckResult(
                    "WARN",
                    "Token expiry",
                    f"token expires soon at {format_unix_timestamp(expires_at)}",
                )
            )
        else:
            results.append(CheckResult("PASS", "Token expiry", format_unix_timestamp(expires_at)))
    else:
        results.append(CheckResult("PASS", "Token expiry", format_unix_timestamp(expires_at)))

    missing_scopes = sorted(REQUIRED_SCOPES - scopes)
    if missing_scopes:
        current_scopes = ", ".join(sorted(scopes)) if scopes else "(none returned)"
        results.append(
            CheckResult(
                "FAIL",
                "Token scopes",
                f"missing required scopes: {', '.join(missing_scopes)}. Current scopes: {current_scopes}",
            )
        )
    else:
        results.append(CheckResult("PASS", "Token scopes", f"required scopes present: {', '.join(sorted(REQUIRED_SCOPES))}"))

    try:
        phone_status, phone_json = graph_get(
            f"https://graph.facebook.com/{graph_version}/{phone_number_id}?fields=id,display_phone_number,verified_name,quality_rating,code_verification_status,name_status,status",
            bearer_token=access_token,
        )
    except Exception as exc:
        results.append(CheckResult("FAIL", "Phone number access", f"request failed: {exc}"))
        print_results(results)
        return 1

    if phone_status == 200:
        display_phone = phone_json.get("display_phone_number", "unknown")
        verified_name = phone_json.get("verified_name", "unknown")
        results.append(CheckResult("PASS", "Phone number access", f"{display_phone} / {verified_name}"))
        if is_test_phone_number(str(display_phone), str(verified_name)):
            results.append(
                CheckResult(
                    "WARN",
                    "Phone number mode",
                    "Meta returned a test phone number. Move to a real production sender before launch.",
                )
            )
    else:
        results.append(CheckResult("FAIL", "Phone number access", f"HTTP {phone_status}: {phone_json}"))

    try:
        waba_status, waba_json = graph_get(
            f"https://graph.facebook.com/{graph_version}/{waba_id}?fields=id,name,currency,timezone_id",
            bearer_token=access_token,
        )
    except Exception as exc:
        results.append(CheckResult("FAIL", "WABA access", f"request failed: {exc}"))
        print_results(results)
        return 1

    if waba_status == 200:
        results.append(CheckResult("PASS", "WABA access", f"{waba_id} ({waba_json.get('name', 'unknown')})"))
    else:
        results.append(CheckResult("FAIL", "WABA access", f"HTTP {waba_status}: {waba_json}"))

    try:
        membership_status, membership_json = graph_get(
            f"https://graph.facebook.com/{graph_version}/{waba_id}/phone_numbers?fields=id,display_phone_number,verified_name,quality_rating,code_verification_status,name_status,status",
            bearer_token=access_token,
        )
    except Exception as exc:
        results.append(CheckResult("FAIL", "Phone belongs to WABA", f"request failed: {exc}"))
        print_results(results)
        return 1

    if membership_status == 200:
        data = membership_json.get("data", []) or []
        is_member = any(item.get("id") == phone_number_id for item in data if isinstance(item, dict))
        results.append(
            CheckResult(
                "PASS" if is_member else "FAIL",
                "Phone belongs to WABA",
                f"phone number {phone_number_id} is attached to WABA {waba_id}"
                if is_member
                else f"phone number {phone_number_id} was not found under WABA {waba_id}",
            )
        )
    else:
        results.append(CheckResult("FAIL", "Phone belongs to WABA", f"HTTP {membership_status}: {membership_json}"))

    try:
        subscribed_status, subscribed_json = graph_get(
            f"https://graph.facebook.com/{graph_version}/{waba_id}/subscribed_apps",
            bearer_token=access_token,
        )
    except Exception as exc:
        results.append(CheckResult("WARN", "WABA subscribed apps", f"request failed: {exc}"))
        print_results(results)
        return 1 if any(result.level == "FAIL" for result in results) else 0

    if subscribed_status == 200:
        subscribed_data = subscribed_json.get("data", []) or []
        is_subscribed = any(
            (
                isinstance(item, dict)
                and (
                    item.get("id") == meta_app_id
                    or (
                        isinstance(item.get("whatsapp_business_api_data"), dict)
                        and item["whatsapp_business_api_data"].get("id") == meta_app_id
                    )
                )
            )
            for item in subscribed_data
        )
        results.append(
            CheckResult(
                "PASS" if is_subscribed else "WARN",
                "WABA subscribed apps",
                f"app {meta_app_id} is subscribed to this WABA"
                if is_subscribed
                else f"app {meta_app_id} was not found in subscribed_apps. Inbound webhook delivery may not work until the app is subscribed.",
            )
        )
    else:
        results.append(CheckResult("WARN", "WABA subscribed apps", f"HTTP {subscribed_status}: {subscribed_json}"))

    print_results(results)
    return 1 if any(result.level == "FAIL" for result in results) else 0


if __name__ == "__main__":
    sys.exit(main())
