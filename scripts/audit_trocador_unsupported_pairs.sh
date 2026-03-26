#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${TROCADOR_API_BASE_URL:-https://api.trocador.app}"
DEFAULT_DELAY_MS="${TROCADOR_AUDIT_DELAY_MS:-500}"
DEFAULT_OUTPUT="${TROCADOR_AUDIT_OUTPUT:-trocador_unsupported_pair_audit.tsv}"

INPUT_FILE=""
PROBE_TRADE=0
OUTPUT_FILE="$DEFAULT_OUTPUT"
DELAY_MS="$DEFAULT_DELAY_MS"

usage() {
    cat <<'EOF'
Usage:
  scripts/audit_trocador_unsupported_pairs.sh [--input PATH] [--output PATH] [--delay-ms N] [--probe-trade]

What it does:
  1. Parses unsupported "coin not found" pairs from a previous full validateaddress run log.
  2. Fetches live /coins from Trocador.
  3. For each unsupported pair:
     - checks whether the pair still exists live in /coins
     - probes /new_rate using a small set of common source coins
     - optionally probes /new_trade using the exact derived address already recorded in the log

Notes:
  - By default this does NOT create trades.
  - --probe-trade creates real Trocador trade records, but does not move funds by itself.
  - For memo-required target coins, the script sends address_memo=0 when probing new_trade.

Examples:
  scripts/audit_trocador_unsupported_pairs.sh
  scripts/audit_trocador_unsupported_pairs.sh --input trocador_full_snapshot_validation.log --probe-trade
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --input)
            INPUT_FILE="${2:-}"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="${2:-}"
            shift 2
            ;;
        --delay-ms)
            DELAY_MS="${2:-}"
            shift 2
            ;;
        --probe-trade)
            PROBE_TRADE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

require_cmd curl
require_cmd jq
require_cmd awk

if [[ -z "${INPUT_FILE}" ]]; then
    if [[ -f "trocador_full_snapshot_validation.log" ]]; then
        INPUT_FILE="trocador_full_snapshot_validation.log"
    elif [[ -f "address.md" ]]; then
        INPUT_FILE="address.md"
    else
        echo "Could not find an input log. Use --input PATH." >&2
        exit 1
    fi
fi

if [[ ! -f "${INPUT_FILE}" ]]; then
    echo "Input file not found: ${INPUT_FILE}" >&2
    exit 1
fi

if [[ -f ".env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

if [[ -z "${TROCADOR_API_KEY:-}" ]]; then
    echo "TROCADOR_API_KEY is required in environment or .env" >&2
    exit 1
fi

sleep_for_delay() {
    awk -v ms="$DELAY_MS" 'BEGIN { printf "%.3f\n", ms / 1000 }'
}

extract_unsupported_pairs() {
    awk '
        /API returned error: \{"error": "coin not found"\}/ && / -> / {
            split($0, left_right, " -> ");
            left = left_right[1];
            right = left_right[2];

            split(left, pair_status, " \\[");
            split(pair_status[1], pair, "/");
            ticker = pair[1];
            network = pair[2];

            split(right, address_and_rest, ": API error:");
            address = address_and_rest[1];

            key = tolower(ticker) "\t" tolower(network);
            if (!seen[key]++) {
                print ticker "\t" network "\t" address;
            }
        }
    ' "$INPUT_FILE"
}

coins_file="$(mktemp)"
unsupported_file="$(mktemp)"
cleanup() {
    rm -f "$coins_file" "$unsupported_file"
}
trap cleanup EXIT

curl -fsS \
    -H "API-Key: ${TROCADOR_API_KEY}" \
    "${API_BASE_URL}/coins" > "$coins_file"

extract_unsupported_pairs > "$unsupported_file"

unsupported_count="$(wc -l < "$unsupported_file" | tr -d ' ')"
if [[ "$unsupported_count" -eq 0 ]]; then
    echo "No unsupported coin-not-found pairs were found in ${INPUT_FILE}" >&2
    exit 0
fi

list_source_pairs() {
    local spec
    for spec in \
        "btc|Mainnet" \
        "xmr|Mainnet" \
        "ltc|Mainnet" \
        "eth|Mainnet" \
        "trx|Mainnet" \
        "usdt|TRC20" \
        "sol|SOL" \
        "bnb|BEP20"
    do
        local ticker="${spec%%|*}"
        local network="${spec##*|}"
        local result
        result="$(
            jq -r \
                --arg t "$ticker" \
                --arg n "$network" \
                '
                first(
                  .[]
                  | select((.ticker | ascii_downcase) == ($t | ascii_downcase))
                  | select((.network | ascii_downcase) == ($n | ascii_downcase))
                  | [
                      .ticker,
                      .network,
                      ((.minimum // 0) | tostring),
                      ((.maximum // 0) | tostring)
                    ]
                  | @tsv
                ) // empty
                ' \
                "$coins_file"
        )"
        if [[ -n "$result" ]]; then
            printf '%s\n' "$result"
        fi
    done
}

calc_probe_amount() {
    local minimum="$1"
    awk -v min="$minimum" '
        BEGIN {
            m = min + 0.0;
            if (m <= 0) {
                printf "%.12f\n", 0.001;
            } else {
                probe = m * 1.25;
                if (probe == m) {
                    probe = m + 0.00000001;
                }
                printf "%.12f\n", probe;
            }
        }
    '
}

find_live_target_meta() {
    local ticker="$1"
    local network="$2"
    jq -c \
        --arg t "$ticker" \
        --arg n "$network" \
        '
        first(
          .[]
          | select((.ticker | ascii_downcase) == ($t | ascii_downcase))
          | select((.network | ascii_downcase) == ($n | ascii_downcase))
          | {
              ticker,
              network,
              memo,
              minimum,
              maximum
            }
        ) // empty
        ' \
        "$coins_file"
}

call_new_rate() {
    local src_ticker="$1"
    local src_network="$2"
    local dst_ticker="$3"
    local dst_network="$4"
    local amount="$5"

    curl -fsS -G \
        -H "API-Key: ${TROCADOR_API_KEY}" \
        --data-urlencode "ticker_from=${src_ticker}" \
        --data-urlencode "network_from=${src_network}" \
        --data-urlencode "ticker_to=${dst_ticker}" \
        --data-urlencode "network_to=${dst_network}" \
        --data-urlencode "amount_from=${amount}" \
        --data-urlencode "best_only=false" \
        "${API_BASE_URL}/new_rate"
}

call_new_trade() {
    local trade_id="$1"
    local src_ticker="$2"
    local src_network="$3"
    local dst_ticker="$4"
    local dst_network="$5"
    local amount="$6"
    local address="$7"
    local provider="$8"
    local fixed="$9"
    local memo_required="${10}"

    local args=(
        -fsS -G
        -H "API-Key: ${TROCADOR_API_KEY}"
        --data-urlencode "id=${trade_id}"
        --data-urlencode "ticker_from=${src_ticker}"
        --data-urlencode "network_from=${src_network}"
        --data-urlencode "ticker_to=${dst_ticker}"
        --data-urlencode "network_to=${dst_network}"
        --data-urlencode "amount_from=${amount}"
        --data-urlencode "address=${address}"
        --data-urlencode "provider=${provider}"
        --data-urlencode "fixed=${fixed}"
        --data-urlencode "payment=false"
    )

    if [[ "${memo_required}" == "true" ]]; then
        args+=(--data-urlencode "address_memo=0")
    fi

    curl "${args[@]}" "${API_BASE_URL}/new_trade"
}

printf '%s\n' \
    "target_ticker	target_network	target_address	live_coin	memo_required	rate_status	rate_source_ticker	rate_source_network	rate_amount	provider	fixed	rate_trade_id	trade_status	trade_id	error" \
    > "$OUTPUT_FILE"

live_coin_missing=0
rate_success=0
rate_failed=0
trade_success=0
trade_failed=0

while IFS=$'\t' read -r ticker network address; do
    target_meta="$(find_live_target_meta "$ticker" "$network")"
    if [[ -z "$target_meta" ]]; then
        live_coin_missing=$((live_coin_missing + 1))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$ticker" "$network" "$address" "no" "" "missing_from_live_coins" "" "" "" "" "" "" "" "" "" \
            >> "$OUTPUT_FILE"
        continue
    fi

    memo_required="$(printf '%s' "$target_meta" | jq -r '.memo')"

    source_meta="$(list_source_pairs || true)"
    if [[ -z "$source_meta" ]]; then
        rate_failed=$((rate_failed + 1))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$ticker" "$network" "$address" "yes" "$memo_required" "no_probe_source_found" "" "" "" "" "" "" "" "" "" \
            >> "$OUTPUT_FILE"
        continue
    fi

    quote_found=0
    probe_error=""
    selected_src_ticker=""
    selected_src_network=""
    selected_amount=""
    selected_provider=""
    selected_fixed=""
    selected_trade_id=""

    while IFS=$'\t' read -r src_ticker src_network src_minimum _src_maximum; do
        selected_src_ticker="$src_ticker"
        selected_src_network="$src_network"
        selected_amount="$(calc_probe_amount "$src_minimum")"

        rate_response="$(call_new_rate "$src_ticker" "$src_network" "$ticker" "$network" "$selected_amount" 2>&1 || true)"
        if jq -e '.trade_id' >/dev/null 2>&1 <<<"$rate_response"; then
            quote_found=1
            selected_provider="$(jq -r '.provider // .quotes.quotes[0].provider // ""' <<<"$rate_response")"
            selected_fixed="$(jq -r 'if .fixed == true then "true" elif .fixed == false then "false" elif (.quotes.quotes[0].fixed // "") | ascii_downcase == "true" then "true" else "false" end' <<<"$rate_response")"
            selected_trade_id="$(jq -r '.trade_id // ""' <<<"$rate_response")"
            break
        fi

        if jq -e '.error' >/dev/null 2>&1 <<<"$rate_response"; then
            probe_error="$(jq -r '.error' <<<"$rate_response")"
        else
            probe_error="$rate_response"
        fi
        sleep "$(sleep_for_delay)"
    done < <(printf '%s\n' "$source_meta")

    if [[ "$quote_found" -eq 0 ]]; then
        rate_failed=$((rate_failed + 1))
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$ticker" "$network" "$address" "yes" "$memo_required" "rate_failed" \
            "$selected_src_ticker" "$selected_src_network" "$selected_amount" "" "" "" "" "" "$probe_error" \
            >> "$OUTPUT_FILE"
        continue
    fi

    rate_success=$((rate_success + 1))
    trade_status="not_probed"
    trade_id=""
    trade_error=""

    if [[ "$PROBE_TRADE" -eq 1 ]]; then
        trade_response="$(
            call_new_trade \
                "$selected_trade_id" \
                "$selected_src_ticker" \
                "$selected_src_network" \
                "$ticker" \
                "$network" \
                "$selected_amount" \
                "$address" \
                "$selected_provider" \
                "$selected_fixed" \
                "$memo_required" 2>&1 || true
        )"

        if jq -e '.trade_id' >/dev/null 2>&1 <<<"$trade_response"; then
            trade_status="trade_created"
            trade_id="$(jq -r '.trade_id // ""' <<<"$trade_response")"
            trade_success=$((trade_success + 1))
        else
            trade_status="trade_failed"
            if jq -e '.error' >/dev/null 2>&1 <<<"$trade_response"; then
                trade_error="$(jq -r '.error' <<<"$trade_response")"
            else
                trade_error="$trade_response"
            fi
            trade_failed=$((trade_failed + 1))
        fi
        sleep "$(sleep_for_delay)"
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$ticker" "$network" "$address" "yes" "$memo_required" "rate_ok" \
        "$selected_src_ticker" "$selected_src_network" "$selected_amount" \
        "$selected_provider" "$selected_fixed" "$selected_trade_id" \
        "$trade_status" "$trade_id" "$trade_error" \
        >> "$OUTPUT_FILE"
done < "$unsupported_file"

echo "Input log: ${INPUT_FILE}"
echo "Output: ${OUTPUT_FILE}"
echo "Unsupported pairs parsed: ${unsupported_count}"
echo "Missing from live /coins: ${live_coin_missing}"
echo "Pairs with successful /new_rate: ${rate_success}"
echo "Pairs without successful /new_rate: ${rate_failed}"
if [[ "$PROBE_TRADE" -eq 1 ]]; then
    echo "Pairs with successful /new_trade: ${trade_success}"
    echo "Pairs with failed /new_trade: ${trade_failed}"
fi
