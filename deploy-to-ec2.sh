#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"
SERVICE_NAME="exchange-backend"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
CADDYFILE="/etc/caddy/Caddyfile"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info() {
    echo -e "${YELLOW}$1${NC}"
}

success() {
    echo -e "${GREEN}$1${NC}"
}

fail() {
    echo -e "${RED}$1${NC}"
    exit 1
}

read_env_var() {
    local key="$1"
    if [ ! -f "$ENV_FILE" ]; then
        return 1
    fi

    awk -F= -v env_key="$key" '
        $1 ~ "^[[:space:]]*" env_key "[[:space:]]*$" {
            value = substr($0, index($0, "=") + 1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
            gsub(/^"/, "", value)
            gsub(/"$/, "", value)
            print value
            exit
        }
    ' "$ENV_FILE"
}

write_env_var() {
    local key="$1"
    local value="$2"

    if grep -q "^${key}=" "$ENV_FILE"; then
        sed -i.bak "s#^${key}=.*#${key}=${value}#" "$ENV_FILE"
    else
        printf '\n%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}

extract_host() {
    local raw="$1"
    raw="${raw#http://}"
    raw="${raw#https://}"
    raw="${raw%%/*}"
    printf '%s' "$raw"
}

info "🚀 Starting EC2 HTTPS deployment..."

[ -f "$ENV_FILE" ] || fail "⚠️  .env file not found at $ENV_FILE"

RAW_PUBLIC_BASE_URL="$(read_env_var PUBLIC_BACKEND_URL || true)"
if [ -z "$RAW_PUBLIC_BASE_URL" ]; then
    RAW_PUBLIC_BASE_URL="$(read_env_var RENDER_EXTERNAL_URL || true)"
fi
if [ -z "$RAW_PUBLIC_BASE_URL" ]; then
    RAW_PUBLIC_BASE_URL="$(read_env_var API_BASE_URL || true)"
fi

[ -n "$RAW_PUBLIC_BASE_URL" ] || fail "Set PUBLIC_BACKEND_URL, RENDER_EXTERNAL_URL, or API_BASE_URL in .env before deploying"

PUBLIC_HOST="$(extract_host "$RAW_PUBLIC_BASE_URL")"
[ -n "$PUBLIC_HOST" ] || fail "Could not extract a public host from $RAW_PUBLIC_BASE_URL"

PUBLIC_BASE_URL="https://${PUBLIC_HOST}"

info "🌐 Using public host: $PUBLIC_HOST"
info "🔒 Normalizing public base URL to: $PUBLIC_BASE_URL"
write_env_var PUBLIC_BACKEND_URL "$PUBLIC_BASE_URL"

if grep -q "^RENDER_EXTERNAL_URL=" "$ENV_FILE"; then
    write_env_var RENDER_EXTERNAL_URL "$PUBLIC_BASE_URL"
fi

info "📦 Updating system packages..."
sudo apt update
sudo apt install -y \
    apt-transport-https \
    build-essential \
    ca-certificates \
    curl \
    debian-archive-keyring \
    debian-keyring \
    git \
    gnupg \
    libssl-dev \
    mysql-client \
    pkg-config

if ! command -v cargo >/dev/null 2>&1; then
    info "🦀 Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"

if ! command -v caddy >/dev/null 2>&1; then
    info "🔐 Installing Caddy..."
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
        | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
        | sudo tee /etc/apt/sources.list.d/caddy-stable.list >/dev/null
    sudo apt update
    sudo apt install -y caddy
else
    success "✅ Caddy already installed"
fi

info "🔨 Building Rust application..."
cd "$ROOT_DIR"
cargo build --release

info "⚙️  Writing systemd service..."
sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Exchange Backend Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$ROOT_DIR
EnvironmentFile=$ENV_FILE
Environment=PATH=$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=$ROOT_DIR/target/release/exchange-shared
Restart=always
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

info "🔁 Writing Caddy reverse proxy config..."
sudo tee "$CADDYFILE" >/dev/null <<EOF
$PUBLIC_HOST {
    encode zstd gzip

    header {
        Strict-Transport-Security "max-age=31536000; includeSubDomains"
    }

    reverse_proxy 127.0.0.1:3000 {
        header_up X-Forwarded-Proto {scheme}
        header_up X-Forwarded-Host {host}
        header_up X-Real-IP {remote_host}
    }
}
EOF

info "🧪 Validating Caddy config..."
sudo caddy validate --config "$CADDYFILE"

info "🚦 Restarting services..."
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl enable caddy
sudo systemctl restart caddy

success "✅ EC2 deployment complete"
echo
echo "Public API: $PUBLIC_BASE_URL"
echo "WhatsApp webhook: ${PUBLIC_BASE_URL}/whatsapp/webhook"
echo "Local health check: http://127.0.0.1:3000/health"
echo
echo "Service status:"
sudo systemctl --no-pager --full status "$SERVICE_NAME"
echo
echo "Caddy status:"
sudo systemctl --no-pager --full status caddy
echo
echo "Next checks:"
echo "  1. Open inbound TCP 80 and 443 on the EC2 security group."
echo "  2. Confirm https://${PUBLIC_HOST}/health loads."
echo "  3. Set the Meta callback URL to ${PUBLIC_BASE_URL}/whatsapp/webhook"
