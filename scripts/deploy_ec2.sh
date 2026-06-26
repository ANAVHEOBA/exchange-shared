#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KEY_PATH="${KEY_PATH:-$ROOT_DIR/exchange-backend-key.pem}"
REMOTE_USER="${REMOTE_USER:-ubuntu}"
REMOTE_HOST="${REMOTE_HOST:-16.171.154.42}"
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/exchange-shared}"
PUBLIC_BACKEND_URL="${PUBLIC_BACKEND_URL:-https://16-171-154-42.sslip.io}"

MODE="${1:-full}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/deploy_ec2.sh full
  bash scripts/deploy_ec2.sh env

Modes:
  full  rsync the repo to EC2, then run deploy-to-ec2.sh remotely
  env   sync only .env, then restart exchange-backend remotely

Optional environment overrides:
  KEY_PATH=/path/to/key.pem
  REMOTE_USER=ubuntu
  REMOTE_HOST=16.171.154.42
  REMOTE_DIR=/home/ubuntu/exchange-shared
  PUBLIC_BACKEND_URL=https://16-171-154-42.sslip.io
EOF
}

if [[ "$MODE" != "full" && "$MODE" != "env" ]]; then
  usage
  exit 1
fi

if [[ ! -f "$KEY_PATH" ]]; then
  echo "Key not found: $KEY_PATH" >&2
  exit 1
fi

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  echo ".env not found in $ROOT_DIR" >&2
  exit 1
fi

SSH_CMD=(ssh -i "$KEY_PATH")
RSYNC_SSH="ssh -i $KEY_PATH"
REMOTE_TARGET="${REMOTE_USER}@${REMOTE_HOST}"

echo "Using remote host: $REMOTE_TARGET"
echo "Using remote dir:  $REMOTE_DIR"
echo "Mode:              $MODE"

if [[ "$MODE" == "env" ]]; then
  echo
  echo "Syncing .env to EC2..."
  rsync -avz -e "$RSYNC_SSH" \
    "$ROOT_DIR/.env" \
    "${REMOTE_TARGET}:${REMOTE_DIR}/.env"

  echo
  echo "Restarting exchange-backend..."
  "${SSH_CMD[@]}" "$REMOTE_TARGET" \
    "sudo systemctl restart exchange-backend && sudo systemctl --no-pager --full status exchange-backend"
else
  echo
  echo "Rsyncing repo to EC2..."
  rsync -avz --delete -e "$RSYNC_SSH" \
    --exclude .git \
    --exclude .agents \
    --exclude .codex \
    --exclude target \
    --exclude .DS_Store \
    --exclude '*.log' \
    "$ROOT_DIR/" \
    "${REMOTE_TARGET}:${REMOTE_DIR}/"

  echo
  echo "Running remote deploy script..."
  "${SSH_CMD[@]}" "$REMOTE_TARGET" \
    "cd ${REMOTE_DIR} && bash deploy-to-ec2.sh"
fi

echo
echo "Checking public health endpoint..."
HEALTH_URL="${PUBLIC_BACKEND_URL}/health"
HEALTH_OK=0

for attempt in {1..12}; do
  if curl --max-time 20 -fsSL "$HEALTH_URL" >/dev/null; then
    HEALTH_OK=1
    break
  fi

  echo "Health check not ready yet (${attempt}/12). Waiting 5s..."
  sleep 5
done

if [[ "$HEALTH_OK" -ne 1 ]]; then
  echo "Health check failed at ${HEALTH_URL}" >&2
  exit 1
fi

echo
echo "Done."
