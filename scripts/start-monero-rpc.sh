#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v monero-wallet-rpc >/dev/null 2>&1; then
  echo "[start-monero-rpc] ERROR: monero-wallet-rpc binary not found on PATH"
  echo "[start-monero-rpc] Install Monero CLI (monero-wallet-rpc) and re-run ./scripts/setup.sh"
  exit 1
fi

if [ -f "$ROOT_DIR/.env" ]; then
  set -a
  . "$ROOT_DIR/.env"
  set +a
fi

PORT="${MONERO_RPC_PORT:-18083}"
BIND="${MONERO_RPC_BIND_IP:-127.0.0.1}"
HOME="${HOME:-/root}"
WALLET_DIR="${MONERO_WALLET_DIR:-$HOME/.monero-wallets}"
DAEMON="${MONERO_DAEMON_ADDRESS:-xmr-node.cakewallet.com:18081}"
LOG_FILE="${MONERO_RPC_LOG:-$PWD/monero-rpc.log}"
PID_FILE="${MONERO_RPC_PID_FILE:-$PWD/monero-rpc.pid}"
RPC_USER="${MONERO_WALLET_RPC_USER:-}"
RPC_PASS="${MONERO_WALLET_RPC_PASSWORD:-}"

mkdir -p "$WALLET_DIR"

# Clean up stale lock files
find "$WALLET_DIR" -name "*.lock" -type f -delete 2>/dev/null || true

# Kill existing RPC if running (using PID file)
if [ -f "$PID_FILE" ]; then
  PID=$(cat "$PID_FILE" 2>/dev/null || true)
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    echo "[start-monero-rpc] Stopping existing RPC (PID: $PID)..."
    kill "$PID" 2>/dev/null || true
    sleep 1
  fi
  rm -f "$PID_FILE"
fi

echo "[start-monero-rpc] Starting wallet RPC (daemon: $DAEMON, wallet dir: $WALLET_DIR, port: $PORT)"

RPC_CMD=(
  monero-wallet-rpc
  --daemon-address "$DAEMON"
  --rpc-bind-port "$PORT"
  --rpc-bind-ip "$BIND"
  --wallet-dir "$WALLET_DIR"
  --rpc-ssl disabled
  --log-level 1
)

# NOTE: This Monero version does not support --enable-multisig-experimental.
# To enable multisig experimental mode, run once per wallet in monero-wallet-cli:
#   set enable-multisig-experimental 1

if [ -z "$RPC_USER" ] || [ -z "$RPC_PASS" ]; then
  RPC_USER="${RPC_USER:-znode_rpc}"
  RPC_PASS="${RPC_PASS:-znode_rpc_pw}"
  echo "[start-monero-rpc] RPC credentials not set; using default testing credentials"
fi

RPC_CMD+=(--rpc-login "$RPC_USER:$RPC_PASS")
echo "[start-monero-rpc] RPC authentication enabled"

if [ "$BIND" != "127.0.0.1" ] && [ "${TEST_MODE:-0}" != "1" ]; then
  echo "[start-monero-rpc] WARNING: Binding to $BIND in production mode"
  echo "[start-monero-rpc] Ensure firewall is configured and consider using TLS/mTLS"
fi

if [ "${MONERO_TRUST_DAEMON:-0}" = "1" ]; then
  RPC_CMD+=(--trusted-daemon)
  echo "[start-monero-rpc] WARNING: Daemon marked as trusted"
fi

# Start wallet RPC using array expansion (safe from injection)
nohup "${RPC_CMD[@]}" >"$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

# Wait for readiness
for i in $(seq 1 15); do
  if command -v ss >/dev/null 2>&1 && ss -ltnp 2>/dev/null | grep -q ":$PORT"; then
    echo "[start-monero-rpc] READY on $BIND:$PORT"
    exit 0
  elif command -v netstat >/dev/null 2>&1 && netstat -ltn 2>/dev/null | grep -q ":$PORT"; then
    echo "[start-monero-rpc] READY on $BIND:$PORT"
    exit 0
  elif command -v curl >/dev/null 2>&1; then
    AUTH_ARGS=""
    if [ -n "$MONERO_WALLET_RPC_USER" ] && [ -n "$MONERO_WALLET_RPC_PASSWORD" ]; then
      AUTH_ARGS="-u $MONERO_WALLET_RPC_USER:$MONERO_WALLET_RPC_PASSWORD"
    fi
    if curl -s --max-time 2 $AUTH_ARGS -X POST "http://${BIND}:${PORT}/json_rpc" \
         -d '{"jsonrpc":"2.0","id":"0","method":"get_version"}' \
         -H "Content-Type: application/json" >/dev/null 2>&1; then
      echo "[start-monero-rpc] READY on $BIND:$PORT"
      exit 0
    fi
  fi
  sleep 1
done

echo "[start-monero-rpc] WARNING: RPC not ready on $BIND:$PORT"
exit 1
