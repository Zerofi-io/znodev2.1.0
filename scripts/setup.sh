#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    ZNode Setup Script                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# --- Check Node.js version ---
echo "[1/5] Checking Node.js version..."
if ! command -v node >/dev/null 2>&1; then
  echo "❌ ERROR: Node.js is not installed."
  echo "   Please install Node.js >= 18 from https://nodejs.org/"
  exit 1
fi

NODE_VER_RAW="$(node -v | sed 's/^v//')"
NODE_MAJOR="${NODE_VER_RAW%%.*}"
if [ "${NODE_MAJOR:-0}" -lt 18 ]; then
  echo "❌ ERROR: Node.js >= 18 required, found v$NODE_VER_RAW"
  echo "   Please upgrade Node.js from https://nodejs.org/"
  exit 1
fi

echo "✓ Node.js v$NODE_VER_RAW detected"
echo ""

# --- Check/Install Go ---
echo "[2/5] Checking Go installation..."
GO_VERSION="1.24.0"
GO_MIN_MAJOR=1
GO_MIN_MINOR=24

install_go() {
  echo "  Installing Go $GO_VERSION..."
  cd /tmp
  wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O go.tar.gz
  sudo rm -rf /usr/local/go
  sudo tar -C /usr/local -xzf go.tar.gz
  rm go.tar.gz
  export PATH="/usr/local/go/bin:$PATH"
  cd "$SCRIPT_DIR/.."
}

# Check if Go is installed and version is sufficient
if command -v go &> /dev/null; then
  GO_VER_RAW="$(go version | grep -oP 'go\K[0-9]+\.[0-9]+' | head -1)"
  GO_MAJOR="${GO_VER_RAW%%.*}"
  GO_MINOR="${GO_VER_RAW##*.}"
  if [ "${GO_MAJOR:-0}" -lt "$GO_MIN_MAJOR" ] || { [ "${GO_MAJOR:-0}" -eq "$GO_MIN_MAJOR" ] && [ "${GO_MINOR:-0}" -lt "$GO_MIN_MINOR" ]; }; then
    echo "  Go $GO_VER_RAW found but >= $GO_MIN_MAJOR.$GO_MIN_MINOR required"
    install_go
  else
    echo "✓ Go $GO_VER_RAW detected"
  fi
elif [ -x "/usr/local/go/bin/go" ]; then
  export PATH="/usr/local/go/bin:$PATH"
  echo "✓ Go found at /usr/local/go/bin/go"
else
  install_go
fi

# Verify Go is working
if ! go version &> /dev/null; then
  echo "❌ ERROR: Go installation failed"
  exit 1
fi
echo ""


# --- Install dependencies ---
echo "[3/5] Installing npm dependencies..."
if [ -f package-lock.json ]; then
  npm ci --omit=dev --quiet
else
  npm install --omit=dev --quiet
fi
echo "✓ Dependencies installed"
echo ""

# --- Collect configuration ---
echo "[5/6] Configuring environment..."
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  echo "⚠️  Existing .env file detected"
  read -r -p "Overwrite existing .env? [y/N]: " ans
  case "${ans,,}" in
    y|yes) echo "Overwriting existing configuration..." ;;
    *) echo "✓ Keeping existing .env"; echo ""; echo "Setup complete! Use ./start to launch the node."; exit 0;;
  esac
fi

# Defaults
DEFAULT_RPC_URL="https://ethereum-sepolia-rpc.publicnode.com"
DEFAULT_MONERO_WALLET_PASSWORD="znode_monero_pw"
DEFAULT_MONERO_WALLET_RPC_USER="znode_rpc"
DEFAULT_MONERO_WALLET_RPC_PASSWORD="znode_rpc_pw"
DEFAULT_P2P_BOOTSTRAP_PEERS=/ip4/185.191.116.142/tcp/9000/p2p/16Uiu2HAmDPgaLxg1KfAfPt3uoLKPz4g4NSwXjhb7U2jaExuSyjiF

echo ""
echo "Required Configuration:"
echo "----------------------"

# Ethereum RPC URL
read -r -p "Ethereum Sepolia RPC URL [press Enter for default]: " RPC_URL
RPC_URL="${RPC_URL:-$DEFAULT_RPC_URL}"

# Ethereum Private Key
read -r -s -p "Ethereum private key (0x...): " PRIVATE_KEY
echo ""
while [ -z "${PRIVATE_KEY}" ]; do
  echo "⚠️  Private key is required."
  read -r -s -p "Ethereum private key (0x...): " PRIVATE_KEY
  echo ""
done

if [[ ! "$PRIVATE_KEY" =~ ^0x ]]; then
  PRIVATE_KEY="0x${PRIVATE_KEY}"
fi

MONERO_WALLET_PASSWORD="${DEFAULT_MONERO_WALLET_PASSWORD}"
echo "ℹ️  Generated Monero wallet password (stored in .env only)"

# All other settings use defaults
MONERO_RPC_URL="http://127.0.0.1:18083"
MONERO_WALLET_RPC_USER="${DEFAULT_MONERO_WALLET_RPC_USER}"
MONERO_WALLET_RPC_PASSWORD="${DEFAULT_MONERO_WALLET_RPC_PASSWORD}"

# Public IP used for P2P announcements (optional but recommended)
echo ""
read -r -p "Public IP address for P2P (optional, press Enter to skip): " PUBLIC_IP

# Normalize PUBLIC_IP input: allow user@host and strip username if present
if [ -n "$PUBLIC_IP" ]; then
  # If in the form user@host, keep only host
  case "$PUBLIC_IP" in
    *@*) PUBLIC_IP="${PUBLIC_IP##*@}" ;;
  esac
fi

P2P_BOOTSTRAP_PEERS=/ip4/185.191.116.142/tcp/9000/p2p/16Uiu2HAmDPgaLxg1KfAfPt3uoLKPz4g4NSwXjhb7U2jaExuSyjiF

# Detect monero-wallet-cli for automatic multisig enabling
MONERO_WALLET_CLI=""
if command -v monero-wallet-cli >/dev/null 2>&1; then
  MONERO_WALLET_CLI="$(command -v monero-wallet-cli)"
  echo "✓ Found monero-wallet-cli at $MONERO_WALLET_CLI"
elif command -v monero-wallet-rpc >/dev/null 2>&1; then
  RPC_BIN="$(command -v monero-wallet-rpc)"
  RPC_DIR="$(dirname "$RPC_BIN")"
  if [ -x "$RPC_DIR/monero-wallet-cli" ]; then
    MONERO_WALLET_CLI="$RPC_DIR/monero-wallet-cli"
    echo "✓ Found monero-wallet-cli next to monero-wallet-rpc at $MONERO_WALLET_CLI"
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "⚠️  monero-wallet-cli not found on PATH. Attempting automatic install..."
  if command -v curl >/dev/null 2>&1 && command -v tar >/dev/null 2>&1; then
    TMP_DIR="$(mktemp -d)"
    CLI_URL="https://downloads.getmonero.org/cli/linux64"
    ARCHIVE="$TMP_DIR/monero-cli.tar.bz2"
    if curl -L -o "$ARCHIVE" "$CLI_URL" >/dev/null 2>&1; then
      if tar -xjf "$ARCHIVE" -C "$TMP_DIR" >/dev/null 2>&1; then
        CLI_BIN="$(find "$TMP_DIR" -maxdepth 3 -type f -name 'monero-wallet-cli' | head -n 1 || true)"
        if [ -n "$CLI_BIN" ] && [ -f "$CLI_BIN" ]; then
          if install -m 0755 "$CLI_BIN" /usr/local/bin/monero-wallet-cli 2>/dev/null; then
            MONERO_WALLET_CLI="/usr/local/bin/monero-wallet-cli"
            echo "✓ Installed monero-wallet-cli to $MONERO_WALLET_CLI"
          else
            echo "⚠️  Failed to install monero-wallet-cli to /usr/local/bin (insufficient permissions?)."
          fi
        else
          echo "⚠️  Could not locate monero-wallet-cli in downloaded archive."
        fi
      else
        echo "⚠️  Failed to extract downloaded Monero CLI archive."
      fi
    else
      echo "⚠️  Failed to download Monero CLI from $CLI_URL"
    fi
    rm -rf "$TMP_DIR"
  else
    echo "⚠️  curl or tar not available; cannot auto-download Monero CLI."
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "⚠️  monero-wallet-cli is still missing. Multisig auto-enable will log a warning until you install it."
fi

echo ""
echo "[5/6] Writing configuration..."

cat > "$ENV_FILE" << EOF_ENV
# Auto-generated by setup.sh on $(date)

PRIVATE_KEY=${PRIVATE_KEY}
RPC_URL=${RPC_URL}
MONERO_WALLET_PASSWORD=${MONERO_WALLET_PASSWORD}
MONERO_WALLET_CLI="$MONERO_WALLET_CLI"

TEST_MODE=0
DRY_RUN=0

P2P_IMPL=libp2p
P2P_REQUIRE_E2E=1
ENABLE_HEARTBEAT_ORACLE=1
PUBLIC_IP=${PUBLIC_IP}
P2P_BOOTSTRAP_PEERS=/ip4/185.191.116.142/tcp/9000/p2p/16Uiu2HAmDPgaLxg1KfAfPt3uoLKPz4g4NSwXjhb7U2jaExuSyjiF

MONERO_RPC_URL=${MONERO_RPC_URL}
MONERO_WALLET_RPC_USER=${MONERO_WALLET_RPC_USER}
MONERO_WALLET_RPC_PASSWORD=${MONERO_WALLET_RPC_PASSWORD}

REGISTRY_ADDR=0xDD245151284B90D067398ffDf32dE08CD36B41A6
STAKING_ADDR=0xC7ECD047a64AcBaD9809dfd26b20E9437514cc3D
ZFI_ADDR=0x2B8428ab14004483f946eBD6b012b8983C2638B1
COORDINATOR_ADDR=0xBEf90Fb37CfaA3ee2B062d7Dd01489e90eb5Fc99

HEARTBEAT_INTERVAL=30
STALE_ROUND_MIN_AGE_MS=600000
STICKY_QUEUE=0
FORCE_SELECT=0
EOF_ENV

chmod 600 "$ENV_FILE" || true
echo "✓ Configuration written to $ENV_FILE"
echo ""

echo "[6/6] Validating configuration..."
if node --check node.js 2>/dev/null; then
  echo "✓ Syntax validation passed"
else
  echo "⚠️  Warning: Could not validate syntax"
fi
echo ""

# Ensure ufw is installed so we can manage firewall rules automatically
if ! command -v ufw >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "[Firewall] ufw not found; installing via apt-get..."
    apt-get update -y >/dev/null 2>&1 && apt-get install -y ufw >/dev/null 2>&1       || echo "[Firewall] Warning: failed to install ufw. Please install it manually and open port 9000."
  else
    echo "[Firewall] ufw not found and apt-get not available. Please install ufw manually and open port 9000."
  fi
fi

# Optional firewall configuration for P2P ports
# Attempts to open inbound TCP ports used by the node and heartbeat oracle.
if command -v ufw >/dev/null 2>&1; then
  echo "[Firewall] Detected ufw; ensuring P2P port 9000 is open..."
  ufw allow 9000/tcp >/dev/null 2>&1 || echo "[Firewall] Warning: failed to run 'ufw allow 9000/tcp'"
else
  echo "[Firewall] ufw not found. Please ensure TCP port 9000 is open inbound for P2P."
fi

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    Setup Complete!                             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "  1. Start the node: ./start"
echo "  2. View logs: tail -f znode.log"
echo ""
echo "⚠️  IMPORTANT:"
echo "  - Node configured for production (Sepolia testnet)"
echo "  - See README.md for full documentation"
echo ""
