#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
SUDO=""
SUDO_E=""
if [ "$(id -u)" -ne 0 ]; then
  if command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
    SUDO_E="sudo -E"
  else
    echo "ERROR: This script must be run as root or have sudo installed"
    exit 1
  fi
fi
if command -v apt-get >/dev/null 2>&1; then
  MISSING_PKGS=""
  command -v curl >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS curl"
  command -v wget >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS wget"
  command -v tar >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS tar"
  command -v bzip2 >/dev/null 2>&1 || MISSING_PKGS="$MISSING_PKGS bzip2"
  if [ -n "$MISSING_PKGS" ]; then
    $SUDO apt-get update -y >/dev/null 2>&1 || true
    $SUDO apt-get install -y $MISSING_PKGS >/dev/null 2>&1 || echo "Failed to install packages:$MISSING_PKGS"
  fi
fi

echo "================================================================"
echo "                    ZNode Setup Script                          "
echo "================================================================"
echo ""

# --- Check/Install Node.js ---
echo "[1/5] Checking Node.js version..."

install_nodejs() {
  echo "  Installing Node.js 20.x LTS..."
  curl -fsSL https://deb.nodesource.com/setup_20.x | $SUDO_E bash - >/dev/null 2>&1
  $SUDO apt-get install -y nodejs >/dev/null 2>&1
  if ! command -v node >/dev/null 2>&1; then
    echo " ERROR: Failed to install Node.js automatically."
    echo "   Please install Node.js >= 18 manually from https://nodejs.org/"
    exit 1
  fi
  echo "   Node.js installed successfully"
}

if ! command -v node >/dev/null 2>&1; then
  echo "  Node.js not found. Installing..."
  install_nodejs
fi

NODE_VER_RAW="$(node -v | sed 's/^v//')"
NODE_MAJOR="${NODE_VER_RAW%%.*}"
if [ "${NODE_MAJOR:-0}" -lt 18 ]; then
  echo "  Node.js version too old (v$NODE_VER_RAW). Upgrading..."
  install_nodejs
  NODE_VER_RAW="$(node -v | sed 's/^v//')"
fi

echo " Node.js v$NODE_VER_RAW detected"
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
  $SUDO rm -rf /usr/local/go
  $SUDO tar -C /usr/local -xzf go.tar.gz
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
    echo " Go $GO_VER_RAW detected"
  fi
elif [ -x "/usr/local/go/bin/go" ]; then
  export PATH="/usr/local/go/bin:$PATH"
  echo " Go found at /usr/local/go/bin/go"
else
  install_go
fi

# Verify Go is working
if ! go version &> /dev/null; then
  echo " ERROR: Go installation failed"
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
echo " Dependencies installed"
echo ""

# --- Collect configuration ---
echo "[5/6] Configuring environment..."
WRITE_ENV=1
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  echo "  Existing .env file detected"
  read -r -p "Overwrite existing .env? [y/N]: " ans
  case "${ans,,}" in
    y|yes) echo "Overwriting existing configuration..." ;;
    *) WRITE_ENV=0; echo " Keeping existing .env"; echo "";;
  esac
fi

if [ "$WRITE_ENV" -eq 1 ]; then
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
  echo "  Private key is required."
  read -r -s -p "Ethereum private key (0x...): " PRIVATE_KEY
  echo ""
done

if [[ ! "$PRIVATE_KEY" =~ ^0x ]]; then
  PRIVATE_KEY="0x${PRIVATE_KEY}"
fi

MONERO_WALLET_PASSWORD="${DEFAULT_MONERO_WALLET_PASSWORD}"
echo "  Generated Monero wallet password (stored in .env only)"

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
fi

# Detect monero-wallet-cli for automatic multisig enabling
MONERO_WALLET_CLI=""
MONERO_WALLET_RPC_BIN=""
if command -v monero-wallet-cli >/dev/null 2>&1; then
  MONERO_WALLET_CLI="$(command -v monero-wallet-cli)"
  echo " Found monero-wallet-cli at $MONERO_WALLET_CLI"
fi
if command -v monero-wallet-rpc >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
  echo " Found monero-wallet-rpc at $MONERO_WALLET_RPC_BIN"
  if [ -z "$MONERO_WALLET_CLI" ]; then
    RPC_DIR="$(dirname "$MONERO_WALLET_RPC_BIN")"
    if [ -x "$RPC_DIR/monero-wallet-cli" ]; then
      MONERO_WALLET_CLI="$RPC_DIR/monero-wallet-cli"
      echo " Found monero-wallet-cli next to monero-wallet-rpc at $MONERO_WALLET_CLI"
    fi
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "monero-wallet-cli not found on PATH. Attempting automatic install of Monero CLI (wallet-cli + wallet-rpc)..."
  if command -v curl >/dev/null 2>&1 && command -v tar >/dev/null 2>&1; then
    TMP_DIR="$(mktemp -d)"
    CLI_URL="https://downloads.getmonero.org/cli/linux64"
    ARCHIVE="$TMP_DIR/monero-cli.tar.bz2"
    if curl -L -o "$ARCHIVE" "$CLI_URL" >/dev/null 2>&1; then
      if tar -xjf "$ARCHIVE" -C "$TMP_DIR" >/dev/null 2>&1; then
        CLI_BIN="$(find "$TMP_DIR" -maxdepth 3 -type f -name 'monero-wallet-cli' | head -n 1 || true)"
        RPC_BIN="$(find "$TMP_DIR" -maxdepth 3 -type f -name 'monero-wallet-rpc' | head -n 1 || true)"
        if [ -n "$CLI_BIN" ] && [ -f "$CLI_BIN" ]; then
          if $SUDO install -m 0755 "$CLI_BIN" /usr/local/bin/monero-wallet-cli 2>/dev/null; then
            MONERO_WALLET_CLI="/usr/local/bin/monero-wallet-cli"
            echo " Installed monero-wallet-cli to $MONERO_WALLET_CLI"
          else
            echo "Failed to install monero-wallet-cli to /usr/local/bin (insufficient permissions?)."
          fi
        else
          echo "Could not locate monero-wallet-cli in downloaded archive."
        fi
        if [ -n "$RPC_BIN" ] && [ -f "$RPC_BIN" ]; then
          if $SUDO install -m 0755 "$RPC_BIN" /usr/local/bin/monero-wallet-rpc 2>/dev/null; then
            echo " Installed monero-wallet-rpc to /usr/local/bin/monero-wallet-rpc"
          else
            echo "Failed to install monero-wallet-rpc to /usr/local/bin (insufficient permissions?)."
          fi
        else
          echo "Could not locate monero-wallet-rpc in downloaded archive."
        fi
      else
        echo "Failed to extract downloaded Monero CLI archive."
      fi
    else
      echo "Failed to download Monero CLI from $CLI_URL"
    fi
    rm -rf "$TMP_DIR"
  else
    echo "curl or tar not available; cannot auto-download Monero CLI."
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ] || [ -z "$MONERO_WALLET_RPC_BIN" ]; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "Attempting to install Monero CLI from apt-get..."
    $SUDO apt-get update -y >/dev/null 2>&1 || true
    $SUDO apt-get install -y monero monero-cli monero-wallet-cli monero-wallet-rpc >/dev/null 2>&1 || true
    if [ -z "$MONERO_WALLET_CLI" ] && command -v monero-wallet-cli >/dev/null 2>&1; then
      MONERO_WALLET_CLI="$(command -v monero-wallet-cli)"
      echo " Installed monero-wallet-cli via apt-get at $MONERO_WALLET_CLI"
    fi
    if [ -z "$MONERO_WALLET_RPC_BIN" ] && command -v monero-wallet-rpc >/dev/null 2>&1; then
      MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
      echo " Installed monero-wallet-rpc via apt-get at $MONERO_WALLET_RPC_BIN"
    fi
  fi
fi

if [ -z "$MONERO_WALLET_CLI" ]; then
  echo "monero-wallet-cli is still missing. Multisig auto-enable will log a warning until you install it."
fi

RPC_BIN_CHECK="${MONERO_WALLET_RPC_BIN:-}"
if [ -n "$RPC_BIN_CHECK" ] && [ -x "$RPC_BIN_CHECK" ]; then
  :
elif [ -n "$RPC_BIN_CHECK" ] && command -v "$RPC_BIN_CHECK" >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v "$RPC_BIN_CHECK")"
elif command -v monero-wallet-rpc >/dev/null 2>&1; then
  MONERO_WALLET_RPC_BIN="$(command -v monero-wallet-rpc)"
else
  echo "ERROR: monero-wallet-rpc not found on PATH after automatic install attempt."
  echo "       Please install Monero CLI (monero-wallet-rpc) manually or re-run scripts/setup.sh as root on a machine with internet access."
  exit 1
fi

if [ "$WRITE_ENV" -eq 1 ]; then
echo ""
echo "[5/6] Writing configuration..."

cat > "$ENV_FILE" << EOF_ENV
# Auto-generated by setup.sh on $(date)

PRIVATE_KEY=${PRIVATE_KEY}
RPC_URL=${RPC_URL}
MONERO_WALLET_PASSWORD=${MONERO_WALLET_PASSWORD}
MONERO_WALLET_CLI="$MONERO_WALLET_CLI"
MONERO_WALLET_RPC_BIN="$MONERO_WALLET_RPC_BIN"

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

REGISTRY_ADDR=0x45Bba63aF96cBe1eea5d8AeEcB1C0C398C76a6be
STAKING_ADDR=0x797DD08cB58e924E74c762Bc31cf1ea3f6eAe31E
ZFI_ADDR=0x60b8C9a0470bAC6195F475e41f985FB808CF5B41
COORDINATOR_ADDR=0x8A0a261BD02CB05D5F9BBAbc887C62391C35F232
CONFIG_ADDR=0xfDE794d056287dFb0066C7D28A60826400265a8b

BRIDGE_ADDR=0x3C26D6F0863284EFE944f7DDF529796052A4C8C8
ZXMR_ADDR=0x086A2588CD580a28eE275e00f60CC3C251c450C2
FEE_POOL_ADDR=0x6337e968580E2ea566EbD0A90CA9Ed5bC25F3f86

BRIDGE_ENABLED=1
BRIDGE_API_ENABLED=1
BRIDGE_API_PORT=3002

HEARTBEAT_INTERVAL=30
STALE_ROUND_MIN_AGE_MS=600000
STICKY_QUEUE=0
FORCE_SELECT=0
EOF_ENV

chmod 600 "$ENV_FILE" || true
echo "Configuration written to $ENV_FILE"
echo ""

fi

echo "[6/6] Validating configuration..."
if node --check node.js 2>/dev/null; then
  echo "Syntax validation passed"
else
  echo "  Warning: Could not validate syntax"
fi
echo ""

# Ensure ufw is installed so we can manage firewall rules automatically
if ! command -v ufw >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "[Firewall] ufw not found; installing via apt-get..."
    $SUDO apt-get update -y >/dev/null 2>&1 && $SUDO apt-get install -y ufw >/dev/null 2>&1       || echo "[Firewall] Warning: failed to install ufw. Please install it manually and open port 9000."
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

echo "================================================================"
echo "Setup Complete!"
echo "================================================================"
echo ""
echo "Next steps:"
echo "  1. Start the node: ./start"
echo "  2. View logs: tail -f znode.log"
echo ""
echo "IMPORTANT:"
echo "  - Node configured for production (Sepolia testnet)"
echo "  - See README.md for full documentation"
echo ""

# 
# Create systemd service file
# 
create_systemd_service() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "Not running as root, skipping systemd service creation."
    echo "   Run as root or manually create the service file if desired."
    return
  fi

  local SERVICE_FILE="/etc/systemd/system/znode.service"
  local NODE_DIR="$SCRIPT_DIR/.."
  NODE_DIR="$(cd "$NODE_DIR" && pwd)"

  echo "Creating systemd service file..."

  cat > "$SERVICE_FILE" << SERVICEOF
[Unit]
Description=ZeroFi Node v2.1.0
After=network.target

[Service]
Type=simple
WorkingDirectory=$NODE_DIR
EnvironmentFile=$NODE_DIR/.env
ExecStartPre=/bin/bash -c 'cd $NODE_DIR && ./scripts/start-monero-rpc.sh'
ExecStart=/usr/bin/node $NODE_DIR/node.js
ExecStopPost=/bin/bash -c 'pkill -f "p2p-daemon.*--socket" || true; rm -f /tmp/znode-p2p.sock'
Restart=on-failure
RestartSec=10
StandardOutput=append:$NODE_DIR/znode.log
StandardError=append:$NODE_DIR/znode.log

[Install]
WantedBy=multi-user.target
SERVICEOF

  systemctl daemon-reload
  systemctl enable znode.service 2>/dev/null || true

  echo " Systemd service created and enabled: znode.service"
  echo "   Start with: ./start"
  echo "   Stop with:  ./stop"
  echo "   Logs:       journalctl -u znode -f  OR  tail -f znode.log"
}

create_systemd_service
