#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="/root/.znode-backup/wallets"
WALLET_DIR="$HOME/.monero-wallets"

echo ""
echo "              Monero Wallet Backup (Encrypted)                  "
echo ""
echo ""

# Create backup directory
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

# Find all znode wallets
WALLETS=$(find "$WALLET_DIR" -maxdepth 1 -name "znode_*" -type f ! -name "*.keys" 2>/dev/null || true)

if [ -z "$WALLETS" ]; then
  echo " No znode wallets found in $WALLET_DIR"
  exit 1
fi

echo "Found wallets to backup:"
for wallet in $WALLETS; do
  basename "$wallet"
done
echo ""

# Get encryption passphrase
read -s -p "Enter encryption passphrase for wallet backup: " PASSPHRASE
echo ""
read -s -p "Confirm passphrase: " PASSPHRASE2
echo ""

if [ "$PASSPHRASE" != "$PASSPHRASE2" ]; then
  echo " Passphrases do not match"
  exit 1
fi

if [ -z "$PASSPHRASE" ]; then
  echo " Passphrase cannot be empty"
  exit 1
fi

echo ""
echo "Backing up wallets..."

for wallet in $WALLETS; do
  WALLET_NAME=$(basename "$wallet")
  KEYS_FILE="${wallet}.keys"
  
  if [ ! -f "$KEYS_FILE" ]; then
    echo "  Warning: ${WALLET_NAME}.keys not found, skipping"
    continue
  fi
  
  # Create tar of wallet and keys file
  TAR_FILE="${BACKUP_DIR}/${WALLET_NAME}.tar"
  tar -cf "$TAR_FILE" -C "$WALLET_DIR" "$WALLET_NAME" "${WALLET_NAME}.keys" 2>/dev/null
  
  # Encrypt with OpenSSL
  ENCRYPTED_FILE="${TAR_FILE}.enc"
  echo "$PASSPHRASE" | openssl enc -aes-256-cbc -salt -pbkdf2 -in "$TAR_FILE" -out "$ENCRYPTED_FILE" -pass stdin
  
  # Remove unencrypted tar
  rm -f "$TAR_FILE"
  chmod 600 "$ENCRYPTED_FILE"
  
  echo "   Backed up: ${WALLET_NAME}  ${ENCRYPTED_FILE}"
done

echo ""
echo " Wallet backup complete!"
echo "   Location: $BACKUP_DIR"
echo "   Encryption: AES-256-CBC"
echo ""
echo "  IMPORTANT: Keep your passphrase safe!"
echo "   Without it, backups cannot be restored."
