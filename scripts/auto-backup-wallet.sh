#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="${WALLET_BACKUP_DIR:-$HOME/.znode-backup/wallets}"
WALLET_DIR="$HOME/.monero-wallets"

if [ -z "${WALLET_BACKUP_PASSPHRASE:-}" ]; then
  PASS_FILE="${WALLET_BACKUP_PASSPHRASE_FILE:-$HOME/.znode-backup/wallet_backup_passphrase.txt}"
  if [ -f "$PASS_FILE" ]; then
    WALLET_BACKUP_PASSPHRASE="$(cat "$PASS_FILE")"
  else
    echo "ERROR: ERROR: WALLET_BACKUP_PASSPHRASE environment variable is required"
    echo "   Set a strong passphrase in your .env file or environment, or let the node generate one on first startup."
    exit 1
  fi
fi

PASSPHRASE="$WALLET_BACKUP_PASSPHRASE"

# Create backup directory
mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"

# Find all znode wallets
WALLETS=$(find "$WALLET_DIR" -maxdepth 1 -name "znode_*" -type f ! -name "*.keys" 2>/dev/null || true)

if [ -z "$WALLETS" ]; then
  echo "  No znode wallets found in $WALLET_DIR"
  exit 0  # Not an error - just nothing to backup yet
fi

BACKUP_COUNT=0
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
  
  # Encrypt with AES-256-CBC + HMAC-SHA256 (encrypt-then-MAC for integrity)
  ENCRYPTED_FILE="${BACKUP_DIR}/${WALLET_NAME}.v2.enc"
  echo "$PASSPHRASE" | openssl enc -aes-256-cbc -salt -pbkdf2 -iter 100000 -in "$TAR_FILE" -out "$ENCRYPTED_FILE" -pass stdin
  
  MAC_KEY=$(echo -n "${PASSPHRASE}:mac" | openssl dgst -sha256 -binary | xxd -p -c 64)
  
  MAC=$(openssl dgst -sha256 -hmac "$MAC_KEY" -binary "$ENCRYPTED_FILE" | xxd -p -c 64)
  echo "$MAC" > "${ENCRYPTED_FILE}.mac"
  chmod 600 "${ENCRYPTED_FILE}.mac"
  
  # Remove unencrypted tar
  rm -f "$TAR_FILE"
  chmod 600 "$ENCRYPTED_FILE"
  
  BACKUP_COUNT=$((BACKUP_COUNT + 1))
done

if [ $BACKUP_COUNT -gt 0 ]; then
  echo " Backed up $BACKUP_COUNT wallet(s) to $BACKUP_DIR"
else
  echo "  No wallets were backed up"
fi

exit 0
