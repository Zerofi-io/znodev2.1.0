#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="/root/.znode-backup/wallets"
WALLET_DIR="$HOME/.monero-wallets"

echo ""
echo "            Monero Wallet Restore (Encrypted)                   "
echo ""
echo ""

if [ ! -d "$BACKUP_DIR" ]; then
  echo " No backup directory found at $BACKUP_DIR"
  exit 1
fi

# Find encrypted backups
BACKUPS=$(find "$BACKUP_DIR" -name "*.tar.enc" 2>/dev/null || true)

if [ -z "$BACKUPS" ]; then
  echo " No encrypted wallet backups found in $BACKUP_DIR"
  exit 1
fi

echo "Found encrypted backups:"
for backup in $BACKUPS; do
  basename "$backup"
done
echo ""

# Get decryption passphrase
read -s -p "Enter decryption passphrase: " PASSPHRASE
echo ""

if [ -z "$PASSPHRASE" ]; then
  echo " Passphrase cannot be empty"
  exit 1
fi

echo ""
echo "Restoring wallets..."

mkdir -p "$WALLET_DIR"

for backup in $BACKUPS; do
  BACKUP_NAME=$(basename "$backup")
  TAR_FILE="${backup%.enc}"
  
  # Decrypt
  echo "$PASSPHRASE" | openssl enc -aes-256-cbc -d -pbkdf2 -in "$backup" -out "$TAR_FILE" -pass stdin 2>/dev/null || {
    echo "   Failed to decrypt ${BACKUP_NAME} (wrong passphrase?)"
    rm -f "$TAR_FILE"
    continue
  }
  
  # Extract to wallet directory
  tar -xf "$TAR_FILE" -C "$WALLET_DIR" 2>/dev/null || {
    echo "   Failed to extract ${BACKUP_NAME}"
    rm -f "$TAR_FILE"
    continue
  }
  
  # Remove temporary tar
  rm -f "$TAR_FILE"
  
  echo "   Restored: ${BACKUP_NAME}"
done

# Fix permissions
chmod 700 "$WALLET_DIR"
find "$WALLET_DIR" -name "znode_*" -type f -exec chmod 600 {} \;

echo ""
echo " Wallet restore complete!"
echo "   Location: $WALLET_DIR"
