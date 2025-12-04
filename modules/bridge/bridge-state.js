import fs from 'fs';
import path from 'path';

function getBridgeStatePath() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); } catch (_ignored) {}
  return path.join(dataDir, 'bridge_state.json');
}

export function loadBridgeState(node) {
  try {
    const statePath = getBridgeStatePath();
    if (!fs.existsSync(statePath)) return;
    const data = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    if (Array.isArray(data.processedDeposits)) {
      node._processedDeposits = new Set(data.processedDeposits);
      console.log(`[Bridge] Loaded ${node._processedDeposits.size} processed deposits from disk`);
    }
    if (data.pendingMintSignatures && typeof data.pendingMintSignatures === 'object') {
      node._pendingMintSignatures = new Map(Object.entries(data.pendingMintSignatures));
      console.log(`[Bridge] Loaded ${node._pendingMintSignatures.size} pending mint signatures from disk`);
    }
    if (data.pendingDepositRequests && typeof data.pendingDepositRequests === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingDepositRequests)) {
        if (v && typeof v === 'object' && typeof v.ethAddress === 'string') {
          map.set(k, { ethAddress: v.ethAddress, clusterId: v.clusterId || null, requestKey: v.requestKey || null });
        } else if (typeof v === 'string') {
          map.set(k, { ethAddress: v, clusterId: null, requestKey: null });
        }
      }
      node._pendingDepositRequests = map;
      console.log(`[Bridge] Loaded ${node._pendingDepositRequests.size} pending deposit requests from disk`);
    }
    if (Array.isArray(data.processedWithdrawals)) {
      const set = new Set();
      for (const h of data.processedWithdrawals) {
        if (typeof h === 'string') set.add(h.toLowerCase());
      }
      node._processedWithdrawals = set;
      console.log(`[Bridge] Loaded ${node._processedWithdrawals.size} processed withdrawals from disk`);
    }
    if (data.pendingWithdrawals && typeof data.pendingWithdrawals === 'object') {
      const map = new Map();
      for (const [k, v] of Object.entries(data.pendingWithdrawals)) {
        map.set(k.toLowerCase(), v);
      }
      node._pendingWithdrawals = map;
      console.log(`[Bridge] Loaded ${node._pendingWithdrawals.size} pending withdrawals from disk`);
    }
  } catch (e) {
    console.log('[Bridge] Failed to load bridge state:', e.message || String(e));
  }
}

export function saveBridgeState(node) {
  try {
    const statePath = getBridgeStatePath();
    const data = {
      savedAt: Date.now(),
      processedDeposits: node._processedDeposits ? Array.from(node._processedDeposits) : [],
      pendingMintSignatures: node._pendingMintSignatures ? Object.fromEntries(node._pendingMintSignatures) : {},
      pendingDepositRequests: node._pendingDepositRequests ? Object.fromEntries(node._pendingDepositRequests) : {},
      processedWithdrawals: node._processedWithdrawals ? Array.from(node._processedWithdrawals) : [],
      pendingWithdrawals: node._pendingWithdrawals ? Object.fromEntries(node._pendingWithdrawals) : {},
    };
    fs.writeFileSync(statePath, JSON.stringify(data, null, 2), { mode: 0o600 });
  } catch (e) {
    console.log('[Bridge] Failed to save bridge state:', e.message || String(e));
  }
}

export function startBridgeStateSaver(node) {
  const saveIntervalMs = Number(process.env.BRIDGE_STATE_SAVE_INTERVAL_MS || 60000);
  if (node._bridgeStateSaverTimer) clearInterval(node._bridgeStateSaverTimer);
  node._bridgeStateSaverTimer = setInterval(() => { saveBridgeState(node); }, saveIntervalMs);
}

function cleanupExpiredSignatures(node) {
  if (!node._pendingMintSignatures || node._pendingMintSignatures.size === 0) return;
  const ttlMs = Number(process.env.MINT_SIGNATURE_TTL_MS || 86400000);
  const now = Date.now();
  let expiredCount = 0;
  for (const [txid, data] of node._pendingMintSignatures) {
    if (data.timestamp && now - data.timestamp > ttlMs) {
      node._pendingMintSignatures.delete(txid);
      expiredCount += 1;
    }
  }
  if (expiredCount > 0) {
    console.log(`[Bridge] Cleaned up ${expiredCount} expired mint signatures`);
    saveBridgeState(node);
  }
}

function cleanupOldProcessedDeposits(node) {
  if (!node._processedDeposits || node._processedDeposits.size === 0) return;
  const maxEntries = Number(process.env.MAX_PROCESSED_DEPOSITS || 10000);
  if (node._processedDeposits.size > maxEntries) {
    const arr = Array.from(node._processedDeposits);
    const toRemove = arr.length - maxEntries;
    for (let i = 0; i < toRemove; i += 1) {
      node._processedDeposits.delete(arr[i]);
    }
    console.log(`[Bridge] Pruned ${toRemove} old processed deposit entries`);
    saveBridgeState(node);
  }
}

export function startCleanupTimer(node) {
  const cleanupIntervalMs = Number(process.env.BRIDGE_CLEANUP_INTERVAL_MS || 3600000);
  if (node._bridgeCleanupTimer) clearInterval(node._bridgeCleanupTimer);
  node._bridgeCleanupTimer = setInterval(() => {
    cleanupExpiredSignatures(node);
    cleanupOldProcessedDeposits(node);
  }, cleanupIntervalMs);
}
