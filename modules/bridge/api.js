import http from 'http';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { ethers } from 'ethers';
import { metrics } from '../core/metrics.js';
import consoleLogger from '../core/console-shim.js';
import { isValidMoneroAddress } from './bridge-service.js';

const MoneroHealth = {
  HEALTHY: 'HEALTHY',
  NEEDS_ATTEMPT_RESET: 'NEEDS_ATTEMPT_RESET',
  NEEDS_GLOBAL_RESET: 'NEEDS_GLOBAL_RESET',
  QUARANTINED: 'QUARANTINED',
};

function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}

function isBridgeApiEnabled() {
  const v = process.env.BRIDGE_API_ENABLED;
  return v === undefined || v === '1';
}

export function startBridgeAPI(node) {
  const apiEnabled = isBridgeApiEnabled();
  if (!apiEnabled) {
    console.log('[BridgeAPI] API server disabled by configuration (BRIDGE_API_ENABLED=0)');
    return;
  }
  const port = Number(process.env.BRIDGE_API_PORT || 3002);
  node._apiRateLimits = new Map();
  const rateLimitWindowMs = Number(process.env.API_RATE_LIMIT_WINDOW_MS || 60000);
  const rateLimitMaxRequests = Number(process.env.API_RATE_LIMIT_MAX_REQUESTS || 60);
  node._apiServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', process.env.BRIDGE_API_CORS_ORIGIN || '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') {
      res.writeHead(200);
      res.end();
      return;
    }
    const clientIP = req.socket?.remoteAddress || req.connection?.remoteAddress || 'unknown';
    const now = Date.now();
    let rateInfo = node._apiRateLimits.get(clientIP);
    if (!rateInfo || now - rateInfo.windowStart > rateLimitWindowMs) {
      rateInfo = { windowStart: now, count: 0 };
    }
    rateInfo.count += 1;
    node._apiRateLimits.set(clientIP, rateInfo);
    if (node._apiRateLimits.size > 500) {
      const cutoff = now - rateLimitWindowMs;
      for (const [ip, info] of node._apiRateLimits) {
        if (info.windowStart < cutoff) {
          node._apiRateLimits.delete(ip);
        }
      }
    }
    if (rateInfo.count > rateLimitMaxRequests) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: 'Too many requests',
        retryAfter: Math.ceil((rateInfo.windowStart + rateLimitWindowMs - now) / 1000),
      }));
      return;
    }
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;
    handleAPIRequest(node, req, res, pathname, url.searchParams);
  });
  node._apiServer.listen(port, () => {
    console.log(`[BridgeAPI] HTTP API server listening on port ${port}`);
  });
  if (!node._apiRateLimitCleanupTimer) {
    node._apiRateLimitCleanupTimer = setInterval(() => {
      if (!node._apiRateLimits) return;
      const cutoff = Date.now() - rateLimitWindowMs;
      for (const [ip, info] of node._apiRateLimits) {
        if (info.windowStart < cutoff) {
          node._apiRateLimits.delete(ip);
        }
      }
    }, 60000);
  }
  node._apiServer.on('error', (e) => {
    console.log('[BridgeAPI] Server error:', e.message || String(e));
  });
}

export function stopBridgeAPI(node) {
  if (node._apiRateLimitCleanupTimer) {
    clearInterval(node._apiRateLimitCleanupTimer);
    node._apiRateLimitCleanupTimer = null;
  }
  if (node._apiServer) {
    node._apiServer.close();
    node._apiServer = null;
    console.log('[BridgeAPI] API server stopped');
  }
}

async function handleAPIRequest(node, req, res, pathname, params) {
  try {
    if (pathname === '/healthz' && req.method === 'GET') {
      const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
      const circuit = {
        open: !!node._rpcCircuitOpen,
        halfOpen: !!node._rpcCircuitHalfOpen,
        openUntil: node._rpcCircuitOpenUntil || 0,
      };
      const p2pStatus = {
        connected: !!(node.p2p && node.p2p.node),
        connectedPeers: node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0,
      };
      const cluster = {
        activeClusterId: node._activeClusterId || null,
        clusterFinalized: !!node._clusterFinalized,
        clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0,
      };
      let status = 'ok';
      if (circuit.open || moneroHealth === MoneroHealth.QUARANTINED) {
        status = 'error';
      } else if (moneroHealth !== MoneroHealth.HEALTHY || !p2pStatus.connected) {
        status = 'degraded';
      }
      return jsonResponse(res, 200, { status, monero: { health: moneroHealth, errors: node.moneroErrorCounts || {} }, rpcCircuitBreaker: circuit, p2p: p2pStatus, cluster });
    }
    if (pathname === '/metrics' && req.method === 'GET') {
      const clusterMetrics = metrics.toJSON();
      const loggerMetrics = consoleLogger ? consoleLogger.getMetrics() : null;
      return jsonResponse(res, 200, { metrics: clusterMetrics, logger: loggerMetrics });
    }
    if (pathname === '/cluster-status' && req.method === 'GET') {
      const status = await getClusterStatus(node);
      return jsonResponse(res, 200, status);
    }
    if (pathname === '/health' && req.method === 'GET') {
      return jsonResponse(res, 200, {
        status: 'ok',
        clusterFinalized: node._clusterFinalized,
        multisigAddress: node._clusterFinalAddress || null,
        bridgeEnabled: isBridgeEnabled(),
      });
    }
    if (pathname === '/bridge/info' && req.method === 'GET') {
      return jsonResponse(res, 200, {
        multisigAddress: node._clusterFinalAddress || null,
        clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0,
        clusterFinalized: node._clusterFinalized,
        bridgeContract: node.bridge ? node.bridge.target || node.bridge.address : null,
        clusterId: node._activeClusterId || null,
        minConfirmations: Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 10),
      });
    }
    if (pathname === '/bridge/cluster/members' && req.method === 'GET') {
      const members = node._clusterMembers || [];
      return jsonResponse(res, 200, { members });
    }
    if (pathname === '/bridge/deposit/request' && req.method === 'POST') {
      const body = await readRequestBody(req);
      const { ethAddress } = body;
      if (!ethAddress || !ethers.isAddress(ethAddress)) {
        return jsonResponse(res, 400, { error: 'Invalid ethAddress' });
      }
      try {
        const result = await registerDepositRequest(node, ethAddress);
        return jsonResponse(res, 200, result);
      } catch (e) {
        return jsonResponse(res, 503, { error: e.message || 'Deposit routing unavailable' });
      }
    }
    if (pathname.startsWith('/bridge/deposit/status/') && req.method === 'GET') {
      const txid = pathname.split('/').pop();
      if (!txid || txid.length < 10) {
        return jsonResponse(res, 400, { error: 'Invalid txid' });
      }
      const signature = getMintSignature(node, txid);
      if (signature) {
        return jsonResponse(res, 200, { status: 'ready', ...signature });
      }
      if (node._processedDeposits && node._processedDeposits.has(txid)) {
        return jsonResponse(res, 200, { status: 'processed', message: 'Deposit processed but no signature available (may have failed consensus)' });
      }
      return jsonResponse(res, 200, { status: 'pending', message: 'Deposit not yet detected or confirmed' });
    }
    if (pathname === '/bridge/signatures' && req.method === 'GET') {
      const signatures = getAllPendingSignatures(node);
      return jsonResponse(res, 200, { signatures });
    }
    if (pathname.startsWith('/bridge/withdrawal/status/') && req.method === 'GET') {
      const txHash = pathname.split('/').pop();
      if (!txHash || txHash.length < 10) {
        return jsonResponse(res, 400, { error: 'Invalid txHash' });
      }
      const status = getWithdrawalStatus(node, txHash);
      if (status) {
        return jsonResponse(res, 200, { status: 'found', ...status });
      }
      return jsonResponse(res, 200, { status: 'not_found', message: 'Withdrawal not yet detected or processed' });
    }
    if (pathname === '/bridge/withdrawals' && req.method === 'GET') {
      const withdrawals = getAllPendingWithdrawals(node);
      return jsonResponse(res, 200, { withdrawals });
    }
    return jsonResponse(res, 404, { error: 'Not found' });
  } catch (e) {
    console.log('[BridgeAPI] Request error:', e.message || String(e));
    return jsonResponse(res, 500, { error: 'Internal server error' });
  }
}

async function getClusterStatus(node) {
  const now = Date.now();
  const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
  const stateSummary = node.stateMachine && typeof node.stateMachine.getSummary === 'function' ? node.stateMachine.getSummary() : null;
  const clusterState = node.clusterState && node.clusterState.state ? node.clusterState.state : null;
  const clusterId = (clusterState && clusterState.clusterId) || node._activeClusterId || null;
  const finalized = clusterState && typeof clusterState.finalized === 'boolean' ? clusterState.finalized : !!node._clusterFinalized;
  const finalAddress = (clusterState && clusterState.finalAddress) || node._clusterFinalAddress || null;
  const selfAddr = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const membersRaw = clusterState && Array.isArray(clusterState.members) ? clusterState.members : node._clusterMembers || [];
  const members = [];
  const nowSec = Math.floor(now / 1000);
  const hbRaw = process.env.HEARTBEAT_INTERVAL;
  const hbParsed = hbRaw != null ? Number(hbRaw) : NaN;
  const hbIntervalSec = Number.isFinite(hbParsed) && hbParsed > 0 ? hbParsed : 30;
  const staleThresholdSec = hbIntervalSec * 5;
  for (const addr of membersRaw) {
    let lastHeartbeatAgoSec = null;
    let status = 'unknown';
    if (node.p2p && typeof node.p2p.getLastHeartbeat === 'function') {
      try {
        const hb = await node.p2p.getLastHeartbeat(addr);
        if (hb && hb.timestamp != null) {
          const tsSec = Number(hb.timestamp);
          if (Number.isFinite(tsSec) && tsSec > 0) {
            const ageSec = Math.max(0, nowSec - tsSec);
            lastHeartbeatAgoSec = ageSec;
            status = ageSec <= staleThresholdSec ? 'healthy' : 'stale';
          }
        }
      } catch (_ignored) {}
    }
    members.push({ address: addr, isSelf: selfAddr ? addr.toLowerCase() === selfAddr : false, lastHeartbeatAgoSec, status });
  }
  const cooldownUntil = (clusterState && clusterState.cooldownUntil) || node._clusterCooldownUntil || null;
  const cooldownRemainingSec = cooldownUntil && cooldownUntil > now ? Math.floor((cooldownUntil - now) / 1000) : 0;
  const lastFailureAt = (clusterState && clusterState.lastFailureAt) || node._lastClusterFailureAt || null;
  const lastFailureReason = (clusterState && clusterState.lastFailureReason) || node._lastClusterFailureReason || null;
  const p2pConnected = !!(node.p2p && node.p2p.node);
  const p2pConnectedPeers = node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0;
  const lastHeartbeatAgoSec = node._lastHeartbeatAt && node._lastHeartbeatAt > 0 ? Math.floor((now - node._lastHeartbeatAt) / 1000) : null;
  const allMembersHealthy = members.length > 0 && members.every((m) => m.status === 'healthy');
  const currentState = stateSummary && stateSummary.state ? stateSummary.state : node.stateMachine && node.stateMachine.currentState ? node.stateMachine.currentState : null;
  const eligibleForBridging = !!clusterId && finalized && currentState === 'ACTIVE' && moneroHealth === MoneroHealth.HEALTHY && p2pConnected && allMembersHealthy && isBridgeEnabled();
  const syncMinVisibility = Number(process.env.MIN_P2P_VISIBILITY || members.length || 0);
  const jitterRawEnv = process.env.NON_COORDINATOR_JITTER_MS;
  const jitterParsedEnv = jitterRawEnv != null ? Number(jitterRawEnv) : NaN;
  const nonCoordinatorJitterMs = Number.isFinite(jitterParsedEnv) && jitterParsedEnv >= 0 ? jitterParsedEnv : 5000;
  const warmupRawEnv = process.env.P2P_WARMUP_MS;
  const warmupParsedEnv = warmupRawEnv != null ? Number(warmupRawEnv) : NaN;
  const p2pWarmupMs = Number.isFinite(warmupParsedEnv) && warmupParsedEnv >= 0 ? warmupParsedEnv : 30000;
  const readyRawEnv = process.env.READY_BARRIER_TIMEOUT_MS;
  const readyParsedEnv = readyRawEnv != null ? Number(readyRawEnv) : NaN;
  const readyBarrierTimeoutMs = Number.isFinite(readyParsedEnv) && readyParsedEnv > 0 ? readyParsedEnv : 300000;
  const attemptsRawEnv = process.env.LIVENESS_ATTEMPTS;
  const attemptsParsedEnv = attemptsRawEnv != null ? Number(attemptsRawEnv) : NaN;
  const livenessAttempts = Number.isFinite(attemptsParsedEnv) && attemptsParsedEnv > 0 ? Math.floor(attemptsParsedEnv) : 3;
  const intervalRawEnv = process.env.LIVENESS_ATTEMPT_INTERVAL_MS;
  const intervalParsedEnv = intervalRawEnv != null ? Number(intervalRawEnv) : NaN;
  const livenessAttemptIntervalMs = Number.isFinite(intervalParsedEnv) && intervalParsedEnv >= 0 ? intervalParsedEnv : 30000;
  return {
    clusterState: currentState,
    timeInStateMs: stateSummary && typeof stateSummary.timeInState === 'number' ? stateSummary.timeInState : node.stateMachine && typeof node.stateMachine.timeInState === 'number' ? node.stateMachine.timeInState : null,
    eligibleForBridging,
    syncConfig: { minP2PVisibility: syncMinVisibility, nonCoordinatorJitterMs, p2pWarmupMs, readyBarrierTimeoutMs, livenessAttempts, livenessAttemptIntervalMs },
    cluster: { id: clusterId, finalized, finalAddress, members, size: members.length, coordinator: clusterState && clusterState.coordinator ? clusterState.coordinator : null, coordinatorIndex: clusterState && typeof clusterState.coordinatorIndex === 'number' ? clusterState.coordinatorIndex : null, cooldownUntil, cooldownRemainingSec, lastFailureAt, lastFailureReason },
    monero: { health: moneroHealth, errors: node.moneroErrorCounts || {} },
    p2p: { connected: p2pConnected, connectedPeers: p2pConnectedPeers, lastHeartbeatAgoSec },
  };
}

function jsonResponse(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : {}); }
      catch { resolve({}); }
    });
    req.on('error', reject);
  });
}

function getBridgeStatePath() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try { fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 }); }
  catch (_ignored) {}
  return path.join(dataDir, 'bridge_state.json');
}

function loadBridgeState(node) {
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
      node._processedWithdrawals = new Set(data.processedWithdrawals);
      console.log(`[Bridge] Loaded ${node._processedWithdrawals.size} processed withdrawals from disk`);
    }
  } catch (e) {
    console.log('[Bridge] Failed to load bridge state:', e.message || String(e));
  }
}

function saveBridgeState(node) {
  try {
    const statePath = getBridgeStatePath();
    const data = {
      savedAt: Date.now(),
      processedDeposits: node._processedDeposits ? Array.from(node._processedDeposits) : [],
      pendingMintSignatures: node._pendingMintSignatures ? Object.fromEntries(node._pendingMintSignatures) : {},
      pendingDepositRequests: node._pendingDepositRequests ? Object.fromEntries(node._pendingDepositRequests) : {},
      processedWithdrawals: node._processedWithdrawals ? Array.from(node._processedWithdrawals) : [],
    };
    fs.writeFileSync(statePath, JSON.stringify(data, null, 2), { mode: 0o600 });
  } catch (e) {
    console.log('[Bridge] Failed to save bridge state:', e.message || String(e));
  }
}

function startBridgeStateSaver(node) {
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

function startCleanupTimer(node) {
  const cleanupIntervalMs = Number(process.env.BRIDGE_CLEANUP_INTERVAL_MS || 3600000);
  if (node._bridgeCleanupTimer) clearInterval(node._bridgeCleanupTimer);
  node._bridgeCleanupTimer = setInterval(() => {
    cleanupExpiredSignatures(node);
    cleanupOldProcessedDeposits(node);
  }, cleanupIntervalMs);
}

export async function startDepositMonitor(node) {
  if (node._depositMonitorRunning) return;
  const bridgeEnabled = isBridgeEnabled();
  if (!bridgeEnabled) {
    console.log('[Bridge] Deposit monitoring disabled by configuration (BRIDGE_ENABLED=0)');
    return;
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    console.log('[Bridge] Cannot start deposit monitor: cluster not finalized');
    return;
  }
  if (!node.bridge) {
    console.log('[Bridge] Cannot start deposit monitor: bridge contract not configured');
    return;
  }
  loadBridgeState(node);
  node._depositMonitorRunning = true;
  const pollIntervalMs = Number(process.env.DEPOSIT_POLL_INTERVAL_MS || 30000);
  const minConfirmations = Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 10);
  console.log(`[Bridge] Starting deposit monitor (poll: ${pollIntervalMs / 1000}s, minConf: ${minConfirmations})`);
  const poll = async () => {
    if (!node._depositMonitorRunning || !node._clusterFinalized) return;
    try { await checkForDeposits(node, minConfirmations); }
    catch (e) { console.log('[Bridge] Deposit check error:', e.message || String(e)); }
  };
  await poll();
  node._depositMonitorTimer = setInterval(poll, pollIntervalMs);
  startBridgeStateSaver(node);
  startCleanupTimer(node);
}

export function stopDepositMonitor(node) {
  node._depositMonitorRunning = false;
  if (node._depositMonitorTimer) {
    clearInterval(node._depositMonitorTimer);
    node._depositMonitorTimer = null;
  }
  console.log('[Bridge] Deposit monitor stopped');
}

const DEPOSIT_REQUEST_ROUND = 9700;
const GLOBAL_BRIDGE_CLUSTER_ID = 'GLOBAL_BRIDGE';
const GLOBAL_BRIDGE_SESSION = 'bridge-global';
const MINT_SIGNATURE_ROUND = 9800;
const MINT_SIGNATURE_COLLECT_ROUND = 9801;
const REQUIRED_MINT_SIGNATURES = 7;
const MINT_SIGNATURE_TIMEOUT_MS = Number(process.env.MINT_SIGNATURE_TIMEOUT_MS || 120000);
const MAX_STAKE_QUERY_FAILURES = 3;

async function isNodeStaked(node, address) {
  if (!node.staking) return false;
  try {
    const info = await node.staking.getNodeInfo(address);
    const stakedAmount = BigInt(info[0].toString());
    const active = info[3];
    return active && stakedAmount >= BigInt('1000000000000000000000000');
  } catch (e) {
    return false;
  }
}

function signDepositRequestPayload(wallet, paymentId, ethAddress, clusterId, requestKey) {
  const message = ethers.solidityPackedKeccak256(
    ['string', 'string', 'address', 'bytes32', 'bytes32'],
    ['deposit-request', paymentId, ethAddress, clusterId || ethers.ZeroHash, requestKey || ethers.ZeroHash]
  );
  return wallet.signMessage(ethers.getBytes(message));
}

function verifyDepositRequestSignature(paymentId, ethAddress, clusterId, requestKey, signature, expectedSigner) {
  try {
    const message = ethers.solidityPackedKeccak256(
      ['string', 'string', 'address', 'bytes32', 'bytes32'],
      ['deposit-request', paymentId, ethAddress, clusterId || ethers.ZeroHash, requestKey || ethers.ZeroHash]
    );
    const recovered = ethers.verifyMessage(ethers.getBytes(message), signature);
    return recovered.toLowerCase() === expectedSigner.toLowerCase();
  } catch (e) {
    return false;
  }
}

async function syncDepositRequests(node) {
  if (!node.p2p || !node.staking) return;
  let members = [];
  try { members = await node.staking.getActiveNodes(); }
  catch (e) {}
  try {
    const payloads = await node.p2p.getPeerPayloads(GLOBAL_BRIDGE_CLUSTER_ID, GLOBAL_BRIDGE_SESSION, DEPOSIT_REQUEST_ROUND, members || []);
    if (!payloads || payloads.length === 0) return;
    if (!node._pendingDepositRequests) node._pendingDepositRequests = new Map();
    let updated = false;
    for (const payload of payloads) {
      try {
        const data = JSON.parse(payload);
        if (!data || data.type !== 'deposit-request') continue;
        if (!data.paymentId || !data.ethAddress || !data.signer || !data.signature) continue;
        if (!ethers.isAddress(data.ethAddress)) continue;
        if (!verifyDepositRequestSignature(data.paymentId, data.ethAddress, data.clusterId, data.requestKey, data.signature, data.signer)) continue;
        const signerStaked = await isNodeStaked(node, data.signer);
        if (!signerStaked) continue;
        if (node._activeClusterId && data.clusterId && typeof data.clusterId === 'string') {
          if (data.clusterId.toLowerCase() !== node._activeClusterId.toLowerCase()) continue;
        }
        if (!node._pendingDepositRequests.has(data.paymentId)) {
          node._pendingDepositRequests.set(data.paymentId, { ethAddress: data.ethAddress, clusterId: data.clusterId || null, requestKey: data.requestKey || null });
          updated = true;
        }
      } catch (e) {}
    }
    if (updated) saveBridgeState(node);
  } catch (e) {}
}

async function checkForDeposits(node, minConfirmations) {
  await syncDepositRequests(node);
  if (!node.monero) return;
  try { await node.monero.refresh(); }
  catch (_ignored) {}
  let deposits;
  try { deposits = await node.monero.getConfirmedDeposits(minConfirmations); }
  catch (e) {
    console.log('[Bridge] Failed to get deposits:', e.message || String(e));
    return;
  }
  if (!deposits || deposits.length === 0) return;
  for (const deposit of deposits) {
    const txid = deposit.txid;
    if (node._processedDeposits && node._processedDeposits.has(txid)) continue;
    if (node._pendingMintSignatures && node._pendingMintSignatures.has(txid)) continue;
    console.log(`[Bridge] New deposit detected: ${txid}`);
    console.log(`  Amount: ${deposit.amount / 1e12} XMR`);
    console.log(`  Confirmations: ${deposit.confirmations}`);
    console.log(`  Payment ID: ${deposit.paymentId || 'none'}`);
    const recipient = parseRecipientFromPaymentId(node, deposit.paymentId);
    if (!recipient) {
      console.log(`[Bridge] Skipping deposit ${txid}: no valid recipient in payment ID`);
      if (!node._processedDeposits) node._processedDeposits = new Set();
      node._processedDeposits.add(txid);
      continue;
    }
    try {
      const consensusResult = await runDepositConsensus(node, deposit, recipient);
      if (consensusResult.success) {
        console.log(`[Bridge] Consensus reached for deposit ${txid}`);
        await generateAndShareMintSignature(node, deposit, recipient);
        await collectMintSignatures(node, deposit.txid);
      } else {
        console.log(`[Bridge] Consensus failed for deposit ${txid}: ${consensusResult.reason}`);
      }
    } catch (e) {
      console.log(`[Bridge] Consensus error for deposit ${txid}:`, e.message || String(e));
    }
    if (!node._processedDeposits) node._processedDeposits = new Set();
    node._processedDeposits.add(txid);
    saveBridgeState(node);
  }
}

function parseRecipientFromPaymentId(node, paymentId) {
  if (!paymentId || paymentId.length === 0) return null;
  let candidate = paymentId;
  if (candidate.length === 40 && /^[0-9a-fA-F]{40}$/.test(candidate)) {
    candidate = '0x' + candidate;
  }
  if (candidate.length === 42 && candidate.startsWith('0x')) {
    try { return ethers.getAddress(candidate); }
    catch (_ignored) {}
  }
  if (node._pendingDepositRequests && node._pendingDepositRequests.has(paymentId)) {
    const entry = node._pendingDepositRequests.get(paymentId);
    if (entry && typeof entry === 'object' && typeof entry.ethAddress === 'string') return entry.ethAddress;
    if (typeof entry === 'string') return entry;
  }
  return null;
}

async function runDepositConsensus(node, deposit, recipient) {
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const depositHash = ethers.keccak256(ethers.AbiCoder.defaultAbiCoder().encode(['string', 'uint256', 'uint256', 'address'], [deposit.txid, deposit.amount, deposit.blockHeight, recipient]));
  console.log(`[Bridge] Running PBFT consensus for deposit ${deposit.txid.slice(0, 16)}...`);
  const topic = `deposit-${deposit.txid.slice(0, 16)}`;
  const timeoutMs = Number(process.env.DEPOSIT_CONSENSUS_TIMEOUT_MS || 120000);
  try {
    const result = await node.p2p.runConsensus(clusterId, sessionId, topic, depositHash, node._clusterMembers, timeoutMs);
    if (result && result.success) return { success: true };
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}

function isClusterCoordinator(node) {
  if (!node._clusterMembers || node._clusterMembers.length === 0) return false;
  const sorted = [...node._clusterMembers].map((a) => a.toLowerCase()).sort();
  return sorted[0] === node.wallet.address.toLowerCase();
}

async function generateAndShareMintSignature(node, deposit, recipient) {
  if (!node.wallet) {
    console.log('[Bridge] Cannot generate signature: no wallet');
    return null;
  }
  const depositId = ethers.keccak256(ethers.toUtf8Bytes(deposit.txid));
  const amountWei = BigInt(deposit.amount) * BigInt(1e6);
  const clusterId = node._activeClusterId || ethers.ZeroHash;
  const messageHash = ethers.keccak256(ethers.solidityPacked(['address', 'bytes32', 'bytes32', 'uint256'], [recipient, clusterId, depositId, amountWei]));
  const signature = await node.wallet.signMessage(ethers.getBytes(messageHash));
  console.log(`[Bridge] Generated mint signature for ${recipient}`);
  if (!node._pendingMintSignatures) node._pendingMintSignatures = new Map();
  const sigData = {
    clusterId,
    depositId,
    amount: amountWei.toString(),
    recipient,
    signatures: [{ signer: node.wallet.address, signature }],
    xmrTxid: deposit.txid,
    timestamp: Date.now(),
  };
  node._pendingMintSignatures.set(deposit.txid, sigData);
  try {
    await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', MINT_SIGNATURE_ROUND, JSON.stringify({
      type: 'mint-signature',
      xmrTxid: deposit.txid,
      depositId,
      amount: amountWei.toString(),
      recipient,
      signer: node.wallet.address,
      signature,
    }));
  } catch (e) {
    console.log('[Bridge] Failed to broadcast signature:', e.message || String(e));
  }
  return sigData;
}

async function collectMintSignatures(node, xmrTxid) {
  if (!node._pendingMintSignatures || !node._pendingMintSignatures.has(xmrTxid)) return;
  const sigData = node._pendingMintSignatures.get(xmrTxid);
  try {
    const complete = await node.p2p.waitForRoundCompletion(node._activeClusterId, node._sessionId || 'bridge', MINT_SIGNATURE_COLLECT_ROUND, node._clusterMembers, MINT_SIGNATURE_TIMEOUT_MS);
    if (!complete) console.log('[Bridge] Timeout waiting for mint signatures');
    const payloads = await node.p2p.getPeerPayloads(node._activeClusterId, node._sessionId || 'bridge', MINT_SIGNATURE_ROUND, node._clusterMembers);
    const existingSigners = new Set(sigData.signatures.map((s) => s.signer.toLowerCase()));
    for (const payload of payloads) {
      try {
        const data = JSON.parse(payload);
        if (data.type !== 'mint-signature' || data.xmrTxid !== xmrTxid) continue;
        if (existingSigners.has(data.signer.toLowerCase())) continue;
        sigData.signatures.push({ signer: data.signer, signature: data.signature });
        existingSigners.add(data.signer.toLowerCase());
      } catch (_ignored) {}
    }
    console.log(`[Bridge] Collected ${sigData.signatures.length} mint signatures for ${xmrTxid.slice(0, 16)}...`);
  } catch (e) {
    console.log('[Bridge] Error collecting signatures:', e.message || String(e));
  }
}

export function getMintSignature(node, xmrTxid) {
  if (!node._pendingMintSignatures) return null;
  const data = node._pendingMintSignatures.get(xmrTxid);
  if (!data) return null;
  return {
    clusterId: data.clusterId,
    depositId: data.depositId,
    amount: data.amount,
    recipient: data.recipient,
    signatures: data.signatures || [],
    xmrTxid: data.xmrTxid,
    timestamp: data.timestamp,
    ready: (data.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
  };
}

export function getAllPendingSignatures(node) {
  if (!node._pendingMintSignatures) return [];
  const result = [];
  for (const [txid, data] of node._pendingMintSignatures) {
    result.push({
      xmrTxid: txid,
      clusterId: data.clusterId,
      depositId: data.depositId,
      amount: data.amount,
      recipient: data.recipient,
      signatures: data.signatures || [],
      timestamp: data.timestamp,
      ready: (data.signatures || []).length >= REQUIRED_MINT_SIGNATURES,
    });
  }
  return result;
}

async function selectClusterForDeposit(node, ethAddress, paymentId) {
  if (!node.registry || !node.staking) {
    return { clusterId: node._activeClusterId || ethers.ZeroHash, requestKey: null };
  }
  let clusters;
  try { clusters = await node.registry.getActiveClusters(); }
  catch (e) {
    throw new Error('Failed to fetch active clusters');
  }
  if (!clusters || clusters.length === 0) {
    return { clusterId: node._activeClusterId || ethers.ZeroHash, requestKey: null };
  }
  const requestKeyHex = ethers.keccak256(ethers.solidityPacked(['address', 'string'], [ethAddress, paymentId]));
  const requestKey = ethers.toBigInt(requestKeyHex);
  const stakes = [];
  let total = 0n;
  let nonZero = false;
  let failedQueries = 0;
  for (const id of clusters) {
    let stake = 0n;
    try {
      const v = await node.registry.getClusterStake(id);
      stake = BigInt(v.toString());
    } catch (e) {
      failedQueries++;
      if (failedQueries >= MAX_STAKE_QUERY_FAILURES) {
        throw new Error('Too many stake query failures');
      }
      stake = 0n;
    }
    stakes.push(stake);
    total += stake;
    if (stake > 0n) nonZero = true;
  }
  let selected = null;
  if (nonZero && total > 0n) {
    const r = requestKey % total;
    let acc = 0n;
    for (let i = 0; i < clusters.length; i += 1) {
      acc += stakes[i];
      if (r < acc) {
        selected = clusters[i];
        break;
      }
    }
    if (!selected) selected = clusters[clusters.length - 1];
  } else {
    const idx = Number(requestKey % BigInt(clusters.length));
    selected = clusters[idx];
  }
  return { clusterId: selected, requestKey: requestKeyHex };
}

export async function registerDepositRequest(node, ethAddress, paymentId) {
  if (!node._pendingDepositRequests) node._pendingDepositRequests = new Map();
  let id = paymentId;
  if (!id) id = crypto.randomBytes(8).toString('hex');
  const sel = await selectClusterForDeposit(node, ethAddress, id);
  let clusterId = sel.clusterId || node._activeClusterId || ethers.ZeroHash;
  let requestKey = sel.requestKey || null;
  let finalAddress = null;
  if (node.registry && clusterId && clusterId !== ethers.ZeroHash) {
    try {
      const info = await node.registry.clusters(clusterId);
      if (info && info[0] && info[2]) finalAddress = info[0];
    } catch (e) {}
  }
  if (!finalAddress && node._clusterFinalAddress) finalAddress = node._clusterFinalAddress;
  if (!finalAddress) throw new Error('No active clusters');
  node._pendingDepositRequests.set(id, { ethAddress, clusterId, requestKey });
  if (node.p2p && node.staking && node.wallet) {
    try {
      const signature = await signDepositRequestPayload(node.wallet, id, ethAddress, clusterId, requestKey);
      const payload = JSON.stringify({
        type: 'deposit-request',
        paymentId: id,
        ethAddress,
        clusterId,
        requestKey,
        signer: node.wallet.address,
        signature,
      });
      await node.p2p.broadcastRoundData(GLOBAL_BRIDGE_CLUSTER_ID, GLOBAL_BRIDGE_SESSION, DEPOSIT_REQUEST_ROUND, payload);
    } catch (e) {}
  }
  saveBridgeState(node);
  let integratedAddress = null;
  if (node.monero) {
    try { integratedAddress = await node.monero.makeIntegratedAddress(id, finalAddress); }
    catch (e) {}
  }
  return { paymentId: id, multisigAddress: finalAddress, integratedAddress: integratedAddress || finalAddress, clusterId, ethAddress };
}

export async function startWithdrawalMonitor(node) {
  if (node._withdrawalMonitorRunning) return;
  const bridgeEnabled = isBridgeEnabled();
  if (!bridgeEnabled) {
    console.log('[Withdrawal] Withdrawal monitoring disabled by configuration (BRIDGE_ENABLED=0)');
    return;
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    console.log('[Withdrawal] Cannot start withdrawal monitor: cluster not finalized');
    return;
  }
  if (!node.bridge) {
    console.log('[Withdrawal] Cannot start withdrawal monitor: bridge contract not configured');
    return;
  }
  node._withdrawalMonitorRunning = true;
  node._processedWithdrawals = node._processedWithdrawals || new Set();
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  console.log('[Withdrawal] Starting withdrawal event monitor...');
  try {
    const activeClusterId = node._activeClusterId;
    if (!activeClusterId) {
      console.log('[Withdrawal] Cannot start withdrawal monitor: missing activeClusterId');
      node._withdrawalMonitorRunning = false;
      return;
    }
    const filter = node.bridge.filters.TokensBurned(null, activeClusterId);
    const currentBlock = await node.provider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - 300);
    try {
      const pastEvents = await node.bridge.queryFilter(filter, fromBlock, currentBlock);
      for (const event of pastEvents) await handleBurnEvent(node, event);
    } catch (e) {
      console.log('[Withdrawal] Failed to query past burn events:', e.message || String(e));
    }
    node.bridge.on(filter, async (user, xmrAddress, amount, fee, event) => { await handleBurnEvent(node, event); });
    console.log('[Withdrawal] Now listening for TokensBurned events');
    setupWithdrawalClaimListener(node);
  } catch (e) {
    console.log('[Withdrawal] Failed to setup event listener:', e.message || String(e));
    node._withdrawalMonitorRunning = false;
  }
}

function setupWithdrawalClaimListener(node) {
  if (!node.p2p || node._withdrawalClaimListenerSetup) return;
  node._withdrawalClaimListenerSetup = true;
  node._withdrawalClaims = node._withdrawalClaims || new Map();
  const pollClaims = async () => {
    if (!node._withdrawalMonitorRunning) return;
    try {
      const payloads = await node.p2p.getPeerPayloads(node._activeClusterId, node._sessionId || 'bridge', 9899, node._clusterMembers);
      for (const payload of payloads) {
        try {
          const data = JSON.parse(payload);
          if (data.type === 'withdrawal-claim' && data.txHash && data.claimer) {
            const existing = node._withdrawalClaims.get(data.txHash);
            if (!existing || existing.claimer !== data.claimer) {
              node._withdrawalClaims.set(data.txHash, { claimer: data.claimer, timestamp: Date.now() });
            }
          }
        } catch (_ignored) {}
      }
    } catch (_ignored) {}
    if (node._withdrawalMonitorRunning) setTimeout(pollClaims, 5000);
  };
  pollClaims();
}

export function stopWithdrawalMonitor(node) {
  node._withdrawalMonitorRunning = false;
  if (node.bridge) {
    try { node.bridge.removeAllListeners('TokensBurned'); }
    catch (_ignored) {}
  }
  console.log('[Withdrawal] Withdrawal monitor stopped');
}

async function handleBurnEvent(node, event) {
  try {
    const txHash = event.transactionHash;
    node._processedWithdrawals = node._processedWithdrawals || new Set();
    if (node._processedWithdrawals.has(txHash)) return;
    const activeClusterId = node._activeClusterId;
    if (!activeClusterId) {
      console.log('[Withdrawal] No activeClusterId set, skipping burn event');
      return;
    }
    const user = event.args[0] ?? event.args.user;
    const clusterIdFromEvent = event.args[1] ?? event.args.clusterId;
    const xmrAddress = event.args[2] ?? event.args.xmrAddress;
    const amount = event.args[3] ?? event.args.burnAmount ?? event.args.amount;
    const fee = event.args[4] ?? event.args.fee;
    console.log('[Withdrawal] TokensBurned event detected:');
    console.log(`  TX: ${txHash}`);
    console.log(`  User: ${user}`);
    console.log(`  Cluster ID: ${clusterIdFromEvent}`);
    console.log(`  XMR Address: ${xmrAddress}`);
    console.log(`  Amount: ${ethers.formatEther(amount)} zXMR`);
    if (typeof clusterIdFromEvent === 'string' && typeof activeClusterId === 'string' && clusterIdFromEvent.toLowerCase() !== activeClusterId.toLowerCase()) {
      console.log(`[Withdrawal] Burn event clusterId mismatch (event=${clusterIdFromEvent}, local=${activeClusterId}), skipping`);
      return;
    }
    if (!isValidMoneroAddress(xmrAddress)) {
      console.log(`[Withdrawal] Invalid Monero address, skipping: ${xmrAddress}`);
      node._processedWithdrawals.add(txHash);
      return;
    }
    const xmrAtomicAmount = BigInt(amount) / BigInt(1e6);
    const withdrawal = { txHash, user, xmrAddress, zxmrAmount: amount.toString(), xmrAmount: xmrAtomicAmount.toString(), blockNumber: event.blockNumber, timestamp: Date.now() };
    try {
      const consensusResult = await runWithdrawalConsensus(node, withdrawal);
      if (consensusResult.success) {
        console.log(`[Withdrawal] Consensus reached for withdrawal ${txHash.slice(0, 18)}...`);
        await executeWithdrawalWithCascade(node, withdrawal);
      } else {
        console.log(`[Withdrawal] Consensus failed for ${txHash.slice(0, 18)}: ${consensusResult.reason}`);
      }
    } catch (e) {
      console.log(`[Withdrawal] Consensus error for ${txHash.slice(0, 18)}:`, e.message || String(e));
    }
    node._processedWithdrawals.add(txHash);
  } catch (e) {
    console.log('[Withdrawal] Error handling burn event:', e.message || String(e));
  }
}

async function runWithdrawalConsensus(node, withdrawal) {
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const withdrawalHash = ethers.keccak256(ethers.AbiCoder.defaultAbiCoder().encode(['bytes32', 'address', 'string', 'uint256'], [withdrawal.txHash, withdrawal.user, withdrawal.xmrAddress, withdrawal.xmrAmount]));
  console.log(`[Withdrawal] Running PBFT consensus for withdrawal ${withdrawal.txHash.slice(0, 18)}...`);
  const topic = `withdrawal-${withdrawal.txHash.slice(0, 18)}`;
  const timeoutMs = Number(process.env.WITHDRAWAL_CONSENSUS_TIMEOUT_MS || 180000);
  try {
    const result = await node.p2p.runConsensus(clusterId, sessionId, topic, withdrawalHash, node._clusterMembers, timeoutMs);
    if (result && result.success) return { success: true };
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}

const WITHDRAWAL_CASCADE_INTERVAL_MS = Number(process.env.WITHDRAWAL_CASCADE_INTERVAL_MS || 60000);

function getWithdrawalTurnIndex(node) {
  if (!node._clusterMembers || node._clusterMembers.length === 0) return -1;
  const sorted = [...node._clusterMembers].map((a) => a.toLowerCase()).sort();
  return sorted.indexOf(node.wallet.address.toLowerCase());
}

async function executeWithdrawalWithCascade(node, withdrawal) {
  const myIndex = getWithdrawalTurnIndex(node);
  if (myIndex < 0) return;
  const existingClaim = node._withdrawalClaims?.get(withdrawal.txHash);
  if (existingClaim && Date.now() - existingClaim.timestamp < WITHDRAWAL_CASCADE_INTERVAL_MS * 2) {
    const claimIndex = getWithdrawalTurnIndex({ ...node, wallet: { address: existingClaim.claimer } });
    if (claimIndex >= 0 && claimIndex < myIndex) {
      console.log(`[Withdrawal] Node ${existingClaim.claimer.slice(0, 10)} claimed this withdrawal, waiting`);
      return;
    }
  }
  const myTurnDelay = myIndex * WITHDRAWAL_CASCADE_INTERVAL_MS;
  const consensusTimestamp = withdrawal.consensusTimestamp || Date.now();
  const waitUntil = consensusTimestamp + myTurnDelay;
  const waitTime = waitUntil - Date.now();
  if (waitTime > 0) {
    console.log(`[Withdrawal] My turn index: ${myIndex}, waiting ${Math.round(waitTime / 1000)}s`);
    await new Promise((resolve) => setTimeout(resolve, waitTime));
    const lateClaim = node._withdrawalClaims?.get(withdrawal.txHash);
    if (lateClaim && Date.now() - lateClaim.timestamp < WITHDRAWAL_CASCADE_INTERVAL_MS) {
      console.log(`[Withdrawal] Another node claimed during wait, aborting`);
      return;
    }
  }
  node._withdrawalClaims = node._withdrawalClaims || new Map();
  node._withdrawalClaims.set(withdrawal.txHash, { claimer: node.wallet.address, timestamp: Date.now() });
  try {
    await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', 9899, JSON.stringify({ type: 'withdrawal-claim', txHash: withdrawal.txHash, claimer: node.wallet.address }));
  } catch (_ignored) {}
  await executeWithdrawal(node, withdrawal);
}

async function executeWithdrawal(node, withdrawal) {
  console.log(`[Withdrawal] Executing withdrawal to ${withdrawal.xmrAddress}`);
  console.log(`  Amount: ${Number(withdrawal.xmrAmount) / 1e12} XMR`);
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  node._pendingWithdrawals.set(withdrawal.txHash, { ...withdrawal, status: 'pending', startedAt: Date.now() });
  try {
    console.log('[Withdrawal] Creating multisig transfer transaction...');
    const destinations = [{ address: withdrawal.xmrAddress, amount: BigInt(withdrawal.xmrAmount) }];
    let txData;
    try {
      txData = await node.monero.transfer(destinations);
      console.log('[Withdrawal] Transfer transaction created');
    } catch (e) {
      console.log('[Withdrawal] Failed to create transfer:', e.message || String(e));
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'failed';
      node._pendingWithdrawals.get(withdrawal.txHash).error = e.message;
      return;
    }
    if (!txData || !txData.txDataHex) {
      console.log('[Withdrawal] No transaction data returned from transfer');
      return;
    }
    console.log('[Withdrawal] Broadcasting unsigned tx for multisig signing...');
    await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', 9900, JSON.stringify({
      type: 'withdrawal-sign-request',
      withdrawalTxHash: withdrawal.txHash,
      xmrAddress: withdrawal.xmrAddress,
      amount: withdrawal.xmrAmount,
      txDataHex: txData.txDataHex,
    }));
    console.log('[Withdrawal] Waiting for multisig signatures...');
    const signatureTimeoutMs = Number(process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);
    const signatureRound = 9901;
    const complete = await node.p2p.waitForRoundCompletion(node._activeClusterId, node._sessionId || 'bridge', signatureRound, node._clusterMembers, signatureTimeoutMs);
    if (!complete) {
      console.log('[Withdrawal] Failed to collect enough signatures');
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'signing_failed';
      return;
    }
    console.log('[Withdrawal] Combining signatures and submitting transaction...');
    const signatures = await node.p2p.getPeerPayloads(node._activeClusterId, node._sessionId || 'bridge', signatureRound, node._clusterMembers);
    console.log(`[Withdrawal] Collected ${signatures.length} multisig signatures`);
    let signedTx;
    try { signedTx = await node.monero.signMultisig(txData.txDataHex); }
    catch (e) {
      console.log('[Withdrawal] Failed to sign multisig tx:', e.message || String(e));
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'sign_failed';
      return;
    }
    try {
      const submitResult = await node.monero.call('submit_multisig', { tx_data_hex: signedTx.txDataHex || txData.txDataHex }, 180000);
      console.log('[Withdrawal] Transaction submitted successfully');
      console.log(`  TX Hash(es): ${submitResult.tx_hash_list ? submitResult.tx_hash_list.join(', ') : 'unknown'}`);
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'completed';
      node._pendingWithdrawals.get(withdrawal.txHash).xmrTxHashes = submitResult.tx_hash_list;
    } catch (e) {
      console.log('[Withdrawal] Failed to submit transaction:', e.message || String(e));
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'submit_failed';
      node._pendingWithdrawals.get(withdrawal.txHash).error = e.message;
    }
  } catch (e) {
    console.log('[Withdrawal] Withdrawal execution error:', e.message || String(e));
    if (node._pendingWithdrawals && node._pendingWithdrawals.has(withdrawal.txHash)) {
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'error';
      node._pendingWithdrawals.get(withdrawal.txHash).error = e.message;
    }
  }
}

export async function handleWithdrawalSignRequest(node, data) {
  if (!data || !data.txDataHex || !data.withdrawalTxHash) {
    console.log('[Withdrawal] Invalid sign request data');
    return;
  }
  console.log(`[Withdrawal] Received sign request for ${data.withdrawalTxHash.slice(0, 18)}...`);
  try {
    const signedTx = await node.monero.signMultisig(data.txDataHex);
    const signatureRound = 9901;
    await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', signatureRound, JSON.stringify({
      type: 'withdrawal-signature',
      withdrawalTxHash: data.withdrawalTxHash,
      signedTxHex: signedTx.txDataHex,
    }));
    console.log(`[Withdrawal] Broadcasted signature for ${data.withdrawalTxHash.slice(0, 18)}`);
  } catch (e) {
    console.log('[Withdrawal] Failed to sign withdrawal tx:', e.message || String(e));
  }
}

export function getWithdrawalStatus(node, ethTxHash) {
  if (!node._pendingWithdrawals) return null;
  return node._pendingWithdrawals.get(ethTxHash) || null;
}

export function getAllPendingWithdrawals(node) {
  if (!node._pendingWithdrawals) return [];
  const result = [];
  for (const [txHash, data] of node._pendingWithdrawals) {
    result.push({ ethTxHash: txHash, ...data });
  }
  return result;
}
