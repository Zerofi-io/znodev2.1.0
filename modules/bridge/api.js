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

export function startBridgeAPI(node) {
  const apiEnabled = process.env.BRIDGE_API_ENABLED === '1';
  if (!apiEnabled) {
    console.log('[BridgeAPI] API server disabled (BRIDGE_API_ENABLED != 1)');
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

    if (node._apiRateLimits.size > 1000) {
      for (const [ip, info] of node._apiRateLimits) {
        if (now - info.windowStart > rateLimitWindowMs) {
          node._apiRateLimits.delete(ip);
        }
      }
    }

    if (rateInfo.count > rateLimitMaxRequests) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Too many requests',
          retryAfter: Math.ceil((rateInfo.windowStart + rateLimitWindowMs - now) / 1000),
        }),
      );
      return;
    }

    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;

    handleAPIRequest(node, req, res, pathname, url.searchParams);
  });

  node._apiServer.listen(port, () => {
    console.log(`[BridgeAPI] HTTP API server listening on port ${port}`);
  });

  node._apiServer.on('error', (e) => {
    console.log('[BridgeAPI] Server error:', e.message || String(e));
  });
}

export function stopBridgeAPI(node) {
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
        connectedPeers:
          node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0,
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

      return jsonResponse(res, 200, {
        status,
        monero: {
          health: moneroHealth,
          errors: node.moneroErrorCounts || {},
        },
        rpcCircuitBreaker: circuit,
        p2p: p2pStatus,
        cluster,
      });
    }

    if (pathname === '/metrics' && req.method === 'GET') {
      const clusterMetrics = metrics.toJSON();
      const loggerMetrics = consoleLogger ? consoleLogger.getMetrics() : null;
      return jsonResponse(res, 200, { metrics: clusterMetrics, logger: loggerMetrics });
    }

    if (pathname === '/health' && req.method === 'GET') {
      return jsonResponse(res, 200, {
        status: 'ok',
        clusterFinalized: node._clusterFinalized,
        multisigAddress: node._clusterFinalAddress || null,
        bridgeEnabled: process.env.BRIDGE_ENABLED === '1',
      });
    }

    if (pathname === '/bridge/info' && req.method === 'GET') {
      return jsonResponse(res, 200, {
        multisigAddress: node._clusterFinalAddress || null,
        clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0,
        clusterFinalized: node._clusterFinalized,
        bridgeContract: node.bridge ? node.bridge.target || node.bridge.address : null,
        minConfirmations: Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 10),
      });
    }

    if (pathname === '/bridge/deposit/request' && req.method === 'POST') {
      const body = await readRequestBody(req);
      const { ethAddress } = body;

      if (!ethAddress || !ethers.isAddress(ethAddress)) {
        return jsonResponse(res, 400, { error: 'Invalid ethAddress' });
      }

      if (!node._clusterFinalAddress) {
        return jsonResponse(res, 503, { error: 'Cluster not ready' });
      }

      const result = registerDepositRequest(node, ethAddress);
      return jsonResponse(res, 200, result);
    }

    if (pathname.startsWith('/bridge/deposit/status/') && req.method === 'GET') {
      const txid = pathname.split('/').pop();

      if (!txid || txid.length < 10) {
        return jsonResponse(res, 400, { error: 'Invalid txid' });
      }

      const signature = getMintSignature(node, txid);

      if (signature) {
        return jsonResponse(res, 200, {
          status: 'ready',
          ...signature,
        });
      }

      if (node._processedDeposits && node._processedDeposits.has(txid)) {
        return jsonResponse(res, 200, {
          status: 'processed',
          message: 'Deposit processed but no signature available (may have failed consensus)',
        });
      }

      return jsonResponse(res, 200, {
        status: 'pending',
        message: 'Deposit not yet detected or confirmed',
      });
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

      return jsonResponse(res, 200, {
        status: 'not_found',
        message: 'Withdrawal not yet detected or processed',
      });
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

function jsonResponse(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        resolve({});
      }
    });
    req.on('error', reject);
  });
}

function getBridgeStatePath() {
  const homeDir = process.env.HOME || process.cwd();
  const dataDir = process.env.BRIDGE_DATA_DIR || path.join(homeDir, '.znode-bridge');
  try {
    fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 });
  } catch {}
  return path.join(dataDir, 'bridge_state.json');
}

function loadBridgeState(node) {
  try {
    const statePath = getBridgeStatePath();
    if (!fs.existsSync(statePath)) {
      return;
    }

    const data = JSON.parse(fs.readFileSync(statePath, 'utf8'));

    if (Array.isArray(data.processedDeposits)) {
      node._processedDeposits = new Set(data.processedDeposits);
      console.log(`[Bridge] Loaded ${node._processedDeposits.size} processed deposits from disk`);
    }

    if (data.pendingMintSignatures && typeof data.pendingMintSignatures === 'object') {
      node._pendingMintSignatures = new Map(Object.entries(data.pendingMintSignatures));
      console.log(
        `[Bridge] Loaded ${node._pendingMintSignatures.size} pending mint signatures from disk`,
      );
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
      pendingMintSignatures: node._pendingMintSignatures
        ? Object.fromEntries(node._pendingMintSignatures)
        : {},
      processedWithdrawals: node._processedWithdrawals
        ? Array.from(node._processedWithdrawals)
        : [],
    };

    fs.writeFileSync(statePath, JSON.stringify(data, null, 2), {
      mode: 0o600,
    });
  } catch (e) {
    console.log('[Bridge] Failed to save bridge state:', e.message || String(e));
  }
}

function startBridgeStateSaver(node) {
  const saveIntervalMs = Number(process.env.BRIDGE_STATE_SAVE_INTERVAL_MS || 60000);

  if (node._bridgeStateSaverTimer) {
    clearInterval(node._bridgeStateSaverTimer);
  }

  node._bridgeStateSaverTimer = setInterval(() => {
    saveBridgeState(node);
  }, saveIntervalMs);
}

function cleanupExpiredSignatures(node) {
  if (!node._pendingMintSignatures || node._pendingMintSignatures.size === 0) {
    return;
  }

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
  if (!node._processedDeposits || node._processedDeposits.size === 0) {
    return;
  }

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

  if (node._bridgeCleanupTimer) {
    clearInterval(node._bridgeCleanupTimer);
  }

  node._bridgeCleanupTimer = setInterval(() => {
    cleanupExpiredSignatures(node);
    cleanupOldProcessedDeposits(node);
  }, cleanupIntervalMs);
}

export async function startDepositMonitor(node) {
  if (node._depositMonitorRunning) {
    return;
  }

  const bridgeEnabled = process.env.BRIDGE_ENABLED === '1';
  if (!bridgeEnabled) {
    console.log('[Bridge] Deposit monitoring disabled (BRIDGE_ENABLED != 1)');
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

  console.log(
    `[Bridge] Starting deposit monitor (poll: ${pollIntervalMs / 1000}s, minConf: ${minConfirmations})`,
  );

  const poll = async () => {
    if (!node._depositMonitorRunning || !node._clusterFinalized) {
      return;
    }

    try {
      await checkForDeposits(node, minConfirmations);
    } catch (e) {
      console.log('[Bridge] Deposit check error:', e.message || String(e));
    }
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

async function checkForDeposits(node, minConfirmations) {
  if (!node.monero) {
    return;
  }

  try {
    await node.monero.refresh();
  } catch {}

  let deposits;
  try {
    deposits = await node.monero.getConfirmedDeposits(minConfirmations);
  } catch (e) {
    console.log('[Bridge] Failed to get deposits:', e.message || String(e));
    return;
  }

  if (!deposits || deposits.length === 0) {
    return;
  }

  for (const deposit of deposits) {
    const txid = deposit.txid;

    if (node._processedDeposits && node._processedDeposits.has(txid)) {
      continue;
    }

    if (node._pendingMintSignatures && node._pendingMintSignatures.has(txid)) {
      continue;
    }

    console.log(`[Bridge] New deposit detected: ${txid}`);
    console.log(`  Amount: ${deposit.amount / 1e12} XMR`);
    console.log(`  Confirmations: ${deposit.confirmations}`);
    console.log(`  Payment ID: ${deposit.paymentId || 'none'}`);

    const recipient = parseRecipientFromPaymentId(node, deposit.paymentId);

    if (!recipient) {
      console.log(`[Bridge] Skipping deposit ${txid}: no valid recipient in payment ID`);
      if (!node._processedDeposits) {
        node._processedDeposits = new Set();
      }
      node._processedDeposits.add(txid);
      continue;
    }

    try {
      const consensusResult = await runDepositConsensus(node, deposit, recipient);

      if (consensusResult.success) {
        console.log(`[Bridge] Consensus reached for deposit ${txid}`);

        if (isClusterCoordinator(node)) {
          await generateMintSignature(node, deposit, recipient);
        }
      } else {
        console.log(`[Bridge] Consensus failed for deposit ${txid}: ${consensusResult.reason}`);
      }
    } catch (e) {
      console.log(`[Bridge] Consensus error for deposit ${txid}:`, e.message || String(e));
    }

    if (!node._processedDeposits) {
      node._processedDeposits = new Set();
    }
    node._processedDeposits.add(txid);

    saveBridgeState(node);
  }
}

function parseRecipientFromPaymentId(node, paymentId) {
  if (!paymentId || paymentId.length === 0) {
    return null;
  }

  let candidate = paymentId;
  if (candidate.length === 40 && /^[0-9a-fA-F]{40}$/.test(candidate)) {
    candidate = '0x' + candidate;
  }

  if (candidate.length === 42 && candidate.startsWith('0x')) {
    try {
      return ethers.getAddress(candidate);
    } catch {}
  }

  if (node._pendingDepositRequests && node._pendingDepositRequests.has(paymentId)) {
    return node._pendingDepositRequests.get(paymentId);
  }

  return null;
}

async function runDepositConsensus(node, deposit, recipient) {
  if (!node.p2p || !node._clusterMembers || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }

  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';

  const depositHash = ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ['string', 'uint256', 'uint256', 'address'],
      [deposit.txid, deposit.amount, deposit.blockHeight, recipient],
    ),
  );

  console.log(`[Bridge] Running PBFT consensus for deposit ${deposit.txid.slice(0, 16)}...`);
  console.log(`  Hash: ${depositHash.slice(0, 18)}...`);

  const topic = `deposit-${deposit.txid.slice(0, 16)}`;
  const timeoutMs = Number(process.env.DEPOSIT_CONSENSUS_TIMEOUT_MS || 120000);

  try {
    const result = await node.p2p.runConsensus(
      clusterId,
      sessionId,
      topic,
      depositHash,
      node._clusterMembers,
      timeoutMs,
    );

    if (result && result.success) {
      return { success: true };
    }
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}

function isClusterCoordinator(node) {
  if (!node._clusterMembers || node._clusterMembers.length === 0) {
    return false;
  }

  const sorted = [...node._clusterMembers].map((a) => a.toLowerCase()).sort();

  return sorted[0] === node.wallet.address.toLowerCase();
}

async function generateMintSignature(node, deposit, recipient) {
  if (!node.wallet) {
    console.log('[Bridge] Cannot generate signature: no wallet');
    return null;
  }

  const depositId = ethers.keccak256(ethers.toUtf8Bytes(deposit.txid));

  const amountWei = BigInt(deposit.amount) * BigInt(1e6);

  const messageHash = ethers.keccak256(
    ethers.solidityPacked(['address', 'bytes32', 'uint256'], [recipient, depositId, amountWei]),
  );

  const signature = await node.wallet.signMessage(ethers.getBytes(messageHash));

  console.log(`[Bridge] Generated mint signature for ${recipient}`);
  console.log(`  Deposit ID: ${depositId}`);
  console.log(`  Amount: ${ethers.formatEther(amountWei)} zXMR`);

  if (!node._pendingMintSignatures) {
    node._pendingMintSignatures = new Map();
  }

  node._pendingMintSignatures.set(deposit.txid, {
    depositId,
    amount: amountWei.toString(),
    recipient,
    signature,
    xmrTxid: deposit.txid,
    timestamp: Date.now(),
  });

  return { depositId, amount: amountWei.toString(), signature };
}

export function getMintSignature(node, xmrTxid) {
  if (!node._pendingMintSignatures) {
    return null;
  }
  return node._pendingMintSignatures.get(xmrTxid) || null;
}

export function getAllPendingSignatures(node) {
  if (!node._pendingMintSignatures) {
    return [];
  }
  const result = [];
  for (const [txid, data] of node._pendingMintSignatures) {
    result.push({ xmrTxid: txid, ...data });
  }
  return result;
}

export function registerDepositRequest(node, ethAddress, paymentId) {
  if (!node._pendingDepositRequests) {
    node._pendingDepositRequests = new Map();
  }

  let id = paymentId;
  if (!id) {
    id = crypto.randomBytes(8).toString('hex');
  }

  node._pendingDepositRequests.set(id, ethAddress);

  return {
    paymentId: id,
    multisigAddress: node._clusterFinalAddress,
    ethAddress,
  };
}

export async function startWithdrawalMonitor(node) {
  if (node._withdrawalMonitorRunning) {
    return;
  }

  const bridgeEnabled = process.env.BRIDGE_ENABLED === '1';
  if (!bridgeEnabled) {
    console.log('[Withdrawal] Withdrawal monitoring disabled (BRIDGE_ENABLED != 1)');
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
    const filter = node.bridge.filters.TokensBurned();

    const currentBlock = await node.provider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - 300);

    try {
      const pastEvents = await node.bridge.queryFilter(filter, fromBlock, currentBlock);
      for (const event of pastEvents) {
        await handleBurnEvent(node, event);
      }
    } catch (e) {
      console.log('[Withdrawal] Failed to query past burn events:', e.message || String(e));
    }

    node.bridge.on(filter, async (user, xmrAddress, amount, fee, event) => {
      await handleBurnEvent(node, event);
    });

    console.log('[Withdrawal] Now listening for TokensBurned events');
  } catch (e) {
    console.log('[Withdrawal] Failed to setup event listener:', e.message || String(e));
    node._withdrawalMonitorRunning = false;
  }
}

export function stopWithdrawalMonitor(node) {
  node._withdrawalMonitorRunning = false;
  if (node.bridge) {
    try {
      node.bridge.removeAllListeners('TokensBurned');
    } catch {}
  }
  console.log('[Withdrawal] Withdrawal monitor stopped');
}

async function handleBurnEvent(node, event) {
  try {
    const txHash = event.transactionHash;

    node._processedWithdrawals = node._processedWithdrawals || new Set();
    if (node._processedWithdrawals.has(txHash)) {
      return;
    }

    const user = event.args[0] || event.args.user;
    const xmrAddress = event.args[1] || event.args.xmrAddress;
    const amount = event.args[2] || event.args.amount;
    const fee = event.args[3] || event.args.fee;

    console.log('[Withdrawal] TokensBurned event detected:');
    console.log(`  TX: ${txHash}`);
    console.log(`  User: ${user}`);
    console.log(`  XMR Address: ${xmrAddress}`);
    console.log(`  Amount: ${ethers.formatEther(amount)} zXMR`);
    console.log(`  Fee: ${ethers.formatEther(fee)} zXMR`);

    if (!isValidMoneroAddress(xmrAddress)) {
      console.log(`[Withdrawal] Invalid Monero address, skipping: ${xmrAddress}`);
      node._processedWithdrawals.add(txHash);
      return;
    }

    const xmrAtomicAmount = BigInt(amount) / BigInt(1e6);

    const withdrawal = {
      txHash,
      user,
      xmrAddress,
      zxmrAmount: amount.toString(),
      xmrAmount: xmrAtomicAmount.toString(),
      blockNumber: event.blockNumber,
      timestamp: Date.now(),
    };

    try {
      const consensusResult = await runWithdrawalConsensus(node, withdrawal);

      if (consensusResult.success) {
        console.log(`[Withdrawal] Consensus reached for withdrawal ${txHash.slice(0, 18)}...`);

        if (isClusterCoordinator(node)) {
          await executeWithdrawal(node, withdrawal);
        }
      } else {
        console.log(
          `[Withdrawal] Consensus failed for ${txHash.slice(0, 18)}: ${consensusResult.reason}`,
        );
      }
    } catch (e) {
      console.log(
        `[Withdrawal] Consensus error for ${txHash.slice(0, 18)}:`,
        e.message || String(e),
      );
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

  const withdrawalHash = ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ['bytes32', 'address', 'string', 'uint256'],
      [withdrawal.txHash, withdrawal.user, withdrawal.xmrAddress, withdrawal.xmrAmount],
    ),
  );

  console.log(
    `[Withdrawal] Running PBFT consensus for withdrawal ${withdrawal.txHash.slice(0, 18)}...`,
  );
  console.log(`  Hash: ${withdrawalHash.slice(0, 18)}...`);

  const topic = `withdrawal-${withdrawal.txHash.slice(0, 18)}`;
  const timeoutMs = Number(process.env.WITHDRAWAL_CONSENSUS_TIMEOUT_MS || 180000);

  try {
    const result = await node.p2p.runConsensus(
      clusterId,
      sessionId,
      topic,
      withdrawalHash,
      node._clusterMembers,
      timeoutMs,
    );

    if (result && result.success) {
      return { success: true };
    }
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}

async function executeWithdrawal(node, withdrawal) {
  console.log(`[Withdrawal] Executing withdrawal to ${withdrawal.xmrAddress}`);
  console.log(`  Amount: ${Number(withdrawal.xmrAmount) / 1e12} XMR`);

  node._pendingWithdrawals = node._pendingWithdrawals || new Map();

  node._pendingWithdrawals.set(withdrawal.txHash, {
    ...withdrawal,
    status: 'pending',
    startedAt: Date.now(),
  });

  try {
    console.log('[Withdrawal] Creating multisig transfer transaction...');

    const destinations = [
      {
        address: withdrawal.xmrAddress,
        amount: BigInt(withdrawal.xmrAmount),
      },
    ];

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

    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      node._sessionId || 'bridge',
      9900,
      JSON.stringify({
        type: 'withdrawal-sign-request',
        withdrawalTxHash: withdrawal.txHash,
        xmrAddress: withdrawal.xmrAddress,
        amount: withdrawal.xmrAmount,
        txDataHex: txData.txDataHex,
      }),
    );

    console.log('[Withdrawal] Waiting for multisig signatures...');

    const signatureTimeoutMs = Number(process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);

    const signatureRound = 9901;
    const complete = await node.p2p.waitForRoundCompletion(
      node._activeClusterId,
      node._sessionId || 'bridge',
      signatureRound,
      node._clusterMembers,
      signatureTimeoutMs,
    );

    if (!complete) {
      console.log('[Withdrawal] Failed to collect enough signatures');
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'signing_failed';
      return;
    }

    console.log('[Withdrawal] Combining signatures and submitting transaction...');

    const signatures = await node.p2p.getPeerPayloads(
      node._activeClusterId,
      node._sessionId || 'bridge',
      signatureRound,
      node._clusterMembers,
    );
    console.log(`[Withdrawal] Collected ${signatures.length} multisig signatures`);

    let signedTx;
    try {
      signedTx = await node.monero.signMultisig(txData.txDataHex);
    } catch (e) {
      console.log('[Withdrawal] Failed to sign multisig tx:', e.message || String(e));
      node._pendingWithdrawals.get(withdrawal.txHash).status = 'sign_failed';
      return;
    }

    try {
      const submitResult = await node.monero.call(
        'submit_multisig',
        {
          tx_data_hex: signedTx.txDataHex || txData.txDataHex,
        },
        180000,
      );

      console.log('[Withdrawal] Transaction submitted successfully');
      console.log(
        `  TX Hash(es): ${submitResult.tx_hash_list ? submitResult.tx_hash_list.join(', ') : 'unknown'}`,
      );

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
    await node.p2p.broadcastRoundData(
      node._activeClusterId,
      node._sessionId || 'bridge',
      signatureRound,
      JSON.stringify({
        type: 'withdrawal-signature',
        withdrawalTxHash: data.withdrawalTxHash,
        signedTxHex: signedTx.txDataHex,
      }),
    );

    console.log(`[Withdrawal] Broadcasted signature for ${data.withdrawalTxHash.slice(0, 18)}`);
  } catch (e) {
    console.log('[Withdrawal] Failed to sign withdrawal tx:', e.message || String(e));
  }
}

export function getWithdrawalStatus(node, ethTxHash) {
  if (!node._pendingWithdrawals) {
    return null;
  }
  return node._pendingWithdrawals.get(ethTxHash) || null;
}

export function getAllPendingWithdrawals(node) {
  if (!node._pendingWithdrawals) {
    return [];
  }
  const result = [];
  for (const [txHash, data] of node._pendingWithdrawals) {
    result.push({ ethTxHash: txHash, ...data });
  }
  return result;
}

