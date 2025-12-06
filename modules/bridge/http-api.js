import http from 'http';
import { ethers } from 'ethers';
import { metrics } from '../core/metrics.js';
import consoleLogger from '../core/console-shim.js';
import { saveBridgeState } from './bridge-state.js';
import { startDepositMonitor, stopDepositMonitor, registerDepositRequest, getMintSignature, getAllPendingSignatures } from './deposits.js';
import { startWithdrawalMonitor, stopWithdrawalMonitor, handleWithdrawalSignRequest, getWithdrawalStatus, getAllPendingWithdrawals } from './withdrawals.js';

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
        if (info.windowStart < cutoff) node._apiRateLimits.delete(ip);
      }
    }
    if (rateInfo.count > rateLimitMaxRequests) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Too many requests', retryAfter: Math.ceil((rateInfo.windowStart + rateLimitWindowMs - now) / 1000) }));
      return;
    }
    const url = new URL(req.url, `http://${req.headers.host}`);
    const pathname = url.pathname;
    handleAPIRequest(node, req, res, pathname, url.searchParams, rateLimitWindowMs);
  });
  node._apiServer.listen(port, () => {
    console.log(`[BridgeAPI] HTTP API server listening on port ${port}`);
  });
  if (!node._apiRateLimitCleanupTimer) {
    node._apiRateLimitCleanupTimer = setInterval(() => {
      if (!node._apiRateLimits) return;
      const cutoff = Date.now() - rateLimitWindowMs;
      for (const [ip, info] of node._apiRateLimits) {
        if (info.windowStart < cutoff) node._apiRateLimits.delete(ip);
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

function jsonResponse(res, status, data) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function isBridgeReadyForDeposit(node) {
  const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
  if (moneroHealth !== MoneroHealth.HEALTHY) {
    return { ok: false, reason: 'Monero backend not healthy' };
  }
  if (!node._clusterFinalized || !node._clusterFinalAddress) {
    return { ok: false, reason: 'Cluster not finalized' };
  }
  if (!node.p2p || !node.p2p.node) {
    return { ok: false, reason: 'P2P not connected' };
  }
  const members = Array.isArray(node._clusterMembers) ? node._clusterMembers : [];
  const self = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const inCluster = self && members.some((m) => m && m.toLowerCase() === self);
  if (!inCluster) {
    return { ok: false, reason: 'Node not member of active cluster' };
  }
  return { ok: true };
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

async function handleAPIRequest(node, req, res, pathname, params, rateLimitWindowMs) {
  try {
    if (pathname === '/healthz' && req.method === 'GET') {
      const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
      const circuit = { open: !!node._rpcCircuitOpen, halfOpen: !!node._rpcCircuitHalfOpen, openUntil: node._rpcCircuitOpenUntil || 0 };
      const p2pStatus = { connected: !!(node.p2p && node.p2p.node), connectedPeers: node.p2p && typeof node.p2p._connectedPeers === 'number' ? node.p2p._connectedPeers : 0 };
      const cluster = { activeClusterId: node._activeClusterId || null, clusterFinalized: !!node._clusterFinalized, clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0 };
      let status = 'ok';
      if (circuit.open || moneroHealth === MoneroHealth.QUARANTINED) status = 'error';
      else if (moneroHealth !== MoneroHealth.HEALTHY || !p2pStatus.connected) status = 'degraded';
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
      return jsonResponse(res, 200, { status: 'ok', clusterFinalized: node._clusterFinalized, multisigAddress: node._clusterFinalAddress || null, bridgeEnabled: isBridgeEnabled() });
    }
    if (pathname === '/bridge/info' && req.method === 'GET') {
      return jsonResponse(res, 200, { multisigAddress: node._clusterFinalAddress || null, clusterMembers: node._clusterMembers ? node._clusterMembers.length : 0, clusterFinalized: node._clusterFinalized, bridgeContract: node.bridge ? node.bridge.target || node.bridge.address : null, clusterId: node._activeClusterId || null, minConfirmations: Number(process.env.MIN_DEPOSIT_CONFIRMATIONS || 10) });
    }
    if (pathname === '/bridge/reserves' && req.method === 'GET') {
      const moneroHealth = node.moneroHealth || MoneroHealth.HEALTHY;
      if (!node.monero || moneroHealth === MoneroHealth.QUARANTINED) {
        return jsonResponse(res, 503, { error: 'Monero backend not healthy', moneroHealth });
      }
      const clusterId = node._activeClusterId || null;
      const multisigAddress = node._clusterFinalAddress || null;
      let walletAddress = null;
      try {
        walletAddress = await node.monero.getAddress();
      } catch (_ignored) {}
      try {
        try { await node.monero.refresh(); } catch (_ignored) {}
        const bal = await node.monero.getBalance();
        const balanceRaw = bal && bal.balance != null ? bal.balance : 0;
        const unlockedRaw = bal && bal.unlockedBalance != null ? bal.unlockedBalance : 0;
        const balanceAtomic = typeof balanceRaw === 'bigint' ? balanceRaw : BigInt(String(balanceRaw));
        const unlockedAtomic = typeof unlockedRaw === 'bigint' ? unlockedRaw : BigInt(String(unlockedRaw));
        const denom = 1000000000000n;
        const toDecimal = (v) => {
          const sign = v < 0n ? '-' : '';
          const abs = v < 0n ? -v : v;
          const intPart = abs / denom;
          const fracPart = abs % denom;
          if (fracPart === 0n) return sign + intPart.toString() + '.0';
          let fracStr = fracPart.toString().padStart(12, '0');
          fracStr = fracStr.replace(/0+$/, '');
          return sign + intPart.toString() + '.' + fracStr;
        };
        const addressVerified = !!(walletAddress && multisigAddress && walletAddress === multisigAddress);
        return jsonResponse(res, 200, {
          clusterId,
          multisigAddress,
          walletAddress,
          addressVerified,
          balanceAtomic: balanceAtomic.toString(),
          unlockedBalanceAtomic: unlockedAtomic.toString(),
          balance: toDecimal(balanceAtomic),
          unlockedBalance: toDecimal(unlockedAtomic),
          checkedAt: Date.now(),
        });
      } catch (e) {
        return jsonResponse(res, 503, { error: 'Failed to query Monero balance', message: e.message || String(e) });
      }
    }
    if (pathname === '/bridge/cluster/members' && req.method === 'GET') {
      const members = node._clusterMembers || [];
      return jsonResponse(res, 200, { members });
    }
    if (pathname === '/bridge/deposit/request' && req.method === 'POST') {
      const body = await readRequestBody(req);
      const { ethAddress, paymentId } = body;
      let normalizedAddress;
      try {
        normalizedAddress = ethAddress ? ethers.getAddress(ethAddress.toLowerCase()) : null;
      } catch (e) {
        normalizedAddress = null;
      }
      if (!normalizedAddress) {
        return jsonResponse(res, 400, { error: 'Invalid ethAddress' });
      }
      const ready = isBridgeReadyForDeposit(node);
      if (!ready.ok) {
        return jsonResponse(res, 503, { error: 'Bridge not ready', reason: ready.reason });
      }
      try {
        const result = await registerDepositRequest(node, normalizedAddress, paymentId);
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
      let usedOnChain = false;
      let depositId = null;
      try {
        depositId = ethers.keccak256(ethers.toUtf8Bytes(txid));
      } catch (_ignored) {}
      if (node.bridge && typeof node.bridge.usedDepositIds === 'function' && depositId) {
        try {
          usedOnChain = await node.bridge.usedDepositIds(depositId);
        } catch (e) {
          console.log('[BridgeAPI] Failed to check usedDepositIds for deposit status:', e.message || String(e));
        }
      }
      const signature = getMintSignature(node, txid);
      if (usedOnChain) {
        if (node._pendingMintSignatures) node._pendingMintSignatures.delete(txid);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        node._processedDeposits.add(txid);
        try { saveBridgeState(node); } catch (_ignored) {}
        return jsonResponse(res, 200, { status: 'processed', message: 'Deposit already minted on-chain', depositId });
      }
      if (signature) return jsonResponse(res, 200, { status: 'ready', ...signature });
      if (node._processedDeposits && node._processedDeposits.has(txid)) {
        return jsonResponse(res, 200, { status: 'processed', message: 'Deposit processed but no signature available (may have failed consensus)' });
      }
      return jsonResponse(res, 200, { status: 'pending', message: 'Deposit not yet detected or confirmed' });
    }
    if (pathname === '/bridge/signatures' && req.method === 'GET') {
      let signatures = getAllPendingSignatures(node);
      if (node.bridge && typeof node.bridge.usedDepositIds === 'function') {
        const filtered = [];
        let stateChanged = false;
        for (const sig of signatures) {
          let used = false;
          try {
            let depId = sig.depositId;
            if (!depId && sig.xmrTxid) {
              depId = ethers.keccak256(ethers.toUtf8Bytes(sig.xmrTxid));
            }
            if (depId) {
              used = await node.bridge.usedDepositIds(depId);
            }
          } catch (e) {
            console.log('[BridgeAPI] Failed to check usedDepositIds for signature:', e.message || String(e));
          }
          if (used) {
            if (node._pendingMintSignatures) node._pendingMintSignatures.delete(sig.xmrTxid);
            if (!node._processedDeposits) node._processedDeposits = new Set();
            node._processedDeposits.add(sig.xmrTxid);
            stateChanged = true;
          } else {
            filtered.push(sig);
          }
        }
        if (stateChanged) {
          try { saveBridgeState(node); } catch (_ignored) {}
        }
        signatures = filtered;
      }
      return jsonResponse(res, 200, { signatures });
    }
    if (pathname === '/bridge/txs' && req.method === 'GET') {
      if (!node.bridge || !node.provider) {
        return jsonResponse(res, 503, { error: 'Bridge contract or provider not configured' });
      }
      const limitParam = params && typeof params.get === 'function' ? params.get('limit') : null;
      const envLimitRaw = process.env.BRIDGE_TX_FEED_LIMIT;
      const limitRaw = limitParam != null && limitParam !== '' ? limitParam : envLimitRaw;
      const limitParsed = limitRaw != null ? Number(limitRaw) : NaN;
      const limit = Number.isFinite(limitParsed) && limitParsed > 0 ? Math.floor(limitParsed) : 10;
      const blocksParam = params && typeof params.get === 'function' ? params.get('blocks') : null;
      const envLookbackRaw = process.env.BRIDGE_TX_FEED_BLOCKS;
      const lookbackRaw = blocksParam != null && blocksParam !== '' ? blocksParam : envLookbackRaw;
      const lookbackParsed = lookbackRaw != null ? Number(lookbackRaw) : NaN;
      const lookback = Number.isFinite(lookbackParsed) && lookbackParsed > 0 ? lookbackParsed : 20000;
      try {
        const currentBlock = await node.provider.getBlockNumber();
        const fromBlock = Math.max(0, currentBlock - lookback);
        const clusterIdFilter = node._activeClusterId || null;
        const txs = [];
        try {
          const mintFilter = clusterIdFilter
            ? node.bridge.filters.TokensMinted(null, clusterIdFilter)
            : node.bridge.filters.TokensMinted();
          const events = await node.bridge.queryFilter(mintFilter, fromBlock, currentBlock);
          for (const ev of events) {
            const args = ev.args || [];
            const user = args[0] ?? args.user ?? null;
            const clusterId = args[1] ?? args.clusterId ?? null;
            const userAmount = args[3] ?? args.userAmount ?? args.amount ?? null;
            const fee = args[4] ?? args.fee ?? null;
            txs.push({
              type: 'MINT',
              direction: 'XMR->zXMR',
              txHash: ev.transactionHash,
              blockNumber: ev.blockNumber,
              logIndex: ev.logIndex,
              user,
              clusterId,
              amount: userAmount ? userAmount.toString() : '0',
              fee: fee ? fee.toString() : '0',
            });
          }
        } catch (e) {
          console.log('[BridgeAPI] Failed to query TokensMinted events:', e.message || String(e));
        }
        try {
          const burnFilter = clusterIdFilter
            ? node.bridge.filters.TokensBurned(null, clusterIdFilter)
            : node.bridge.filters.TokensBurned();
          const events = await node.bridge.queryFilter(burnFilter, fromBlock, currentBlock);
          for (const ev of events) {
            const args = ev.args || [];
            const user = args[0] ?? args.user ?? null;
            const clusterId = args[1] ?? args.clusterId ?? null;
            const xmrAddress = args[2] ?? args.xmrAddress ?? null;
            const burnAmount = args[3] ?? args.burnAmount ?? args.amount ?? null;
            const fee = args[4] ?? args.fee ?? null;
            txs.push({
              type: 'BURN',
              direction: 'zXMR->XMR',
              txHash: ev.transactionHash,
              blockNumber: ev.blockNumber,
              logIndex: ev.logIndex,
              user,
              clusterId,
              xmrAddress,
              amount: burnAmount ? burnAmount.toString() : '0',
              fee: fee ? fee.toString() : '0',
            });
          }
        } catch (e) {
          console.log('[BridgeAPI] Failed to query TokensBurned events:', e.message || String(e));
        }
        const uniqueBlocks = Array.from(
          new Set(
            txs
              .map((t) => (typeof t.blockNumber === 'number' ? t.blockNumber : null))
              .filter((n) => n != null),
          ),
        );
        const blockTimestamps = {};
        for (const bn of uniqueBlocks) {
          try {
            const block = await node.provider.getBlock(bn);
            if (block && typeof block.timestamp === 'number') {
              blockTimestamps[bn] = block.timestamp;
            }
          } catch (e) {
            console.log('[BridgeAPI] Failed to fetch block for tx feed:', e.message || String(e));
          }
        }
        for (const tx of txs) {
          if (tx.blockNumber != null && Object.prototype.hasOwnProperty.call(blockTimestamps, tx.blockNumber)) {
            tx.timestamp = blockTimestamps[tx.blockNumber];
          }
        }
        txs.sort((a, b) => {
          if (a.blockNumber !== b.blockNumber) return b.blockNumber - a.blockNumber;
          if (a.logIndex != null && b.logIndex != null) return b.logIndex - a.logIndex;
          return 0;
        });
        return jsonResponse(res, 200, { transactions: txs.slice(0, limit) });
      } catch (e) {
        console.log('[BridgeAPI] Failed to build tx feed:', e.message || String(e));
        return jsonResponse(res, 503, { error: 'Failed to load bridge transactions' });
      }
    }
    if (pathname.startsWith('/bridge/withdrawal/status/') && req.method === 'GET') {
      const txHash = pathname.split('/').pop();
      if (!txHash || txHash.length < 10) {
        return jsonResponse(res, 400, { error: 'Invalid txHash' });
      }
      const status = getWithdrawalStatus(node, txHash);
      if (status) return jsonResponse(res, 200, { status: 'found', ...status });
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

  // Simple in-memory cache for on-chain cluster membership per address to avoid
  // hammering the registry on every /cluster-status call.
  const cacheTtlMsRaw = process.env.MEMBER_CLUSTER_CACHE_TTL_MS;
  const cacheTtlMsParsed = cacheTtlMsRaw != null ? Number(cacheTtlMsRaw) : NaN;
  const cacheTtlMs = Number.isFinite(cacheTtlMsParsed) && cacheTtlMsParsed > 0 ? cacheTtlMsParsed : 300000; // 5 minutes
  if (!node._memberClusterCache) {
    node._memberClusterCache = new Map();
  }

  async function getOnChainClusterId(addr) {
    if (!clusterId) return null;
    const key = String(addr || '').toLowerCase();
    const isSelf = !!(selfAddr && key === selfAddr);
    // For non-self members, trust the local clusterId without hitting the registry.
    if (!isSelf) {
      return String(clusterId);
    }
    if (!node.registry || !node.registry.nodeToCluster) return null;
    try {
      const nowMs = Date.now();
      const cached = node._memberClusterCache.get(key);
      if (cached && typeof cached.ts === 'number' && nowMs - cached.ts < cacheTtlMs) {
        return cached.clusterId || null;
      }
      const cid = await node.registry.nodeToCluster(addr);
      const cidStr = cid != null ? String(cid) : null;
      node._memberClusterCache.set(key, { clusterId: cidStr, ts: nowMs });
      return cidStr;
    } catch (_ignored) {
      return null;
    }
  }

  const memberPromises = membersRaw.map(async (addr) => {
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

    const onChainClusterId = await getOnChainClusterId(addr);
    const key = String(addr || '').toLowerCase();
    const isSelf = !!(selfAddr && key === selfAddr);
    const inCluster = isSelf
      ? (!!clusterId && onChainClusterId && String(onChainClusterId).toLowerCase() === String(clusterId).toLowerCase())
      : !!clusterId;

    return {
      address: addr,
      isSelf: selfAddr ? addr.toLowerCase() === selfAddr : false,
      lastHeartbeatAgoSec,
      status,
      onChainClusterId,
      inCluster,
    };
  });

  const members = await Promise.all(memberPromises);
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
