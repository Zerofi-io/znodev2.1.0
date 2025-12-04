import http from 'http';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const dotenvResult = dotenv.config({ path: __dirname + '/.env' });
if (dotenvResult.error) {
  console.warn('[ClusterAgg] Warning: failed to load .env file:', dotenvResult.error.message);
  console.warn('[ClusterAgg] Proceeding with existing environment variables.');
}

function deriveNodeBasesFromEnv() {
  const bases = [];

  const bridgePortRaw = process.env.BRIDGE_API_PORT;
  const bridgePortParsed = bridgePortRaw != null ? Number(bridgePortRaw) : NaN;
  const bridgePort =
    Number.isFinite(bridgePortParsed) && bridgePortParsed > 0 ? bridgePortParsed : 3002;

  const publicIp = (process.env.PUBLIC_IP || '').trim();
  if (publicIp) {
    bases.push(`http://${publicIp}:${bridgePort}`);
  }

  const peersRaw = process.env.P2P_BOOTSTRAP_PEERS || '';
  if (peersRaw) {
    const tokens = peersRaw
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const token of tokens) {
      let host = null;

      const ip4Match = token.match(/\/ip4\/([^/]+)/);
      if (ip4Match && ip4Match[1]) {
        host = ip4Match[1];
      }

      if (!host) {
        const dns4Match = token.match(/\/dns4\/([^/]+)/);
        if (dns4Match && dns4Match[1]) {
          host = dns4Match[1];
        }
      }

      if (!host) {
        const cleaned = token.replace(/^[a-zA-Z]+:\/\//, '');
        const firstSlash = cleaned.indexOf('/');
        const withoutPath = firstSlash >= 0 ? cleaned.slice(0, firstSlash) : cleaned;
        const parts = withoutPath.split(':');
        if (parts[0]) {
          host = parts[0];
        }
      }

      if (host && (!publicIp || host !== publicIp)) {
        bases.push(`http://${host}:${bridgePort}`);
      }
    }
  }

  return Array.from(new Set(bases));
}

const ENV_NODE_BASES = (process.env.ZNODE_API_BASES || '')
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);

let NODE_BASES = ENV_NODE_BASES.length ? ENV_NODE_BASES : deriveNodeBasesFromEnv();

const PORT_RAW = process.env.CLUSTER_AGG_PORT;
const PORT_PARSED = PORT_RAW != null ? Number(PORT_RAW) : NaN;
const PORT = Number.isFinite(PORT_PARSED) && PORT_PARSED > 0 ? PORT_PARSED : 4000;

const TIMEOUT_RAW = process.env.CLUSTER_AGG_NODE_TIMEOUT_MS;
const TIMEOUT_PARSED = TIMEOUT_RAW != null ? Number(TIMEOUT_RAW) : NaN;
const NODE_TIMEOUT_MS =
  Number.isFinite(TIMEOUT_PARSED) && TIMEOUT_PARSED > 0 ? TIMEOUT_PARSED : 3000;

const MIN_ELIGIBLE_RAW = process.env.CLUSTER_MIN_ELIGIBLE_NODES;
const MIN_ELIGIBLE_PARSED = MIN_ELIGIBLE_RAW != null ? Number(MIN_ELIGIBLE_RAW) : NaN;
const MIN_ELIGIBLE_NODES =
  Number.isFinite(MIN_ELIGIBLE_PARSED) && MIN_ELIGIBLE_PARSED > 0
    ? MIN_ELIGIBLE_PARSED
    : 7;

if (!NODE_BASES.length) {
  console.error(
    '[ClusterAgg] No nodes configured. Set ZNODE_API_BASES to a comma-separated list of node API base URLs, or ensure PUBLIC_IP / P2P_BOOTSTRAP_PEERS / BRIDGE_API_PORT are configured.',
  );
}

async function fetchClusterStatus(baseUrl) {
  const url = `${baseUrl.replace(/\/$/, '')}/cluster-status`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), NODE_TIMEOUT_MS);

  try {
    const res = await fetch(url, { signal: controller.signal, cache: 'no-store' });
    if (!res.ok) {
      return { ok: false, error: `HTTP ${res.status}`, baseUrl };
    }
    const data = await res.json();
    return { ok: true, data, baseUrl };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    return { ok: false, error: msg, baseUrl };
  } finally {
    clearTimeout(timeout);
  }
}

function isNodeEligible(status) {
  if (!status || typeof status !== 'object') return false;
  if (status.eligibleForBridging === true) return true;
  const cluster = status.cluster || {};
  const finalized = !!cluster.finalized;
  const moneroHealth = status.monero && status.monero.health;
  const p2p = status.p2p || {};
  const p2pConnected = !!p2p.connected;
  return !!cluster.id && finalized && moneroHealth === 'HEALTHY' && p2pConnected;
}


async function fetchClusterReserve(baseUrl) {
  const url = `${baseUrl.replace(/\/$/, '')}/bridge/reserves`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), NODE_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: controller.signal, cache: 'no-store' });
    if (!res.ok) {
      return { ok: false, error: `HTTP ${res.status}`, baseUrl };
    }
    const data = await res.json();
    return { ok: true, data, baseUrl };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    return { ok: false, error: msg, baseUrl };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchNodeTxs(baseUrl, limit) {
  const base = baseUrl.replace(/\/$/, '');
  let url = baseUrl.replace(/\/$/, '') + '/bridge/txs';
  const n = Number(limit);
  if (Number.isFinite(n) && n > 0) {
    url += '?limit=' + Math.floor(n);
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), NODE_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: controller.signal, cache: 'no-store' });
    if (!res.ok) {
      return { ok: false, error: `HTTP ${res.status}`, baseUrl };
    }
    const data = await res.json();
    return { ok: true, data, baseUrl };
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    return { ok: false, error: msg, baseUrl };
  } finally {
    clearTimeout(timeout);
  }
}

async function aggregateClusters() {
  const timestamp = Date.now();

  if (!NODE_BASES.length) {
    return {
      timestamp,
      totalNodes: 0,
      totalClusters: 0,
      minEligibleNodes: MIN_ELIGIBLE_NODES,
      clusters: [],
      errors: ['ZNODE_API_BASES not configured'],
    };
  }

  const results = await Promise.allSettled(NODE_BASES.map((base) => fetchClusterStatus(base)));

  const clusters = new Map();
  const errors = [];

  for (const r of results) {
    if (r.status !== 'fulfilled') {
      errors.push('Request failed');
      continue;
    }
    const { ok, data, error, baseUrl } = r.value;
    if (!ok) {
      errors.push(`[${baseUrl}] ${error}`);
      continue;
    }

    const eligible = isNodeEligible(data);
    const cluster = (data && data.cluster) || {};
    const clusterId = cluster.id;
    if (!clusterId) {
      continue;
    }

    let entry = clusters.get(clusterId);
    if (!entry) {
      entry = {
        id: clusterId,
        finalAddress: cluster.finalAddress || null,
        size: typeof cluster.size === 'number' ? cluster.size : null,
        eligibleNodes: 0,
        totalNodes: 0,
        nodes: [],
      };
      clusters.set(clusterId, entry);
    }

    entry.totalNodes += 1;
    if (eligible) {
      entry.eligibleNodes += 1;
    }

    const p2p = data.p2p || {};

    entry.nodes.push({
      apiBase: baseUrl,
      eligibleForBridging: eligible,
      clusterState: data.clusterState || null,
      moneroHealth: data.monero && data.monero.health ? data.monero.health : null,
      p2pConnected: !!p2p.connected,
      lastHeartbeatAgoSec:
        typeof p2p.lastHeartbeatAgoSec === 'number' ? p2p.lastHeartbeatAgoSec : null,
    });
  }

  const filtered = Array.from(clusters.values()).filter(
    (c) => c.eligibleNodes > 0 && !!c.finalAddress,
  );

  return {
    timestamp,
    totalNodes: NODE_BASES.length,
    totalClusters: filtered.length,
    minEligibleNodes: MIN_ELIGIBLE_NODES,
    clusters: filtered,
    errors,
  };
}

const server = http.createServer(async (req, res) => {
  try {
    const { method, url } = req;
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }
    if (!url) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Bad request' }));
      return;
    }

    const u = new URL(url, 'http://localhost');

    // Proxy POST /bridge/deposit/request to a healthy node
    if (u.pathname === '/bridge/deposit/request' && method === 'POST') {
      if (!NODE_BASES.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes configured' }));
        return;
      }

      const body = await new Promise((resolve, reject) => {
        let raw = '';
        req.on('data', (chunk) => { raw += chunk; });
        req.on('end', () => {
          try { resolve(raw ? JSON.parse(raw) : {}); } catch (e) { resolve({}); }
        });
        req.on('error', reject);
      });

      let lastError = null;
      for (const base of NODE_BASES) {
        const target = base.replace(/\/$/, '') + '/bridge/deposit/request';
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), NODE_TIMEOUT_MS);
          const resp = await fetch(target, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal,
          });
          clearTimeout(timeout);
          if (!resp.ok) {
            lastError = new Error(`HTTP ${resp.status}`);
            continue;
          }
          const data = await resp.json().catch(() => ({}));
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(data));
          return;
        } catch (e) {
          lastError = e;
        }
      }

      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Deposit routing unavailable',
          message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
        }),
      );
      return;
    }

    // Proxy GET /bridge/signatures to a healthy node
    if (u.pathname === '/bridge/signatures' && method === 'GET') {
      if (!NODE_BASES.length) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No nodes configured' }));
        return;
      }

      let lastError = null;
      for (const base of NODE_BASES) {
        const target = base.replace(/\/$/, '') + '/bridge/signatures';
        try {
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), NODE_TIMEOUT_MS);
          const resp = await fetch(target, { method: 'GET', signal: controller.signal });
          clearTimeout(timeout);
          if (!resp.ok) {
            lastError = new Error(`HTTP ${resp.status}`);
            continue;
          }
          const data = await resp.json().catch(() => ({}));
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(data));
          return;
        } catch (e) {
          lastError = e;
        }
      }

      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          error: 'Signature routing unavailable',
          message: lastError ? lastError.message || String(lastError) : 'no nodes responded',
        }),
      );
      return;
    }

    const u2 = u;
    const method2 = method;

    if (method === 'GET' && u.pathname === '/clusters') {
      const data = await aggregateClusters();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
      return;
    }

    if (method === 'GET' && u.pathname === '/api/clusters/active') {
      const data = await aggregateClusters();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          clusters: data.clusters.map((c) => ({
            id: c.id,
            multisigAddress: c.finalAddress,
            nodeCount: c.totalNodes,
            eligibleNodes: c.eligibleNodes,
            timestamp: data.timestamp,
          })),
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/api/reserves/xmr') {
      const data = await aggregateClusters();
      const clusters = [];
      let totalAtomic = 0n;
      for (const c of data.clusters) {
        const nodes = Array.isArray(c.nodes) ? c.nodes : [];
        const primary = nodes.find((n) => n.eligibleForBridging) || nodes[0];
        if (!primary || !primary.apiBase) {
          clusters.push({
            id: c.id,
            multisigAddress: c.finalAddress || null,
            nodeApiBase: null,
            error: 'no_node',
          });
          continue;
        }
        const r = await fetchClusterReserve(primary.apiBase);
        if (!r.ok) {
          clusters.push({
            id: c.id,
            multisigAddress: c.finalAddress || null,
            nodeApiBase: primary.apiBase,
            error: r.error || 'reserve_error',
          });
          continue;
        }
        const d = r.data || {};
        const balRaw = d.balanceAtomic != null ? d.balanceAtomic : d.unlockedBalanceAtomic;
        let balAtomic = 0n;
        try {
          if (balRaw != null) balAtomic = BigInt(String(balRaw));
        } catch (_ignored) {}
        totalAtomic += balAtomic;
        clusters.push({
          id: c.id,
          multisigAddress: d.multisigAddress || c.finalAddress || null,
          nodeApiBase: primary.apiBase,
          addressVerified: !!d.addressVerified,
          balanceAtomic: balAtomic.toString(),
          balance: d.balance || null,
          unlockedBalanceAtomic: d.unlockedBalanceAtomic != null ? String(d.unlockedBalanceAtomic) : null,
          unlockedBalance: d.unlockedBalance || null,
          checkedAt: d.checkedAt || data.timestamp,
        });
      }
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
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          timestamp: data.timestamp,
          totalClusters: clusters.length,
          clusters,
          totalAtomic: totalAtomic.toString(),
          total: toDecimal(totalAtomic),
          errors: data.errors || [],
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/api/txs') {
      const limitParam = u.searchParams.get('limit');
      const envLimitRaw = process.env.CLUSTER_AGG_TX_LIMIT;
      const limitRaw = limitParam != null && limitParam !== '' ? limitParam : envLimitRaw;
      const limitParsed = limitRaw != null ? Number(limitRaw) : NaN;
      const limit =
        Number.isFinite(limitParsed) && limitParsed > 0 ? Math.floor(limitParsed) : 20;
      const perNodeLimit = Math.max(limit * 2, 20);

      const errors = [];
      const txMap = new Map();

      const results = await Promise.allSettled(
        NODE_BASES.map((base) => fetchNodeTxs(base, perNodeLimit)),
      );

      for (const r of results) {
        if (r.status !== 'fulfilled') {
          errors.push('Request failed');
          continue;
        }
        const { ok, data, error, baseUrl } = r.value;
        if (!ok) {
          errors.push(`[${baseUrl}] ${error}`);
          continue;
        }
        const list = Array.isArray(data.transactions) ? data.transactions : [];
        for (const tx of list) {
          const key = String(tx.txHash || '') + ':' + String(tx.type || '');
          if (!key.trim()) continue;
          const existing = txMap.get(key);
          if (!existing || (tx.blockNumber ?? 0) > (existing.blockNumber ?? 0)) {
            txMap.set(key, { ...tx, nodeApiBase: baseUrl });
          }
        }
      }

      const all = Array.from(txMap.values());
      all.sort((a, b) => {
        const ta = typeof a.timestamp === 'number' ? a.timestamp : 0;
        const tb = typeof b.timestamp === 'number' ? b.timestamp : 0;
        if (ta !== tb) return tb - ta;
        if ((a.blockNumber || 0) !== (b.blockNumber || 0)) {
          return (b.blockNumber || 0) - (a.blockNumber || 0);
        }
        if (a.logIndex != null && b.logIndex != null) return b.logIndex - a.logIndex;
        return 0;
      });

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          timestamp: Date.now(),
          totalNodes: NODE_BASES.length,
          totalTransactions: all.length,
          transactions: all.slice(0, limit),
          errors,
        }),
      );
      return;
    }

    if (method === 'GET' && u.pathname === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(
        JSON.stringify({
          status: 'ok',
          nodesConfigured: NODE_BASES.length,
          minEligibleNodes: MIN_ELIGIBLE_NODES,
        }),
      );
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Internal error', message: msg }));
  }
});

server.listen(PORT, () => {
  console.log(
    `[ClusterAgg] HTTP aggregator listening on port ${PORT} (nodes: ${NODE_BASES.length}, minEligible: ${MIN_ELIGIBLE_NODES})`,
  );
});
