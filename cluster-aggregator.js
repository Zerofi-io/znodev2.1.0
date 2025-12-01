import http from 'http';

const NODE_BASES = (process.env.ZNODE_API_BASES || '')
  .split(',')
  .map((s) => s.trim())
  .filter((s) => s.length > 0);

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
    '[ClusterAgg] No nodes configured. Set ZNODE_API_BASES to a comma-separated list of node API base URLs.',
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

  const clusterState = status.clusterState;
  const cluster = status.cluster || {};
  const finalized = !!cluster.finalized;
  const moneroHealth = status.monero && status.monero.health;
  const p2p = status.p2p || {};
  const p2pConnected = !!p2p.connected;

  return (
    !!cluster.id &&
    finalized &&
    clusterState === 'ACTIVE' &&
    moneroHealth === 'HEALTHY' &&
    p2pConnected
  );
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
    (c) => c.eligibleNodes >= MIN_ELIGIBLE_NODES && !!c.finalAddress,
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
    if (!url) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Bad request' }));
      return;
    }

    const u = new URL(url, 'http://localhost');

    if (method === 'GET' && u.pathname === '/clusters') {
      const data = await aggregateClusters();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
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
