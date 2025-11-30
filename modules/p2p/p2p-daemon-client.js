import net from 'net';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { EventEmitter } from 'events';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default class P2PDaemonClient extends EventEmitter {
  constructor(options = {}) {
    super();
    this.on('error', (err) => {
      console.error('[P2P-Client] Client error:', err.message || String(err));
    });
    this.socketPath = options.socketPath || '/tmp/znode-p2p.sock';
    this.daemonPath = options.daemonPath || path.join(__dirname, '..', '..', 'p2p-daemon', 'p2p-daemon');
    this.privateKey = options.privateKey;
    this.ethereumAddress = options.ethereumAddress;
    this.listenAddr = options.listenAddr || '/ip4/0.0.0.0/tcp/9000';
    this.bootstrapPeers = options.bootstrapPeers || '';

    this.process = null;
    this.requestId = 0;
    this.pendingRequests = new Map();
    this.connected = false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.socket = null;
    this.buffer = '';
    this._peerId = '';
    this._connectedPeers = 0;
  }

  failPendingRequests(error) {
    if (!this.pendingRequests || this.pendingRequests.size === 0) {
      return;
    }
    const err = error instanceof Error ? error : new Error(error || 'P2P daemon connection lost');
    for (const { reject, timeout } of this.pendingRequests.values()) {
      if (timeout) {
        clearTimeout(timeout);
      }
      try {
        reject(err);
      } catch {}
    }
    this.pendingRequests.clear();
  }

  async start() {
    if (!fs.existsSync(this.daemonPath)) {
      throw new Error(
        `p2p-daemon binary not found at ${this.daemonPath}. Run 'cd p2p-daemon && go build' first.`,
      );
    }

    if (fs.existsSync(this.socketPath)) {
      fs.unlinkSync(this.socketPath);
    }

    const args = [
      '--socket',
      this.socketPath,
      '--key',
      this.privateKey,
      '--eth-address',
      this.ethereumAddress,
      '--listen',
      this.listenAddr,
    ];

    if (this.bootstrapPeers) {
      args.push('--bootstrap', this.bootstrapPeers);
    }

    console.log('[P2P-Client] Starting Go p2p-daemon...');
    this.process = spawn(this.daemonPath, args, {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    this.process.stdout.on('data', (data) => {
      const lines = data
        .toString()
        .split('\n')
        .filter((l) => l.trim());
      for (const line of lines) {
        console.log(`[p2p-daemon] ${line}`);
      }
    });

    this.process.stderr.on('data', (data) => {
      const lines = data
        .toString()
        .split('\n')
        .filter((l) => l.trim());
      for (const line of lines) {
        console.error(`[p2p-daemon] ${line}`);
      }
    });

    this.process.on('exit', (code, signal) => {
      console.log(`[P2P-Client] Daemon exited with code ${code}, signal ${signal}`);
      this.connected = false;
      this.emit('daemon-exit', { code, signal });
    });

    await this.waitForSocket(10000);

    await this.connect();

    const status = await this.status();
    console.log(`[P2P-Client] Connected to daemon. PeerID: ${status.peerId}`);

    return status;
  }

  async waitForSocket(timeoutMs = 10000) {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (fs.existsSync(this.socketPath)) {
        await new Promise((r) => setTimeout(r, 100));
        return;
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error(`Timeout waiting for daemon socket at ${this.socketPath}`);
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.createConnection(this.socketPath);

      this.socket.on('connect', () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        resolve();
      });

      this.socket.on('data', (data) => {
        this.handleData(data);
      });

      this.socket.on('error', (err) => {
        if (!this.connected) {
          reject(err);
          return;
        }
        this.connected = false;
        console.error('[P2P-Client] Socket error:', err.message);
        this.failPendingRequests(err);
        this.emit('error', err);
      });

      this.socket.on('close', () => {
        this.connected = false;
        this.failPendingRequests(new Error('P2P daemon socket closed'));
        this.emit('disconnect');
      });
    });
  }

  handleData(data) {
    this.buffer += data.toString();

    let newlineIndex;
    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      const line = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + 1);

      if (line.trim()) {
        try {
          const response = JSON.parse(line);
          this.handleResponse(response);
        } catch {
          console.error('[P2P-Client] Invalid JSON response:', line);
        }
      }
    }
  }

  handleResponse(response) {
    const { id, result, error } = response;

    const pending = this.pendingRequests.get(id);
    if (!pending) {
      console.warn('[P2P-Client] Received response for unknown request:', id);
      return;
    }

    this.pendingRequests.delete(id);
    clearTimeout(pending.timeout);

    if (error) {
      pending.reject(new P2PError(error.message, error.code, error.data));
    } else {
      pending.resolve(result);
    }
  }

  async call(method, params, timeoutMs = 300000) {
    if (!this.connected) {
      throw new Error('Not connected to p2p-daemon');
    }

    const id = ++this.requestId;
    const request = {
      jsonrpc: '2.0',
      method,
      params,
      id,
    };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`RPC call ${method} timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      this.pendingRequests.set(id, { resolve, reject, timeout });

      this.socket.write(JSON.stringify(request) + '\n', (err) => {
        if (err) {
          this.pendingRequests.delete(id);
          clearTimeout(timeout);
          reject(err);
        }
      });
    });
  }

  async stop() {
    if (this.socket) {
      this.socket.end();
      this.socket = null;
    }

    if (this.process) {
      this.process.kill('SIGTERM');

      await new Promise((resolve) => {
        const timeout = setTimeout(() => {
          this.process.kill('SIGKILL');
          resolve();
        }, 5000);

        this.process.on('exit', () => {
          clearTimeout(timeout);
          resolve();
        });
      });

      this.process = null;
    }

    this.connected = false;
    this.failPendingRequests(new Error('P2P client stopped'));
    console.log('[P2P-Client] Stopped');
  }

  async status() {
    const status = await this.call('P2P.Status', {});
    if (status && typeof status.peerId === 'string') {
      this._peerId = status.peerId;
    }
    if (status && typeof status.connectedPeers === 'number') {
      this._connectedPeers = status.connectedPeers;
    }
    return status;
  }

  peerID() {
    return this._peerId || '';
  }

  async joinCluster(clusterId, members, isCoordinator) {
    const memberList = members.map((addr) => ({
      address: addr,
      peerId: '',
    }));
    return this.call('P2P.JoinCluster', {
      clusterId,
      members: memberList,
      isCoordinator,
    });
  }

  async leaveCluster(clusterId) {
    return this.call('P2P.LeaveCluster', { clusterId });
  }

  async broadcastRoundData(clusterId, sessionId, round, payload) {
    const _topic = this.topic(clusterId, sessionId);
    return this.call('P2P.BroadcastRoundData', {
      clusterId: _topic,
      round,
      payload,
    });
  }

  async waitForRoundCompletion(clusterId, sessionId, round, members, timeoutMs = 300000) {
    const _topic = this.topic(clusterId, sessionId);
    try {
      const result = await this.call(
        'P2P.WaitForRoundCompletion',
        {
          clusterId: _topic,
          round,
          members,
          timeoutMs,
        },
        timeoutMs + 5000,
      );

      return !!(result && result.complete);
    } catch (e) {
      if (e && e.code === -32000 && e.data && typeof e.data === 'object') {
        const receivedCount = Array.isArray(e.data.received) ? e.data.received.length : 0;
        const missing = Array.isArray(e.data.missing) ? e.data.missing : [];
        const total = Array.isArray(members) ? members.length : 0;
        console.log(
          `[P2P-Client] Round ${round} timeout: ${receivedCount}/${total} received; ` +
            `missing: ${missing.join(', ')}`,
        );
        return false;
      }
      throw e;
    }
  }
  async getPeerPayloads(clusterId, sessionId, round, members) {
    const _topic = this.topic(clusterId, sessionId);
    const result = await this.call('P2P.GetPeerPayloads', {
      clusterId: _topic,
      round,
      members,
    });

    const sorted = (result.payloads || []).sort((a, b) =>
      a.address.toLowerCase().localeCompare(b.address.toLowerCase()),
    );
    return sorted.map((p) => p.payload);
  }
  async computeConsensusHash(clusterId, sessionId, round, members) {
    const _topic = this.topic(clusterId, sessionId);
    const result = await this.call('P2P.ComputeConsensusHash', {
      clusterId: _topic,
      round,
      members,
    });
    return result.hash;
  }

  async runConsensus(clusterId, sessionId, phase, data, members, timeoutMs) {
    const _topic = this.topic(clusterId, sessionId);
    const effectiveTimeout =
      timeoutMs != null ? timeoutMs : Number(process.env.PBFT_RPC_TIMEOUT_MS || 300000);
    const result = await this.call(
      'P2P.RunConsensus',
      {
        clusterId: _topic,
        phase,
        data: data || '',
        members,
      },
      effectiveTimeout,
    );
    return result;
  }

  async getPBFTDebugState(clusterId, phase) {
    const result = await this.call('P2P.PBFTDebug', {
      clusterId,
      phase,
    });
    return result;
  }

  async broadcastRoundSignature(clusterId, sessionId, round, dataHash, signature) {
    const sig = signature || dataHash;
    const hash = dataHash || sig;
    return this.call('P2P.BroadcastSignature', {
      clusterId,
      round,
      dataHash: hash,
      signature: sig,
    });
  }

  async waitForSignatureBarrier(clusterId, sessionId, round, members, timeoutMs = 300000) {
    const result = await this.call(
      'P2P.WaitForSignatureBarrier',
      {
        clusterId,
        round,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );

    return {
      complete: result.complete,
      consensusHash: result.consensusHash,
      mismatched: result.mismatched || [],
      missing: result.missing || [],
    };
  }
  async broadcastIdentity(clusterId, publicKey) {
    return this.call('P2P.BroadcastIdentity', {
      clusterId,
      publicKey,
    });
  }

  async waitForIdentities(clusterId, members, timeoutMs = 60000) {
    return this.call(
      'P2P.WaitForIdentities',
      {
        clusterId,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );
  }

  async getPeerBindings(clusterId) {
    const result = await this.call('P2P.GetPeerBindings', { clusterId });

    const bindings = {};
    for (const b of result.bindings || []) {
      bindings[b.address] = b.peerId;
    }
    return bindings;
  }

  async clearPeerBindings(clusterId) {
    return this.call('P2P.ClearPeerBindings', { clusterId });
  }

  async getPeerScores(clusterId) {
    const result = await this.call('P2P.GetPeerScores', { clusterId });
    const scores = {};
    for (const s of result.scores || []) {
      scores[s.address] = s.score;
    }
    return scores;
  }

  async reportPeerFailure(clusterId, address, reason) {
    return this.call('P2P.ReportPeerFailure', {
      clusterId,
      address,
      reason,
    });
  }

  async connectToCluster(clusterId, clusterNodes, isCoordinator, registry) {
    await this.joinCluster(clusterId, clusterNodes, isCoordinator);

    await this.broadcastIdentity(clusterId, '');
    return true;
  }

  cleanupOldMessages() {}

  countConnectedPeers() {
    return this._connectedPeers || 0;
  }

  get node() {
    return this.connected ? { status: 'started' } : null;
  }

  async broadcastHeartbeat() {
    await this.call('P2P.BroadcastHeartbeat', {});
  }

  async startQueueDiscovery(stakingAddress, chainId = 1) {
    await this.call('P2P.StartQueueDiscovery', {
      stakingAddress: stakingAddress,
      chainId: chainId,
      intervalMs: 30000,
    });
  }

  async setHeartbeatDomain(chainId, stakingAddress) {
    await this.call('P2P.SetHeartbeatDomain', {
      chainId: chainId,
      stakingAddress: stakingAddress,
    });
  }

  async startHeartbeat(intervalMs = 30000) {
    await this.call('P2P.StartHeartbeat', { intervalMs });
  }

  async stopHeartbeat() {
    await this.call('P2P.StopHeartbeat', {});
  }

  async getRecentPeers(ttlMs = 300000) {
    const result = await this.call('P2P.GetRecentPeers', { ttlMs });
    return result?.peers || [];
  }

  async clearClusterRounds(clusterId, sessionId, rounds) {
    const _topic = this.topic(clusterId, sessionId);
    return this.call('P2P.ClearClusterRounds', { clusterId: _topic, rounds });
  }

  async clearClusterSignatures(clusterId, rounds = []) {
    return this.call('P2P.ClearClusterSignatures', {
      clusterId,
      rounds,
    });
  }

  async waitForIdentityBarrier(clusterId, members, timeoutMs = 60000) {
    return this.call(
      'P2P.WaitForIdentityBarrier',
      {
        clusterId,
        members,
        timeoutMs,
      },
      timeoutMs + 5000,
    );
  }

  async getLastHeartbeat(address) {
    const result = await this.call('P2P.GetLastHeartbeat', { address });
    const ts = result && typeof result.timestamp !== 'undefined' ? Number(result.timestamp) : 0;
    if (!Number.isFinite(ts) || ts <= 0) return null;
    return { timestamp: ts };
  }

  async getHeartbeats(ttlMs = 300000) {
    const result = await this.call('P2P.GetHeartbeats', { ttlMs });
    const map = new Map();
    for (const hb of result?.heartbeats || []) {
      if (!hb || !hb.address) continue;
      map.set(String(hb.address).toLowerCase(), { timestamp: hb.timestamp });
    }
    return map;
  }

  async getQueuePeers(ttlMs = 300000) {
    const result = await this.call('P2P.GetRecentPeers', { ttlMs });
    return result?.peers || [];
  }

  setActiveCluster(clusterId) {
    this._activeCluster = clusterId;
  }

  get activeCluster() {
    return this._activeCluster || null;
  }

  get roundData() {
    if (!this._roundData) {
      this._roundData = new Map();
    }
    return this._roundData;
  }

  set round0Responder(fn) {
    this._round0Responder = fn;
  }

  get round0Responder() {
    return this._round0Responder || null;
  }
  topic(clusterId, sessionId) {
    return sessionId ? `${clusterId}:${sessionId}` : clusterId;
  }
}

class P2PError extends Error {
  constructor(message, code, data) {
    super(message);
    this.name = 'P2PError';
    this.code = code;
    this.data = data;
  }
}
