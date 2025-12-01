import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import path from 'path';
import fs from 'fs';
import os from 'os';
import { execFile, spawn } from 'child_process';
import { promisify } from 'util';
import crypto from 'crypto';
import './modules/core/console-shim.js';

const execFileAsync = promisify(execFile);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const dotenvResult = dotenv.config({ path: __dirname + '/.env' });
if (dotenvResult.error) {
  console.error('[ERROR] Failed to load .env file:', dotenvResult.error.message);
  console.error('Please ensure .env file exists. Run scripts/setup.sh to create it.\n');
  process.exit(1);
}

import ConfigValidator from './modules/core/config-validator.js';
import { resolveRuntimeConfig } from './modules/core/runtime-config.js';
import { computeRoundDigest, canonicalizeMembers } from './modules/cluster/round-utils.js';
import {
  pruneBlacklistEntries,
  computeAdaptiveBlacklistCooldownMs,
} from './modules/cluster/blacklist-utils.js';
import { startHeartbeatLoop, startSlashingLoop, monitorNetwork } from './modules/core/monitoring.js';
const validator = new ConfigValidator();
const validationResult = validator.validate();

if (!validationResult.valid) {
  validator.printResults();
  console.error(
    '\n[ERROR] Configuration validation failed. Please fix the errors above and try again.',
  );
  console.error('See config/.env.example for configuration documentation.\n');
  process.exit(1);
}

if (validationResult.warnings.length > 0) {
  validator.printResults();
}

const { TEST_MODE, DRY_RUN, EFFECTIVE_RPC_URL, usedDemoRpc } = resolveRuntimeConfig(process.env);

if (TEST_MODE) {
  console.warn('[WARN]  ═══════════════════════════════════════════════════════════════');
  console.warn('[WARN]  WARNING: TEST_MODE is ENABLED');
  console.warn('[WARN]  This mode uses insecure defaults and should NEVER be used in production!');
  console.warn('[WARN]  Set TEST_MODE=0 for production use.');
  console.warn('[WARN]  ═══════════════════════════════════════════════════════════════');
}

if (DRY_RUN) {
  console.log(
    '[INFO]  DRY_RUN mode enabled: on-chain transactions will be simulated but not sent.',
  );
  console.log('[INFO]  Set DRY_RUN=0 to send real transactions.');
  if (!process.env.DRY_RUN) {
    console.warn(
      '[WARN]  DRY_RUN defaulted to enabled (not explicitly set). Set DRY_RUN=0 for production.',
    );
  }
}

if (!EFFECTIVE_RPC_URL) {
  console.error('ERROR: RPC_URL or ETH_RPC_URL is required.');
  console.error('Set RPC_URL=<your-rpc-url> or enable TEST_MODE=1 for testing.');
  process.exit(1);
}

if (usedDemoRpc) {
  console.warn('[WARN]  TEST_MODE: Using demo RPC URL (rate-limited, for testing only)');
}

if (!process.env.PRIVATE_KEY) {
  console.error('Environment variable PRIVATE_KEY is required to run znode.');
  process.exit(1);
}

if (!process.env.MONERO_WALLET_PASSWORD) {
  console.error('ERROR: MONERO_WALLET_PASSWORD is required.');
  console.error('Set MONERO_WALLET_PASSWORD=<password> in your .env file.');
  console.error(
    'SECURITY: Never derive passwords from private keys. Use unique, random passwords.',
  );
  process.exit(1);
}
console.log('Using MONERO_WALLET_PASSWORD from environment for Monero wallet.');

import { AsyncMutex, ClusterState, ClusterStateMachine } from './modules/cluster/cluster-state.js';
import { ethers } from 'ethers';
import MoneroRPC from './modules/monero/monero-rpc.js';
import RPCManager from './modules/monero/rpc-manager.js';
import P2PDaemonClient from './modules/p2p/p2p-daemon-client.js';
import { withRetry } from './modules/core/retry.js';
import {
  startBridgeAPI,
  stopBridgeAPI,
  startDepositMonitor,
  stopDepositMonitor,
  startWithdrawalMonitor,
  stopWithdrawalMonitor,
  getMintSignature,
  getAllPendingSignatures,
  registerDepositRequest,
  getWithdrawalStatus,
  getAllPendingWithdrawals,
  handleWithdrawalSignRequest,
} from './modules/bridge/api.js';
import { checkSelfStakeHealth, checkEmergencySweep, dissolveStuckFinalizedCluster } from './modules/core/health.js';
import { metrics } from './modules/core/metrics.js';

const P2PExchange = P2PDaemonClient;

const MoneroHealth = {
  HEALTHY: 'HEALTHY',
  NEEDS_ATTEMPT_RESET: 'NEEDS_ATTEMPT_RESET',
  NEEDS_GLOBAL_RESET: 'NEEDS_GLOBAL_RESET',
  QUARANTINED: 'QUARANTINED',
};

class ZNode {
  constructor() {
    this.provider = new ethers.JsonRpcProvider(EFFECTIVE_RPC_URL);
    this.wallet = new ethers.Wallet(process.env.PRIVATE_KEY, this.provider);

    this._orchestrationMutex = new AsyncMutex();
    this.clusterState = new ClusterState();
    this.stateMachine = new ClusterStateMachine();

    this.stateMachine.on('transition', ({ from, to, data }) => {
      metrics.onStateTransition(from, to, data);
    });

    this.clusterState.on('stateChange', ({ newState }) => {
      this._activeClusterId = newState.clusterId || null;
      this._clusterMembers = newState.members || null;
      this._clusterFinalAddress = newState.finalAddress || null;
      this._clusterFinalized = !!newState.finalized;
      this._pendingR3 = newState.pendingR3 || null;
      this._clusterFinalizationStartAt = newState.finalizationStartAt || null;
      this._clusterFailoverAttempted = !!newState.failoverAttempted;
      this._clusterCooldownUntil = newState.cooldownUntil || null;
      this._lastClusterFailureAt = newState.lastFailureAt || null;
      this._lastClusterFailureReason = newState.lastFailureReason || null;
    });
    this._clusterBlacklistPath = __dirname + '/.cluster-blacklist.json';
    this._clusterFailures = new Map();
    this._clusterFailMeta = {};
    this._clusterBlacklist = {};
    this._blacklistSavePending = false;
    this._blacklistSaveQueued = false;

    this._clusterCooldownUntil = null;
    this._lastClusterFailureAt = null;
    this._lastClusterFailureReason = null;

    this.moneroPassword = process.env.MONERO_WALLET_PASSWORD;

    const registryABI = [
      'function registerNode(bytes32 codeHash) external',
      'function unregisterNode() external',
      'function finalizeCluster(address[] members, string moneroAddress) external returns (bytes32 clusterId)',
      'function dissolveCluster(bytes32 clusterId) external returns (bool rewarded)',
      'function clusters(bytes32) external view returns (string moneroAddress, uint256 createdAt, bool finalized)',
      'function getRegisteredNodes(uint256 offset, uint256 limit) external view returns (address[] memory)',
      'function canParticipate(address node) external view returns (bool)',
      'function canDissolve(bytes32 clusterId) external view returns (bool, uint256 blacklistedCount)',
      'function isClusterActive(bytes32 clusterId) external view returns (bool)',
      'function getClusterMembers(bytes32 clusterId) external view returns (address[] memory)',
      'function getClusterHealth(bytes32 clusterId) external view returns (uint256 healthy, uint256 slashed, uint256 blacklisted)',
      'function canRejoinCluster(address node) external view returns (bool canRejoin, bytes32 clusterId)',
      'function nodes(address) external view returns (bytes32 codeHash, bool registered, bool inCluster)',
      'function nodeToCluster(address) external view returns (bytes32)',
    ];

    const stakingABI = [
      'function getNodeInfo(address node) external view returns (uint256,uint256,uint256,bool,uint256,uint256,uint256)',
      'function stake(bytes32 _codeHash, string _moneroFeeAddress) external',
      'function topUpStake() external',
      'function getActiveNodes() external view returns (address[] memory)',
      'function slashForDowntimeWithProof(address _node, uint256 lastHeartbeatTs, bytes calldata sig) external',
      'function getSlashingInfo(address _node) external view returns (uint256 slashingStage, uint256 lastProved, bool isBlacklisted)',
      'function isBlacklisted(address _node) external view returns (bool)',
      'function getDissolutionPool() external view returns (uint256)',
      'function owner() external view returns (address)',
    ];

    const zfiABI = [
      'function balanceOf(address) view returns (uint256)',
      'function allowance(address owner, address spender) view returns (uint256)',
      'function approve(address spender, uint256 amount) returns (bool)',
      'function decimals() view returns (uint8)',
    ];
    const exchangeCoordinatorABI = [
      'function submitExchangeInfo(bytes32 clusterId, uint8 round, string exchangeInfo, address[] clusterNodes) external',
      'function getExchangeRoundInfo(bytes32 clusterId, uint8 round, address[] clusterNodes) external view returns (address[] addresses, string[] exchangeInfos)',
      'function getExchangeRoundStatus(bytes32 clusterId, uint8 round) external view returns (bool complete, uint8 submitted)',
    ];

    const bridgeABI = [
      'function mint(bytes32 clusterId, bytes32 depositId, uint256 amount, bytes signature) external',
      'function burnForXMR(bytes32 clusterId, uint256 amount, string xmrAddress) external',
      'function isSignatureUsed(bytes signature) external view returns (bool)',
      'event TokensMinted(address indexed user, bytes32 indexed clusterId, bytes32 indexed depositId, uint256 userAmount, uint256 fee)',
      'event TokensBurned(address indexed user, bytes32 indexed clusterId, string xmrAddress, uint256 burnAmount, uint256 fee)',
    ];

    const KNOWN_DEFAULTS = {
      11155111: {
        REGISTRY_ADDR: '0x2716396399377f3D6412660956280e308794417C',
        STAKING_ADDR: '0xC7ECD047a64AcBaD9809dfd26b20E9437514cc3D',
        ZFI_ADDR: '0x2B8428ab14004483f946eBD6b012b8983C2638B1',
        COORDINATOR_ADDR: '0xA02269FDCbA510B4D6aE6E36FcCC0Cdde0164815',
        BRIDGE_ADDR: '0xD35D383298ea021945d91A0F9ead65c101bdA384',
      },
    };

    this._knownDefaults = KNOWN_DEFAULTS;
    this._chainIdVerified = false;
    this._registryABI = registryABI;
    this._stakingABI = stakingABI;
    this._zfiABI = zfiABI;
    this._exchangeCoordinatorABI = exchangeCoordinatorABI;
    this._bridgeABI = bridgeABI;

    this.registry = null;
    this.bridge = null;

    this._processedDeposits = new Set();
    this._pendingMintSignatures = new Map();
    this._depositMonitorRunning = false;
    this.staking = null;
    this.zfi = null;
    this.exchangeCoordinator = null;

    this.monero = new MoneroRPC({
      url:
        process.env.MONERO_RPC_URL || process.env.MONERO_WALLET_RPC_URL || 'http://127.0.0.1:18083',
    });

    this.rpcManager = new RPCManager({ url: this.monero.url });
    this._rpcRestartPromise = null;
    this._rpcRestartAttempts = 0;
    this._rpcLastRestartTime = 0;
    this._rpcRestartWindow = [];
    this._rpcCircuitOpen = false;
    this._rpcCircuitOpenUntil = 0;
    this._rpcCircuitHalfOpen = false;
    this._walletRotationStats = { total: 0 };
    this.moneroHealth = MoneroHealth.HEALTHY;
    this.moneroErrorCounts = {
      kexRoundMismatch: 0,
      kexTimeout: 0,
      rpcFailure: 0,
    };
    this.currentAttemptWallet = null;

    this._walletMutex = new AsyncMutex();

    this._moneroRawCall = this.monero.call.bind(this.monero);

    this.monero.call = async (method, params = {}, timeout) => {
      return this._walletMutex.withLock(async () => {
        const waitOnCall = process.env.RPC_CIRCUIT_BREAKER_WAIT_ON_CALL === '1';
        const now = Date.now();

        if (this._rpcCircuitOpen && now < this._rpcCircuitOpenUntil) {
          if (waitOnCall) {
            const remaining = Math.ceil((this._rpcCircuitOpenUntil - now) / 1000);
            console.log(
              `[RPC] Circuit breaker open, wait-through mode: waiting ${remaining}s before retry...`,
            );
            await new Promise((resolve) => setTimeout(resolve, this._rpcCircuitOpenUntil - now));
            this._rpcCircuitOpen = false;
            this._rpcCircuitHalfOpen = true;
            console.log('[RPC] Circuit breaker entering half-open state after wait-through...');
          } else {
            const remaining = Math.ceil((this._rpcCircuitOpenUntil - now) / 1000);
            throw new Error(
              `RPC circuit breaker open: cooldown active for ${remaining}s. Manual intervention may be required.`,
            );
          }
        }

        if (this._rpcCircuitOpen && now >= this._rpcCircuitOpenUntil) {
          console.log('[RPC] Circuit breaker entering half-open state, testing recovery...');
          this._rpcCircuitHalfOpen = true;
          this._rpcCircuitOpen = false;
        }

        try {
          const result = await this._moneroRawCall(method, params, timeout);
          if (this._rpcCircuitHalfOpen) {
            console.log('[RPC] Circuit breaker closed: recovery successful');
            this._rpcCircuitHalfOpen = false;
            this._rpcRestartWindow = [];
            this._rpcRestartAttempts = 0;
          }
          return result;
        } catch (e) {
          const msg = ((e && e.code) || (e && e.message) || '').toString();
          if (/ECONNREFUSED|ETIMEDOUT|ECONNRESET|EHOSTUNREACH|ENETUNREACH|timeout/i.test(msg)) {
            if (this._rpcCircuitHalfOpen) {
              console.error('[RPC] Circuit breaker re-opening: recovery failed');
              this._rpcCircuitOpen = true;
              this._rpcCircuitHalfOpen = false;
              const cooldownMs = Number(process.env.RPC_CIRCUIT_BREAKER_COOLDOWN_MS || 60000);
              this._rpcCircuitOpenUntil = Date.now() + cooldownMs;
              throw new Error(
                `RPC recovery failed in half-open state. Circuit breaker re-opened for ${cooldownMs / 1000}s.`,
              );
            }

            if (this._rpcRestartPromise) {
              console.log('[RPC] Restart already in progress, waiting...');
              await this._rpcRestartPromise;
            } else {
              const now = Date.now();
              const windowMs = Number(process.env.RPC_CIRCUIT_BREAKER_WINDOW_MS || 300000);
              const maxFailures = Number(process.env.RPC_CIRCUIT_BREAKER_MAX_FAILURES || 10);
              const cooldownMs = Number(process.env.RPC_CIRCUIT_BREAKER_COOLDOWN_MS || 60000);

              this._rpcRestartWindow = this._rpcRestartWindow.filter((t) => t > now - windowMs);

              if (this._rpcRestartWindow.length >= maxFailures) {
                this._rpcCircuitOpen = true;
                this._rpcCircuitOpenUntil = now + cooldownMs;
                console.error(
                  `[RPC] Circuit breaker OPEN: ${maxFailures} failures in ${windowMs / 1000}s. Cooldown for ${cooldownMs / 1000}s. Will test recovery after cooldown.`,
                );
                throw new Error(
                  `RPC restart limit exceeded: ${maxFailures} failures in ${windowMs / 1000}s. Circuit breaker open for ${cooldownMs / 1000}s.`,
                );
              }

              this._rpcRestartWindow.push(now);
              const backoffMs = Math.min(1000 * Math.pow(2, this._rpcRestartAttempts), 30000);
              const elapsed = now - this._rpcLastRestartTime;
              if (elapsed < backoffMs) {
                const remaining = backoffMs - elapsed;
                console.log(`[RPC] Backoff active: waiting ${remaining}ms before retry`);
                await new Promise((resolve) => setTimeout(resolve, remaining));
              }

              this._rpcRestartPromise = this.rpcManager
                .restart(this.monero, this.monero._lastWallet)
                .then(() => {
                  this._rpcRestartAttempts = 0;
                  this._rpcLastRestartTime = Date.now();
                  console.log(
                    `[RPC] Restart successful (${this._rpcRestartWindow.length} failures in window)`,
                  );
                })
                .catch((err) => {
                  this._rpcRestartAttempts++;
                  this._rpcLastRestartTime = Date.now();
                  console.error(`[RPC] Restart failed: ${err.message || err}`);
                  throw new Error(`RPC restart failed: ${err.message || err}`);
                })
                .finally(() => {
                  this._rpcRestartPromise = null;
                });
              await this._rpcRestartPromise;
            }
            return await this._moneroRawCall(method, params, timeout);
          }
          throw e;
        }
      });
    };

    this.baseWalletName =
      process.env.MONERO_WALLET_NAME || `znode_${this.wallet.address.slice(2, 10)}`;
    this.clusterWalletName = null;
    this.multisigInfo = null;
    this.clusterId = null;
    this._selfSlashed = false;

    this.p2p = new P2PExchange({
      ethereumAddress: this.wallet.address,
      privateKey: this.wallet.privateKey,
      listenAddr: process.env.P2P_LISTEN_ADDR || '/ip4/0.0.0.0/tcp/9000',
      bootstrapPeers: process.env.P2P_BOOTSTRAP_PEERS || '',
    });

    this.getRoundStatus = async (clusterId, round) => {
      try {
        return await this.exchangeCoordinator.getExchangeRoundStatus(clusterId, round);
      } catch (e) {
        try {
          return await this.exchangeCoordinator.getExchangeRoundStatus(round);
        } catch {
          throw e;
        }
      }
    };
  }

  async _loadClusterBlacklist() {
    try {
      const exists = await fs.promises.access(this._clusterBlacklistPath).then(() => true).catch(() => false);
      if (exists) {
        const data = await fs.promises.readFile(this._clusterBlacklistPath, 'utf8');
        const parsed = JSON.parse(data);
        this._clusterFailures = new Map(Object.entries(parsed.failures || {}));
        this._clusterFailMeta = parsed.meta || {};
        this._clusterBlacklist = parsed.blacklist || {};

        const now = Date.now();
        const { pruned, active } = pruneBlacklistEntries(this._clusterBlacklist, now);
        this._clusterBlacklist = pruned;

        console.log(
          `[Blacklist] Loaded ${this._clusterFailures.size} cluster failure records from disk`,
        );
        if (active > 0) {
          console.log(`[Blacklist] ${active} active blacklisted clusters`);
        }
      }
    } catch (e) {
      console.warn(`[Blacklist] Failed to load from disk: ${e.message}`);
    }
  }

  _saveClusterBlacklist() {
    if (this._blacklistSavePending) {
      this._blacklistSaveQueued = true;
      return;
    }

    this._blacklistSavePending = true;
    this._blacklistSaveQueued = false;

    setImmediate(() => {
      try {
        const now = Date.now();
        const { pruned: prunedBlacklist } = pruneBlacklistEntries(
          this._clusterBlacklist || {},
          now,
        );

        const MAX_FAILURE_RECORDS = 1000;
        const failureEntries = Array.from(this._clusterFailures.entries());
        if (failureEntries.length > MAX_FAILURE_RECORDS) {
          this._clusterFailures = new Map(failureEntries.slice(-MAX_FAILURE_RECORDS));
        }

        const data = {
          failures: Object.fromEntries(this._clusterFailures),
          meta: this._clusterFailMeta,
          blacklist: prunedBlacklist,
          updated: new Date().toISOString(),
        };

        const tmpPath = this._clusterBlacklistPath + '.tmp';
        fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), {
          mode: 0o600,
        });
        fs.renameSync(tmpPath, this._clusterBlacklistPath);
      } catch (e) {
        console.warn(`[Blacklist] Failed to save to disk: ${e.message}`);
      } finally {
        this._blacklistSavePending = false;
        if (this._blacklistSaveQueued) {
          this._saveClusterBlacklist();
        }
      }
    });
  }



  _getClusterStatePath() {
    return path.join(__dirname, '.cluster-state.json');
  }

  _saveClusterState() {
    if (!this._activeClusterId || !this._clusterFinalized) {
      return;
    }
    try {
      const state = {
        clusterId: this._activeClusterId,
        members: this._clusterMembers,
        finalAddress: this._clusterFinalAddress,
        walletName: this.clusterWalletName,
        savedAt: Date.now(),
      };
      const statePath = this._getClusterStatePath();
      const tmpPath = statePath + '.' + Date.now() + '.tmp';
      fs.writeFileSync(tmpPath, JSON.stringify(state, null, 2), { mode: 0o600 });
      fs.renameSync(tmpPath, statePath);
      console.log('[Cluster] Saved cluster state to disk');
    } catch (e) {
      console.log('[Cluster] Failed to save cluster state:', e.message || String(e));
    }
  }

  _clearClusterState() {
    try {
      const statePath = this._getClusterStatePath();
      if (fs.existsSync(statePath)) {
        fs.unlinkSync(statePath);
        console.log('[Cluster] Cleared persisted cluster state');
      }
    } catch (e) {
      console.log('[Cluster] Failed to clear cluster state:', e.message || String(e));
    }
  }

  async _verifyAndRestoreCluster() {
    const statePath = this._getClusterStatePath();
    if (!fs.existsSync(statePath)) {
      return await this._recoverClusterFromChain();
    }
    let state;
    try {
      state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    } catch (e) {
      console.log('[Cluster] Failed to load cluster state:', e.message || String(e));
      return false;
    }
    if (!state.clusterId || !state.members || !state.finalAddress) {
      console.log('[Cluster] Invalid cluster state file');
      this._clearClusterState();
      return false;
    }
    try {
      const clusterInfo = await this.registry.clusters(state.clusterId);
      const onChainAddress = clusterInfo && clusterInfo[0];
      const finalized = clusterInfo && clusterInfo[2];
      const onChainMembers = await this.registry.getClusterMembers(state.clusterId);
      if (!finalized) {
        console.log('[Cluster] Saved cluster is not finalized on-chain, clearing state');
        this._clearClusterState();
        return false;
      }
      if (onChainAddress !== state.finalAddress) {
        console.log('[Cluster] Cluster address mismatch, clearing state');
        this._clearClusterState();
        return false;
      }
      const myAddress = this.wallet.address.toLowerCase();
      const memberAddresses = (onChainMembers || []).map(a => a.toLowerCase());
      if (!memberAddresses.includes(myAddress)) {
        console.log('[Cluster] Not a member of saved cluster, clearing state');
        this._clearClusterState();
        return false;
      }
      this._activeClusterId = state.clusterId;
      this._clusterMembers = state.members;
      this._clusterFinalAddress = state.finalAddress;
      this._clusterFinalized = true;
      this.clusterWalletName = state.walletName;
      if (this.p2p && typeof this.p2p.setActiveCluster === 'function') {
        this.p2p.setActiveCluster(state.clusterId);
      }
      console.log('[Cluster] Restored finalized cluster from on-chain state');
      console.log('  ClusterId: ' + state.clusterId.slice(0, 18) + '...');
      console.log('  Monero address: ' + state.finalAddress.slice(0, 24) + '...');
      console.log('  Members: ' + state.members.length);
      return true;
    } catch (e) {
      console.log('[Cluster] Failed to verify cluster on-chain:', e.message || String(e));
      return false;
    }
  }

  async _recoverClusterFromChain() {
    try {
      console.log('[Cluster] No state file found, checking on-chain for existing cluster membership...');
      const myAddress = this.wallet.address;
      const clusterId = await this.registry.nodeToCluster(myAddress);
      if (!clusterId || clusterId === '0x0000000000000000000000000000000000000000000000000000000000000000') {
        console.log('[Cluster] Node is not in any cluster on-chain');
        return false;
      }
      console.log('[Cluster] Found on-chain cluster:', clusterId.slice(0, 18) + '...');
      const isActive = await this.registry.isClusterActive(clusterId);
      if (!isActive) {
        console.log('[Cluster] On-chain cluster is not active');
        return false;
      }
      const members = await this.registry.getClusterMembers(clusterId);
      const clusterData = await this.registry.clusters(clusterId);
      const moneroAddress = clusterData[0];
      const finalized = clusterData[2];
      if (!finalized) {
        console.log('[Cluster] On-chain cluster is not finalized');
        return false;
      }
      const walletDir =
        process.env.MONERO_WALLET_DIR ||
        (process.env.HOME
          ? path.join(process.env.HOME, '.monero-wallets')
          : path.join(process.cwd(), '.monero-wallets'));
      const shortEth = myAddress.slice(2, 10);
      let walletName = null;
      try {
        const files = fs.readdirSync(walletDir);
        const pattern = new RegExp(`^znode_att_${shortEth}_[a-fA-F0-9]{8}_\d{2}\.keys$`, 'i');
        for (const file of files) {
          if (pattern.test(file)) {
            walletName = file.replace('.keys', '');
            break;
          }
        }
      } catch (e) {
        console.log('[Cluster] Could not scan wallet directory:', e.message || String(e));
      }
      if (!walletName) {
        console.log('[Cluster] No matching wallet file found for recovery');
        return false;
      }
      this._activeClusterId = clusterId;
      this._clusterMembers = Array.from(members);
      this._clusterFinalAddress = moneroAddress;
      this._clusterFinalized = true;
      this.clusterWalletName = walletName;
      if (this.p2p && typeof this.p2p.setActiveCluster === 'function') {
        this.p2p.setActiveCluster(clusterId);
      }
      this._saveClusterState();
      console.log('[Cluster] Recovered finalized cluster from on-chain data');
      console.log('  ClusterId: ' + clusterId.slice(0, 18) + '...');
      console.log('  Monero address: ' + moneroAddress.slice(0, 24) + '...');
      console.log('  Members: ' + members.length);
      console.log('  Wallet: ' + walletName);
      return true;
    } catch (e) {
      console.log('[Cluster] Failed to recover cluster from chain:', e.message || String(e));
      return false;
    }
  }

  async _waitForOnChainFinalizationForSelf(expectedMoneroAddress, fallbackMembers) {
    if (!this.registry || !this.wallet) {
      return false;
    }
    const timeoutRaw = process.env.FINALIZE_ONCHAIN_TIMEOUT_MS;
    const timeoutParsed = timeoutRaw != null ? Number(timeoutRaw) : NaN;
    const timeoutMs =
      Number.isFinite(timeoutParsed) && timeoutParsed > 0 ? timeoutParsed : 5 * 60 * 1000;
    const pollRaw = process.env.FINALIZE_ONCHAIN_POLL_MS;
    const pollParsed = pollRaw != null ? Number(pollRaw) : NaN;
    const pollMs =
      Number.isFinite(pollParsed) && pollParsed > 0 ? pollParsed : 10 * 1000;
    const selfAddress = this.wallet.address;
    const zeroCluster =
      '0x0000000000000000000000000000000000000000000000000000000000000000';
    console.log('[Cluster] Waiting for on-chain finalization (direct poll)');
    const startedAt = Date.now();
    while (Date.now() - startedAt < timeoutMs) {
      try {
        const clusterId = await this.registry.nodeToCluster(selfAddress);
        if (clusterId && clusterId !== zeroCluster) {
          const info = await this.registry.clusters(clusterId);
          const moneroAddress = info && info[0];
          const finalized = info && info[2];
          if (finalized && moneroAddress) {
            if (expectedMoneroAddress && moneroAddress !== expectedMoneroAddress) {
              console.log(
                '[Cluster] On-chain cluster finalized but Monero address mismatch; continuing to poll',
              );
            } else {
              let membersOnChain = null;
              try {
                membersOnChain = await this.registry.getClusterMembers(clusterId);
              } catch (_e) {}
              console.log('[Cluster] Finalization confirmed on-chain (direct poll)');
              this._onClusterFinalized(
                clusterId,
                Array.isArray(membersOnChain) && membersOnChain.length
                  ? membersOnChain
                  : fallbackMembers,
                moneroAddress,
              );
              return true;
            }
          }
        }
      } catch (e) {
        console.log('[Cluster] Finalization poll error:', e.message || String(e));
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
    console.log('[Cluster] Finalization not detected on-chain within timeout window');
    return false;
  }

  _onClusterFinalized(clusterId, members, finalAddress) {
    const effectiveClusterId = clusterId || this._activeClusterId;
    if (!effectiveClusterId) {
      return;
    }
    const wasFinalized = this._clusterFinalized;
    if (Array.isArray(members) && members.length) {
      this._clusterMembers = members;
    }
    if (finalAddress) {
      this._clusterFinalAddress = finalAddress;
    }
    this._activeClusterId = effectiveClusterId;
    this._clusterFinalized = true;
    if (!this._clusterFinalizedAt) {
      this._clusterFinalizedAt = Date.now();
    }
    if (this.p2p && typeof this.p2p.setActiveCluster === 'function') {
      this.p2p.setActiveCluster(effectiveClusterId);
    }
    if (typeof this._saveClusterState === 'function') {
      this._saveClusterState();
    }
    if (wasFinalized && this._clusterFinalizedAt) {
      return;
    }
    console.log('[OK] Cluster finalized on-chain');
    try {
      this.startDepositMonitor();
    } catch (e) {
      console.log('[Bridge] Failed to start deposit monitor:', e.message || String(e));
    }
    try {
      this.startBridgeAPI();
    } catch (e) {
      console.log('[BridgeAPI] Failed to start API server:', e.message || String(e));
    }
    try {
      this.startWithdrawalMonitor();
    } catch (e) {
      console.log('[Withdrawal] Failed to start withdrawal monitor:', e.message || String(e));
    }
    if (this.stateMachine && typeof this.stateMachine.transition === 'function') {
      this.stateMachine.transition('ACTIVE', { clusterId: effectiveClusterId }, 'cluster finalized');
    }
  }


  async _withRetry(fn, options = {}) {
    return withRetry(fn, {
      ...options,
      cleanup: async () => {
        if (this._activeClusterId) {
          await this._cleanupClusterAttempt(this._activeClusterId);
        }
      },
    });
  }

  async _submitRoundExchangeInfo(clusterId, round, clusterNodes, payloads) {
    if (!this.exchangeCoordinator) return true;
    if (DRY_RUN) {
      console.log(`[RoundBarrier] [DRY_RUN] Skipping submitExchangeInfo for round ${round}`);
      return true;
    }

    try {
      const me = this.wallet.address.toLowerCase();
      const membersLower = (clusterNodes || []).map((a) => (a || '').toLowerCase());
      if (!membersLower.includes(me)) {
        console.log(
          `[RoundBarrier] [WARN]  Local membership check failed: ${this.wallet.address} not in clusterNodes for R${round}`,
        );
        return false;
      }
      try {
        const prev = await this.exchangeCoordinator.multisigExchangeData(
          clusterId,
          round,
          this.wallet.address,
        );
        if (prev && typeof prev === 'string' && prev.length > 0) {
          console.log(
            `[RoundBarrier] Round ${round} exchange info already present on-chain; continuing`,
          );
          return true;
        }
      } catch (_ignored) {}
    } catch (_ignored) {}

    const digest = computeRoundDigest(payloads);
    const info = {
      type: 'round_complete',
      node: this.wallet.address,
      round,
      peerCount: Array.isArray(payloads) ? payloads.length : 0,
      digest,
    };
    let exchangeInfo;
    try {
      exchangeInfo = JSON.stringify(info);
    } catch {
      exchangeInfo = `round_complete:${round}:${info.peerCount}:${digest || '0x0'}`;
    }

    try {
      if (typeof this.exchangeCoordinator.submitExchangeInfo.staticCall === 'function') {
        await this.exchangeCoordinator.submitExchangeInfo.staticCall(
          clusterId,
          round,
          exchangeInfo,
          clusterNodes,
        );
      } else if (
        this.exchangeCoordinator.callStatic &&
        this.exchangeCoordinator.callStatic.submitExchangeInfo
      ) {
        await this.exchangeCoordinator.callStatic.submitExchangeInfo(
          clusterId,
          round,
          exchangeInfo,
          clusterNodes,
        );
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (/already submitted|duplicate|already.*submitted/i.test(msg)) {
        console.log(
          `[RoundBarrier] Round ${round} exchange info already submitted (preflight); continuing`,
        );
        return true;
      }
      console.log(`[RoundBarrier] Preflight revert for R${round}: ${msg}`);
      return false;
    }

    let gasLimit = null;
    try {
      const est = await this.exchangeCoordinator.estimateGas.submitExchangeInfo(
        clusterId,
        round,
        exchangeInfo,
        clusterNodes,
      );

      gasLimit = (est * 12n) / 10n;
    } catch (_ignored) {}

    const fee = await this.provider.getFeeData();

    const twoGwei = ethers.parseUnits('2', 'gwei');
    const base = fee.maxFeePerGas ?? fee.gasPrice ?? ethers.parseUnits('5', 'gwei');
    const prio = fee.maxPriorityFeePerGas ?? twoGwei;
    const maxFee = base * 2n + prio;

    const retries = Number(process.env.ROUND_SUBMIT_RETRIES || 1);
    let attempt = 0;
    while (true) {
      try {
        const overrides = { maxFeePerGas: maxFee, maxPriorityFeePerGas: prio };
        if (gasLimit) overrides.gasLimit = gasLimit;
        const tx = await this.exchangeCoordinator.submitExchangeInfo(
          clusterId,
          round,
          exchangeInfo,
          clusterNodes,
          overrides,
        );
        console.log(`[RoundBarrier] submitExchangeInfo(R${round}) sent: ${tx.hash}`);
        await tx.wait();
        console.log(`[RoundBarrier] submitExchangeInfo(R${round}) confirmed`);
        return true;
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (/already submitted|duplicate|already.*submitted/i.test(msg)) {
          console.log(
            `[RoundBarrier] Round ${round} exchange info already submitted on-chain; continuing`,
          );
          return true;
        }

        const retryable =
          /timeout|replacement|underpriced|nonce|network|rate|temporar|EHOSTUNREACH|ECONN|ETIMEDOUT/i.test(
            msg,
          );
        if (attempt < retries && retryable) {
          attempt++;
          const backoff = 500 + Math.floor(Math.random() * 500);
          console.log(
            `[RoundBarrier] send retry ${attempt}/${retries} for R${round} after ${backoff}ms: ${msg}`,
          );
          await new Promise((r) => setTimeout(r, backoff));
          continue;
        }
        console.log(`[RoundBarrier] submitExchangeInfo error for round ${round}: ${msg}`);
        return false;
      }
    }
  }

  async _waitForOnChainRoundBarrier(clusterId, round, clusterNodes, timeoutMs) {
    if (!this.exchangeCoordinator) {
      return true;
    }
    if (DRY_RUN) {
      console.log(`[RoundBarrier] [DRY_RUN] Skipping on-chain barrier wait for round ${round}`);
      return true;
    }

    const expected = Array.isArray(clusterNodes) ? clusterNodes.length : 0;
    const start = Date.now();
    const noTimeout = !Number.isFinite(timeoutMs) || timeoutMs <= 0;
    const pollMs = 8000;
    console.log(
      `[RoundBarrier] Waiting for on-chain round ${round} completion (0/${expected})... timeoutMs=${noTimeout ? 'infinite' : timeoutMs}`,
    );

    while (true) {
      let complete = false;
      let submitted = 0;
      try {
        const status = await this.getRoundStatus(clusterId, round);
        if (Array.isArray(status)) {
          [complete, submitted] = status;
        } else if (status && typeof status === 'object') {
          complete = !!status.complete;
          submitted = Number(status.submitted || 0);
        }
      } catch (e) {
        console.log(
          `[RoundBarrier] getExchangeRoundStatus error for round ${round}:`,
          e.message || String(e),
        );
        return false;
      }

      if (complete || (expected > 0 && submitted >= expected)) {
        console.log(
          `[RoundBarrier] Round ${round} on-chain complete: ${submitted}/${expected} nodes`,
        );
        return true;
      }

      const elapsed = Date.now() - start;
      if (!noTimeout && elapsed >= timeoutMs) {
        console.log(
          `[RoundBarrier] Round ${round} on-chain barrier timeout: ${submitted}/${expected} nodes`,
        );

        try {
          const info = await this.exchangeCoordinator.getExchangeRoundInfo(
            clusterId,
            round,
            clusterNodes,
          );
          const missing = [];
          for (let i = 0; i < clusterNodes.length; i++) {
            const exInfo = info.exchangeInfos ? info.exchangeInfos[i] : info[1] ? info[1][i] : '';
            if (!exInfo || exInfo === '') {
              missing.push(clusterNodes[i]);
            }
          }
          if (missing.length > 0) {
            console.log(`[RoundBarrier] Missing on-chain R${round} from: ${missing.join(', ')}`);
          }
        } catch (e) {
          console.log(`[RoundBarrier] Could not identify missing nodes: ${e.message || e}`);
        }
        return false;
      }

      if (elapsed % 30000 < pollMs) {
        console.log(
          `[RoundBarrier] Round ${round} on-chain progress: ${submitted}/${expected} nodes`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }

  async _cleanupClusterAttempt(clusterId) {
    if (!this.p2p) return;

    try {
      if (typeof this.p2p.leaveCluster === 'function') {
        await this.p2p.leaveCluster(clusterId);
      }
    } catch (e) {
      console.log('[WARN]  P2P leaveCluster error during cleanup:', e.message || String(e));
    }

    try {
      if (typeof this.p2p.cleanupOldMessages === 'function') {
        this.p2p.cleanupOldMessages();
      }
    } catch (e) {
      console.log('[WARN]  P2P cleanupOldMessages error during cleanup:', e.message || String(e));
    }

    try {
      if (typeof this.p2p.clearClusterRounds === 'function') {
        this.p2p.clearClusterRounds(clusterId, null, []);
      }
    } catch (e) {
      console.log('[WARN]  P2P clearClusterRounds error during cleanup:', e.message || String(e));
    }

    try {
      if (typeof this.p2p.clearClusterSignatures === 'function') {
        this.p2p.clearClusterSignatures(clusterId);
      }
    } catch (e) {
      console.log(
        '[WARN]  P2P clearClusterSignatures error during cleanup:',
        e.message || String(e),
      );
    }

    this._pendingR3 = null;
  }

  async start() {
    console.log('\n═══════════════════════════════════════════════');
    console.log('   ZNode - Monero Multisig (WORKING!)');
    console.log('═══════════════════════════════════════════════\n');
    console.log(`Address: ${this.wallet.address}`);

    await this._loadClusterBlacklist();

    const network = await this.provider.getNetwork();
    const chainIdBigInt = network.chainId;

    if (chainIdBigInt > Number.MAX_SAFE_INTEGER) {
      throw new Error(
        `Chain ID ${chainIdBigInt} exceeds Number.MAX_SAFE_INTEGER. This chain is not supported.`,
      );
    }

    this.chainId = Number(chainIdBigInt);
    console.log(`Network: ${network.name} (chainId: ${this.chainId})`);

    if (process.env.CHAIN_ID) {
      const envChainId = Number(process.env.CHAIN_ID);
      if (isNaN(envChainId) || envChainId < 1) {
        throw new Error(`Invalid CHAIN_ID in environment: ${process.env.CHAIN_ID}`);
      }
      if (envChainId !== this.chainId) {
        throw new Error(
          `CHAIN_ID mismatch: env=${envChainId}, provider=${this.chainId}. Please ensure CHAIN_ID matches your RPC endpoint's network.`,
        );
      }
      console.log(`[OK] CHAIN_ID validated: ${this.chainId}\n`);
    } else {
      console.log(`[WARN]  CHAIN_ID not set, using ${this.chainId} from provider\n`);
      process.env.CHAIN_ID = String(this.chainId);
    }

    const defaults = this._knownDefaults[this.chainId];
    if (!process.env.REGISTRY_ADDR && !defaults) {
      throw new Error(
        `No default contract addresses for chainId ${this.chainId}. Please set REGISTRY_ADDR, STAKING_ADDR, ZFI_ADDR, and COORDINATOR_ADDR explicitly.`,
      );
    }
    this._chainIdVerified = true;

    const REGISTRY_ADDR = process.env.REGISTRY_ADDR || defaults.REGISTRY_ADDR;
    const STAKING_ADDR = process.env.STAKING_ADDR || defaults.STAKING_ADDR;
    const ZFI_ADDR = process.env.ZFI_ADDR || defaults.ZFI_ADDR;
    const COORDINATOR_ADDR = process.env.COORDINATOR_ADDR || defaults.COORDINATOR_ADDR;

    console.log(`Contract Addresses:`);
    console.log(`  Registry: ${REGISTRY_ADDR}`);
    console.log(`  Staking: ${STAKING_ADDR}`);
    console.log(`  ZFI: ${ZFI_ADDR}`);
    console.log(`  Coordinator: ${COORDINATOR_ADDR}\n`);

    this.registry = new ethers.Contract(REGISTRY_ADDR, this._registryABI, this.wallet);
    this.staking = new ethers.Contract(STAKING_ADDR, this._stakingABI, this.wallet);
    this.zfi = new ethers.Contract(ZFI_ADDR, this._zfiABI, this.wallet);
    this.exchangeCoordinator = new ethers.Contract(
      COORDINATOR_ADDR,
      this._exchangeCoordinatorABI,
      this.wallet,
    );

    const BRIDGE_ADDR = process.env.BRIDGE_ADDR || (defaults ? defaults.BRIDGE_ADDR : null);
    if (BRIDGE_ADDR && BRIDGE_ADDR !== ethers.ZeroAddress) {
      this.bridge = new ethers.Contract(BRIDGE_ADDR, this._bridgeABI, this.wallet);
      console.log(`  Bridge: ${BRIDGE_ADDR}`);
    } else {
      console.log('  Bridge: not configured (BRIDGE_ADDR not set)');
    }

    if (this.p2p && typeof this.p2p.setHeartbeatDomain === 'function') {
      try {
        console.log(
          `[Heartbeat] Configuring P2P heartbeat domain with chainId: ${this.chainId}, stakingAddr: ${STAKING_ADDR}`,
        );
        await this.p2p.setHeartbeatDomain(this.chainId, STAKING_ADDR);
      } catch (e) {
        console.warn('[WARN]  Failed to configure P2P heartbeat domain:', e.message || String(e));
      }
    }

    try {
      await this.checkRequirements();
      await this.setupMonero();
      await this.startP2P();
      this.startHeartbeatLoop();
      try {
        this.rpcManager.startHealthWatch(this.monero);
      } catch (e) {
        console.warn('Failed to start RPC health watch:', e.message || String(e));
      }

      await this.ensureRegistered();
      const restoredCluster = await this._verifyAndRestoreCluster();
      if (restoredCluster && this.clusterWalletName) {
        try {
          await this.monero.openWallet(this.clusterWalletName, this.moneroPassword);
          console.log('[Cluster] Opened cluster wallet: ' + this.clusterWalletName);
        } catch (e) {
          console.log('[Cluster] Failed to open cluster wallet:', e.message || String(e));
          this._activeClusterId = null;
          this._clusterMembers = null;
          this._clusterFinalAddress = null;
          this._clusterFinalized = false;
          this._clearClusterState();
        }
      }
      if (this._clusterFinalized) {
        console.log('[Cluster] Resuming active cluster from restored state');
        if (typeof this._onClusterFinalized === 'function') {
          this._onClusterFinalized(this._activeClusterId, this._clusterMembers, this._clusterFinalAddress);
        }
      } else {
        await this.monitorNetwork();
      }
      this.startSlashingLoop();
    } catch (e) {
      console.error('\n[ERROR] Startup failed:', e.message || String(e));
      console.error('Cleaning up...');
      await this.stop();
      throw e;
    }
  }

  async stop() {
    try {
      try {
        if (this.registry && this.wallet && this.registry.nodes) {
          const info = await this.registry.nodes(this.wallet.address);
          let registered = false;
          if (info && info.registered !== undefined) {
            registered = !!info.registered;
          } else if (Array.isArray(info) && info.length >= 2) {
            registered = !!info[1];
          }
          if (registered) {
            console.log('[INFO] Unregistering from network (shutdown)...');
            if (DRY_RUN) {
              console.log('[DRY_RUN] Would send unregisterNode transaction');
            } else {
              try {
                const tx = await this.registry.unregisterNode();
                await tx.wait();
                console.log('[OK] Unregistered from network');
              } catch (e) {
                console.log('Unregister on shutdown failed:', e.message || String(e));
              }
            }
          }
        }
      } catch (e) {
        console.log('Deregistration status check failed:', e.message || String(e));
      }

      if (this.p2p && typeof this.p2p.stop === 'function') {
        try {
          await this.p2p.stop();
        } catch (e) {
          console.log('P2P stop error:', e.message || String(e));
        }
      }
      if (this.rpcManager && this.rpcManager._timer) {
        clearInterval(this.rpcManager._timer);
        this.rpcManager._timer = null;
      }
      if (this._heartbeatTimer) {
        clearInterval(this._heartbeatTimer);
        this._heartbeatTimer = null;
      }
      if (this._monitorTimer) {
        clearInterval(this._monitorTimer);
        this._monitorTimer = null;
      }
      console.log('ZNode stopped.');
    } catch (e) {
      console.log('Error during shutdown:', e.message || String(e));
    }
  }

  async checkRequirements() {
    console.log('[INFO] Checking requirements...');

    const network = await this.provider.getNetwork();
    console.log(`  Network: ${network.name} (chainId: ${network.chainId})`);

    const ethBalance = await this.provider.getBalance(this.wallet.address);
    if (ethBalance < ethers.parseEther('0.001')) {
      throw new Error('Insufficient ETH for gas (need >= 0.001 ETH)');
    }

    let zfiDecimals;
    try {
      zfiDecimals = await this.zfi.decimals();
      console.log(`  ZFI Decimals: ${zfiDecimals}`);
    } catch (e) {
      if (!TEST_MODE) {
        throw new Error(
          `Failed to read ZFI decimals from contract. This is required in production mode: ${e.message}`,
        );
      }
      console.warn(
        `[WARN]  Could not read ZFI decimals, assuming 18 (TEST_MODE only): ${e.message}`,
      );
      zfiDecimals = 18;
    }

    const zfiBal = await this.zfi.balanceOf(this.wallet.address);
    console.log(`  ZFI Balance: ${ethers.formatUnits(zfiBal, zfiDecimals)}`);

    let stakedAmt = 0n;
    let slashingStage = 0n;
    try {
      const info = await this.staking.getNodeInfo(this.wallet.address);
      if (!Array.isArray(info) || info.length < 7) {
        console.warn('[WARN]  Unexpected getNodeInfo response format, treating as not staked');
        stakedAmt = 0n;
      } else {
        stakedAmt = info[0];
        slashingStage = info[5];
      }
    } catch {
      stakedAmt = 0n;
    }
    console.log(`  ZFI Staked: ${ethers.formatUnits(stakedAmt, zfiDecimals)}`);

    let isBlacklisted = false;
    try {
      isBlacklisted = await this.staking.isBlacklisted(this.wallet.address);
    } catch (e) {
      console.warn(
        '[WARN]  Could not read blacklist status from staking contract:',
        e.message || String(e),
      );
    }
    if (isBlacklisted) {
      throw new Error(
        'This address has been blacklisted due to slashing. It cannot participate as a node.',
      );
    }

    try {
      const res = await this.staking.checkSlashingStatus(this.wallet.address);
      if (Array.isArray(res) && res.length >= 3) {
        const needsSlash = !!res[0];
        const stage = res[1];
        const hoursOfflinePending = res[2];
        if (needsSlash) {
          console.warn(
            `[WARN]  checkSlashingStatus reports pending downtime slash (stage ${stage}, offline ${hoursOfflinePending} hours). Network participants may submit a slashing tx using signed heartbeats.`,
          );
        }
      }
    } catch (e) {
      console.warn(
        '[WARN]  Could not read slashing status from staking contract:',
        e.message || String(e),
      );
    }

    const required = ethers.parseUnits('1000000', zfiDecimals);

    if (stakedAmt >= required) {
      console.log('  Stake already at or above required amount.');
    } else {
      const stakingAddr = this.staking.target || this.staking.address;
      let amountNeeded;
      let actionLabel;

      if (stakedAmt > 0n) {
        amountNeeded = required - stakedAmt;
        actionLabel = 'top up existing stake';
        console.log(
          `  Detected partial stake. Need to top up by ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI to reach 1,000,000.`,
        );
      } else {
        amountNeeded = required;
        actionLabel = 'initial stake';
        console.log('  No existing stake found; will stake full 1,000,000 ZFI.');
      }

      if (zfiBal < amountNeeded) {
        const slashingStageNum = Number(slashingStage || 0n);
        if (slashingStageNum > 0) {
          throw new Error(
            `This node has been partially slashed (stage ${slashingStageNum}) and does not have enough ZFI to restore the full 1,000,000 ZFI stake. Missing ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI.`,
          );
        }
        throw new Error(
          `Insufficient ZFI to ${actionLabel} by ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI`,
        );
      }

      const allowance = await this.zfi.allowance(this.wallet.address, stakingAddr);
      if (allowance < amountNeeded) {
        const TEST_MODE = process.env.TEST_MODE === '1';
        const approvalMultiplier = TEST_MODE ? Number(process.env.APPROVAL_MULTIPLIER || '1') : 1;

        if (approvalMultiplier !== 1 && !TEST_MODE) {
          console.warn('[WARN]  APPROVAL_MULTIPLIER ignored in production mode (using 1x)');
        }

        const approvalAmount = amountNeeded * BigInt(approvalMultiplier);

        console.log('  Approving ZFI for staking...');
        console.log(
          `  [INFO] approve(${stakingAddr}, ${ethers.formatUnits(approvalAmount, zfiDecimals)} ZFI)`,
        );
        if (approvalMultiplier > 1) {
          console.log(`  [INFO]  TEST_MODE: Approving ${approvalMultiplier}x required amount`);
        }

        if (DRY_RUN) {
          console.log('  [DRY_RUN] Would send approve transaction');
        } else {
          const txA = await this.zfi.approve(stakingAddr, approvalAmount);
          await txA.wait();
          console.log('  [OK] Approved');
        }
      }

      if (stakedAmt === 0n) {
        const moneroFeeAddr = process.env.MONERO_FEE_ADDRESS;
        if (!moneroFeeAddr) {
          throw new Error(
            'MONERO_FEE_ADDRESS is required for staking. Set this to your Monero address for receiving fees.',
          );
        }

        console.log('  Staking 1,000,000 ZFI...');
        const codeHash = ethers.id('znode-v3');
        console.log(`  [INFO] stake(${codeHash}, ${moneroFeeAddr})`);
        if (DRY_RUN) {
          console.log('  [DRY_RUN] Would send stake transaction');
        } else {
          const txS = await this.staking.stake(codeHash, moneroFeeAddr);
          await txS.wait();
          console.log('  [OK] Staked');
        }
      } else {
        console.log('  Topping up existing stake to 1,000,000 ZFI...');
        console.log('  [INFO] topUpStake()');
        if (DRY_RUN) {
          console.log('  [DRY_RUN] Would send topUpStake transaction');
        } else {
          const txT = await this.staking.topUpStake();
          await txT.wait();
          console.log('  [OK] Top-up complete');
        }
      }
    }

    console.log('[OK] Requirements met\n');
  }

  async _checkSelfStakeHealth() {
    return checkSelfStakeHealth(this);
  }

  async setupMonero() {
    console.log('[INFO] Setting up Monero with multisig support...');

    try {
      const restoreScript = path.join(__dirname, 'scripts', 'auto-restore-wallet.sh');
      if (fs.existsSync(restoreScript)) {
        try {
          fs.accessSync(restoreScript, fs.constants.X_OK);
        } catch {
          console.warn('[WARN]  auto-restore-wallet.sh exists but is not executable');
        }
        await execFileAsync(restoreScript, [], {
          cwd: __dirname,
          timeout: 30000,
        });
      }
    } catch (restoreErr) {
      const msg = restoreErr.message || String(restoreErr);
      if (!/exit code 0/i.test(msg)) {
        console.warn('[WARN]  Wallet restore script failed:', msg);
      }
    }

    for (let i = 1; i <= 10; i++) {
      try {
        await this.monero.openWallet(this.baseWalletName, this.moneroPassword);
        console.log(`[OK] Base wallet opened: ${this.baseWalletName}`);
        break;
      } catch (error) {
        if (error.code === 'ECONNREFUSED' && i < 10) {
          console.log(`  Waiting for Monero RPC (attempt ${i}/10)...`);
          await new Promise((r) => setTimeout(r, 3000));
          continue;
        }

        if (error.code === 'ECONNREFUSED') {
          throw new Error('Monero RPC not available');
        }

        console.log('  Creating wallet with password...');
        try {
          await this.monero.createWallet(this.baseWalletName, this.moneroPassword);
          console.log(`[OK] Base wallet created: ${this.baseWalletName}`);

          try {
            const backupScript = path.join(__dirname, 'scripts', 'auto-backup-wallet.sh');
            if (fs.existsSync(backupScript)) {
              try {
                fs.accessSync(backupScript, fs.constants.X_OK);
              } catch {
                throw new Error('auto-backup-wallet.sh exists but is not executable');
              }

              let backupPass = process.env.WALLET_BACKUP_PASSPHRASE;
              try {
                const homeDir = process.env.HOME || process.cwd();
                const defaultPassFile = path.join(
                  homeDir,
                  '.znode-backup',
                  'wallet_backup_passphrase.txt',
                );
                const passFile = process.env.WALLET_BACKUP_PASSPHRASE_FILE || defaultPassFile;
                const passDir = path.dirname(passFile);
                try {
                  fs.mkdirSync(passDir, { recursive: true, mode: 0o700 });
                } catch (_ignored) {}

                if (!backupPass) {
                  if (fs.existsSync(passFile)) {
                    backupPass = fs.readFileSync(passFile, 'utf8').trim();
                    if (!backupPass) {
                      console.warn(
                        '[WARN]  Wallet backup passphrase file is empty, regenerating a new one',
                      );
                    } else {
                      console.log('[OK] Loaded existing wallet backup passphrase from disk');
                    }
                  }
                }

                if (!backupPass) {
                  backupPass = crypto.randomBytes(32).toString('hex');
                  fs.writeFileSync(passFile, backupPass + '\n', {
                    mode: 0o600,
                  });
                  console.log(
                    `[OK] Generated new wallet backup passphrase and saved to ${passFile}`,
                  );
                }
              } catch (passErr) {
                console.warn(
                  '[WARN]  Failed to initialize wallet backup passphrase:',
                  passErr.message || String(passErr),
                );
              }

              const env = { ...process.env };
              if (backupPass) {
                env.WALLET_BACKUP_PASSPHRASE = backupPass;
              }

              await execFileAsync(backupScript, [], {
                cwd: __dirname,
                timeout: 30000,
                env,
              });
              console.log('[OK] Base wallet backed up using auto-backup-wallet.sh');
            } else {
              console.log('[WARN]  auto-backup-wallet.sh not found, skipping automatic backup');
            }
          } catch (backupErr) {
            console.log(
              '[WARN]  Base wallet backup failed:',
              backupErr.message || String(backupErr),
            );
          }
        } catch (e2) {
          const msg = e2 && e2.message ? e2.message : String(e2);

          if (/already exists/i.test(msg) || /timeout/i.test(msg)) {
            console.log('  Create failed or timed out; attempting to open...');
            try {
              await this.monero.openWallet(this.baseWalletName, this.moneroPassword);
              console.log(`[OK] Base wallet opened: ${this.baseWalletName}`);
            } catch (e3) {
              const openMsg = e3 && e3.message ? e3.message : String(e3);
              console.error(`  [ERROR] Wallet exists but cannot open: ${openMsg}`);
              console.error(`  [INFO]  Run ./clean-restart to delete old wallet files and retry`);
              throw new Error(
                `Wallet ${this.baseWalletName} exists but cannot be opened. Run ./clean-restart to reset.`,
              );
            }
          } else {
            throw e2;
          }
        }
        break;
      }
    }

    await this._autoEnableMoneroMultisigExperimental();
  }

  async _enableMoneroMultisigExperimentalForWallet(walletFile, label) {
    const walletDir =
      process.env.MONERO_WALLET_DIR ||
      (process.env.HOME
        ? path.join(process.env.HOME, '.monero-wallets')
        : path.join(process.cwd(), '.monero-wallets'));
    const cliPath = process.env.MONERO_WALLET_CLI || 'monero-wallet-cli';

    if (!walletFile || !this.moneroPassword) {
      return false;
    }

    const display = label || `wallet ${walletFile}`;
    console.log(`  Ensuring Monero multisig experimental mode is enabled for ${display}...`);

    try {
      await this._walletMutex.withLock(async () => {
        try {
          await this._moneroRawCall('close_wallet', {}, 180000);
        } catch (_ignored) {}
      });
    } catch (_ignored) {}

    const lines = ['set enable-multisig-experimental 1', this.moneroPassword, 'set', 'exit'];
    const input = lines.join('\n') + '\n';

    let ok = true;
    try {
      const args = [
        '--wallet-file',
        walletFile,
        '--password',
        this.moneroPassword,
        '--daemon-address',
        process.env.MONERO_DAEMON_ADDRESS || 'xmr-node.cakewallet.com:18081',
        '--offline',
      ];

      const { stdout, stderr, exitCode } = await new Promise((resolve) => {
        const child = spawn(cliPath, args, { cwd: walletDir });

        let stdout = '';
        let stderr = '';

        child.stdout.on('data', (d) => {
          stdout += d.toString();
        });
        child.stderr.on('data', (d) => {
          stderr += d.toString();
        });

        child.on('error', (err) => {
          console.log('  [WARN] Failed to start monero-wallet-cli:', err.message || String(err));
          resolve({
            stdout,
            stderr: (stderr || '') + String(err),
            exitCode: -1,
          });
        });

        child.on('close', (code) => {
          resolve({ stdout, stderr, exitCode: code });
        });

        child.stdin.write(input);
        child.stdin.end();
      });

      const out = (stdout || '') + (stderr || '');
      const hasFatalError =
        out.includes('Error: invalid password') ||
        out.includes('Error: Unknown command') ||
        out.includes('failed to load wallet') ||
        out.includes('is opened by another wallet program') ||
        out.includes('Key file not found');

      if (hasFatalError || exitCode !== 0) {
        ok = false;
        console.log(
          '  [WARN] monero-wallet-cli reported an error while enabling multisig experimental mode:',
        );
        console.log(out || `(no output, exit code ${exitCode})`);
      } else {
        console.log(
          `  [OK] Enabled multisig experimental mode for wallet ${walletFile} using ${cliPath}`,
        );
      }
    } catch (e) {
      ok = false;
      console.log(
        '  [WARN] Failed to auto-enable multisig experimental mode via monero-wallet-cli:',
        e.message || String(e),
      );
    }

    try {
      await this._walletMutex.withLock(async () => {
        try {
          await this._moneroRawCall(
            'open_wallet',
            { filename: walletFile, password: this.moneroPassword },
            180000,
          );
        } catch (e) {
          console.log(
            '  [WARN] Failed to reopen wallet after multisig flag attempt:',
            e.message || String(e),
          );
        }
      });
    } catch (e) {
      console.log(
        '  [WARN] Wallet mutex error while reopening wallet after multisig flag attempt:',
        e.message || String(e),
      );
    }

    return ok;
  }

  _resetMoneroHealth() {
    this.moneroHealth = MoneroHealth.HEALTHY;
  }

  _ensureMoneroErrorCounts() {
    if (!this.moneroErrorCounts) {
      this.moneroErrorCounts = {
        kexRoundMismatch: 0,
        kexTimeout: 0,
        rpcFailure: 0,
      };
    }
  }

  _noteMoneroKexRoundMismatch(round, msg) {
    this._ensureMoneroErrorCounts();
    this.moneroErrorCounts.kexRoundMismatch = (this.moneroErrorCounts.kexRoundMismatch || 0) + 1;
    const count = this.moneroErrorCounts.kexRoundMismatch;
    const threshold = Number(process.env.MONERO_KEX_MISMATCH_QUARANTINE_THRESHOLD || 3);
    console.log(
      `[MoneroHealth] KEX round mismatch in round ${round}; count=${count}/${threshold}: ${msg}`,
    );
    if (this.moneroHealth !== MoneroHealth.QUARANTINED) {
      this.moneroHealth = MoneroHealth.NEEDS_GLOBAL_RESET;
    }
    if (count >= threshold && this.moneroHealth !== MoneroHealth.QUARANTINED) {
      console.log(
        '[MoneroHealth] KEX mismatch threshold exceeded; node will self-quarantine from new cluster attempts.',
      );
      this.moneroHealth = MoneroHealth.QUARANTINED;
    }
  }

  _noteMoneroRpcFailure(kind, msg) {
    this._ensureMoneroErrorCounts();
    this.moneroErrorCounts.rpcFailure = (this.moneroErrorCounts.rpcFailure || 0) + 1;
    console.log(`[MoneroHealth] Monero RPC failure (${kind}): ${msg}`);
    if (this.moneroHealth === MoneroHealth.HEALTHY) {
      this.moneroHealth = MoneroHealth.NEEDS_ATTEMPT_RESET;
    }
  }

  _noteMoneroKexTimeout(round) {
    this._ensureMoneroErrorCounts();
    this.moneroErrorCounts.kexTimeout = (this.moneroErrorCounts.kexTimeout || 0) + 1;
    console.log(
      `[MoneroHealth] KEX timeout at round ${round}; count=${this.moneroErrorCounts.kexTimeout}`,
    );
  }

  _computeAttemptWalletName(clusterId, attempt) {
    const shortEth = this.wallet.address.slice(2, 10);
    const clusterPart = (clusterId || '').toString().slice(2, 10) || 'noclust';
    const attemptPart = String(attempt || 1).padStart(2, '0');
    return `znode_att_${shortEth}_${clusterPart}_${attemptPart}`;
  }

  async _deleteWalletFiles(walletFile) {
    if (!walletFile) return;
    const walletDir =
      process.env.MONERO_WALLET_DIR ||
      (process.env.HOME
        ? path.join(process.env.HOME, '.monero-wallets')
        : path.join(process.cwd(), '.monero-wallets'));
    const walletPath = path.join(walletDir, walletFile);
    const keysPath = `${walletPath}.keys`;
    try {
      await fs.promises.unlink(walletPath);
      console.log(`[MoneroAttempt] Deleted wallet file: ${walletFile}`);
    } catch (e) {
      if (e && e.code !== 'ENOENT') {
        console.log(
          `[MoneroAttempt] Failed to delete wallet file ${walletFile}:`,
          e.message || String(e),
        );
      }
    }
    try {
      await fs.promises.unlink(keysPath);
      console.log(`[MoneroAttempt] Deleted wallet keys file: ${walletFile}.keys`);
    } catch (e) {
      if (e && e.code !== 'ENOENT') {
        console.log(
          `[MoneroAttempt] Failed to delete wallet keys file ${walletFile}.keys:`,
          e.message || String(e),
        );
      }
    }
  }

  async _preflightMoneroForClusterAttempt(clusterId, attempt) {
    if (!this.moneroHealth) {
      this.moneroHealth = MoneroHealth.HEALTHY;
    }

    if (this.moneroHealth === MoneroHealth.QUARANTINED) {
      console.log('[MoneroHealth] Node is QUARANTINED; refusing to start new multisig attempt.');
      return false;
    }

    if (this.moneroHealth === MoneroHealth.NEEDS_GLOBAL_RESET) {
      console.log(
        '[MoneroHealth] Global Monero reset required before new cluster attempt (from previous error).',
      );
      try {
        await this._hardResetMoneroWalletState('monero health = NEEDS_GLOBAL_RESET pre-attempt');
      } catch (e) {
        console.log('[MoneroHealth] Hard reset before attempt failed:', e.message || String(e));
        return false;
      }
      this._resetMoneroHealth();
    } else if (
      this.moneroHealth === MoneroHealth.NEEDS_ATTEMPT_RESET &&
      this.currentAttemptWallet
    ) {
      console.log('[MoneroHealth] Cleaning up prior attempt wallet before new cluster attempt.');
      await this._deleteWalletFiles(this.currentAttemptWallet);
      this.currentAttemptWallet = null;
      this._resetMoneroHealth();
    }

    const attemptWallet = this._computeAttemptWalletName(clusterId, attempt);
    this.currentAttemptWallet = attemptWallet;
    console.log(
      `[MoneroAttempt] Preparing fresh attempt wallet for cluster=${clusterId} attempt=${attempt}: ${attemptWallet}`,
    );

    try {
      try {
        await this.monero.closeWallet();
      } catch (_ignored) {}
      await this.monero.createWallet(attemptWallet, this.moneroPassword);
      console.log(`[MoneroAttempt] Created attempt wallet: ${attemptWallet}`);
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (/already exists/i.test(msg)) {
        console.log(
          `[MoneroAttempt] Attempt wallet already exists; reusing existing files: ${attemptWallet}`,
        );
      } else {
        console.log('[MoneroAttempt] Failed to create attempt wallet:', msg);
        throw e;
      }
    }

    try {
      await this.monero.openWallet(attemptWallet, this.moneroPassword);
      console.log(`[MoneroAttempt] Opened attempt wallet: ${attemptWallet}`);
    } catch (e) {
      console.log(
        '[MoneroAttempt] Failed to open attempt wallet after create:',
        e.message || String(e),
      );
      throw e;
    }

    try {
      await this._enableMoneroMultisigExperimentalForWallet(
        attemptWallet,
        `cluster attempt wallet ${attemptWallet}`,
      );
    } catch (e) {
      console.log(
        '[MoneroAttempt] Failed to enable multisig experimental mode on attempt wallet:',
        e.message || String(e),
      );
      return false;
    }

    this.multisigInfo = null;
    this._pendingR3 = null;

    return true;
  }

  async _postAttemptMoneroCleanup(clusterId, attempt, success) {
    if (success) {
      this._resetMoneroHealth();
      this.currentAttemptWallet = null;
      return;
    }

    if (this.currentAttemptWallet) {
      console.log(
        `[MoneroAttempt] Cleaning up failed attempt wallet for cluster=${clusterId}: ${this.currentAttemptWallet}`,
      );
      try {
        await this.monero.closeWallet();
      } catch (_ignored) {}
      await this._deleteWalletFiles(this.currentAttemptWallet);
      this.currentAttemptWallet = null;
    }
  }

  async _rotateBaseWalletAfterKexError(reason) {
    const key = (reason || 'unknown').toString().slice(0, 120);

    if (!this._walletRotationStats) {
      this._walletRotationStats = { total: 0 };
    }
    this._walletRotationStats.total = (this._walletRotationStats.total || 0) + 1;
    this._walletRotationStats[key] = (this._walletRotationStats[key] || 0) + 1;

    try {
      console.log(`  [INFO] Rotating base wallet due to Monero KEX error: ${key}`);
      console.log(
        `    Rotation counts so far: total=${this._walletRotationStats.total}, reasonCount=${this._walletRotationStats[key]}`,
      );
      try {
        await this.monero.closeWallet();
      } catch (_ignored) {}
      await new Promise((r) => setTimeout(r, 500));
      const suffix = Math.floor(Date.now() / 1000)
        .toString(36)
        .slice(-4);
      this.baseWalletName = `${this.baseWalletName}_kx${suffix}`;
      this._updateEnv('MONERO_WALLET_NAME', this.baseWalletName);
      await this.monero.createWallet(this.baseWalletName, this.moneroPassword);
      console.log(`  [OK] New base wallet created after kex error: ${this.baseWalletName}`);
      try {
        await this._enableMoneroMultisigExperimentalForWallet(
          this.baseWalletName,
          `rotated base wallet ${this.baseWalletName} after kex error`,
        );
      } catch (e2) {
        console.log(
          '  [WARN] Failed to enable multisig experimental mode on rotated base wallet:',
          e2.message || String(e2),
        );
      }
      this.multisigInfo = null;
    } catch (rotateErr) {
      console.log(
        '  [WARN] Wallet rotation after kex error failed:',
        rotateErr.message || String(rotateErr),
      );
    }
  }

  async _hardResetMoneroWalletState(reason) {
    if (this._clusterFinalAddress) {
      console.log(
        '[MoneroReset] Cluster has a finalized multisig; skipping hard reset to preserve cluster wallet state.',
      );
      return;
    }

    const tag = (reason || 'unknown').toString().slice(0, 120);
    const walletDir = process.env.MONERO_WALLET_DIR || path.join(os.homedir(), '.monero-wallets');
    const maxResets = Number(process.env.MAX_AUTO_WALLET_RESETS || 1);
    this._walletHardResetCount = this._walletHardResetCount || 0;

    if (this._walletHardResetCount >= maxResets) {
      console.log(
        `[MoneroReset] Skipping hard reset (already performed ${this._walletHardResetCount}/${maxResets} times this process)`,
      );
      return;
    }

    this._walletHardResetCount += 1;
    console.log(`[INFO] [MoneroReset] Performing hard reset of Monero wallet state due to: ${tag}`);

    await this._walletMutex.withLock(async () => {
      try {
        try {
          await this.monero.closeWallet();
        } catch (_ignored) {}

        try {
          const entries = await fs.promises.readdir(walletDir, {
            withFileTypes: true,
          });
          for (const entry of entries) {
            const isFile = typeof entry.isFile === 'function' ? entry.isFile() : entry.isFile;
            if (!isFile) continue;
            const name = entry.name || '';
            if (!name.startsWith('znode_')) continue;
            try {
              await fs.promises.unlink(path.join(walletDir, name));
              console.log(`[MoneroReset] Deleted wallet file: ${name}`);
            } catch (e) {
              console.log(
                `[MoneroReset] Failed to delete wallet file ${name}:`,
                e.message || String(e),
              );
            }
          }
        } catch (e) {
          console.log(
            `[MoneroReset] Warning: could not scan wallet dir ${walletDir}:`,
            e.message || String(e),
          );
        }

        this.baseWalletName =
          process.env.MONERO_WALLET_NAME || `znode_${this.wallet.address.slice(2, 10)}`;
        this.multisigInfo = null;
        this._pendingR3 = null;

        try {
          await this.monero.createWallet(this.baseWalletName, this.moneroPassword);
          console.log(`[MoneroReset] Fresh base wallet created: ${this.baseWalletName}`);
        } catch (e) {
          console.log('[MoneroReset] Failed to create fresh base wallet:', e.message || String(e));
          return;
        }

        try {
          await this._enableMoneroMultisigExperimentalForWallet(
            this.baseWalletName,
            `hard reset base wallet ${this.baseWalletName}`,
          );
        } catch (e) {
          console.log(
            '[MoneroReset] Failed to enable multisig experimental mode after hard reset:',
            e.message || String(e),
          );
        }
      } catch (e) {
        console.log('[MoneroReset] Hard reset failed:', e.message || String(e));
      }
    });

    this.currentAttemptWallet = null;
    this._resetMoneroHealth();
    if (this.moneroErrorCounts) {
      this.moneroErrorCounts.kexRoundMismatch = 0;
      this.moneroErrorCounts.kexTimeout = 0;
      this.moneroErrorCounts.rpcFailure = 0;
    }
  }

  async _retireClusterWalletAndBackups(reason) {
    this._clearClusterState();
    if (!this._clusterFinalAddress) {
      return;
    }
    const tag = (reason || 'unknown').toString().slice(0, 120);
    const walletDir = process.env.MONERO_WALLET_DIR || path.join(os.homedir(), '.monero-wallets');
    const backupDir =
      process.env.WALLET_BACKUP_DIR || path.join(os.homedir(), '.znode-backup', 'wallets');
    const walletFile = this.clusterWalletName || this.baseWalletName;

    if (!walletFile) {
      console.log('[Cluster] No cluster wallet file recorded; skipping retirement.');
      return;
    }

    console.log(
      `[INFO] [Cluster] Retiring cluster wallet and backups due to cluster death: ${tag}`,
    );

    await this._walletMutex.withLock(async () => {
      try {
        try {
          if (
            this.monero &&
            this.monero._lastWallet &&
            this.monero._lastWallet.filename === walletFile
          ) {
            await this.monero.closeWallet();
          }
        } catch (_ignored) {}

        try {
          await fs.promises.unlink(path.join(walletDir, walletFile));
          console.log(`[Cluster] Deleted cluster wallet file: ${walletFile}`);
        } catch (_ignored) {}
        try {
          await fs.promises.unlink(path.join(walletDir, `${walletFile}.keys`));
          console.log(`[Cluster] Deleted cluster wallet keys file: ${walletFile}.keys`);
        } catch (_ignored) {}

        try {
          const entries = await fs.promises.readdir(backupDir, {
            withFileTypes: true,
          });
          for (const entry of entries) {
            const isFile = typeof entry.isFile === 'function' ? entry.isFile() : entry.isFile;
            if (!isFile) continue;
            const name = entry.name || '';
            if (!name.startsWith(walletFile)) continue;
            try {
              await fs.promises.unlink(path.join(backupDir, name));
              console.log(`[Cluster] Deleted cluster backup file: ${name}`);
            } catch (e) {
              console.log(
                `[Cluster] Failed to delete cluster backup file ${name}:`,
                e.message || String(e),
              );
            }
          }
        } catch (e) {
          console.log(
            `[Cluster] Warning: could not scan backup dir ${backupDir}:`,
            e.message || String(e),
          );
        }
      } catch (e) {
        console.log(
          '[Cluster] Error while retiring cluster wallet/backups:',
          e.message || String(e),
        );
      }
    });
  }

  async _autoEnableMoneroMultisigExperimental() {
    const walletFile = this.baseWalletName;
    if (!walletFile || !this.moneroPassword) {
      return;
    }
    await this._enableMoneroMultisigExperimentalForWallet(walletFile, `base wallet ${walletFile}`);
  }

  async startP2P() {
    console.log('[INFO] Starting P2P network...');

    try {
      await this.p2p.start();
      if (typeof this.p2p.startQueueDiscovery === 'function') {
        await this.p2p.startQueueDiscovery(
          this.staking.target || this.staking.address,
          this.chainId,
        );
      }
      console.log('[OK] P2P network started\n');
    } catch (error) {
      console.log('[WARN]  P2P start failed:', error.message);
      console.log(
        '  Cluster formation and multisig coordination are disabled until P2P is available.\n',
      );
    }
  }

  async initClusterP2P(clusterId, clusterNodes, isCoordinator = false) {
    if (!this.p2p || !this.p2p.node) {
      console.log('[WARN]  P2P not available, using smart contract');
      return false;
    }

    try {
      console.log('[INFO] Initializing P2P for cluster...');
      await this.p2p.connectToCluster(clusterId, clusterNodes, isCoordinator, this.registry);

      if (this.p2p) {
        this.p2p.round0Responder = async (cid) => {
          if (!this.multisigInfo) {
            await this.prepareMultisig();
          }
          return this.multisigInfo;
        };
      }

      await new Promise((r) => setTimeout(r, 3000));
      console.log('[OK] P2P cluster initialized');
      return true;
    } catch (error) {
      console.log('[WARN]  P2P cluster init failed:', error.message);
      return false;
    }
  }
  async prepareMultisig() {
    console.log('\n' + '[INFO] Preparing multisig...');
    try {
      const info = await this.monero.prepareMultisig();
      this.multisigInfo = info;
      console.log('[OK] Multisig info generated');
      const infoHash = crypto
        .createHash('sha256')
        .update(this.multisigInfo)
        .digest('hex')
        .slice(0, 10);
      console.log(`  Info hash: ${infoHash}... (length: ${this.multisigInfo.length})`);
      return this.multisigInfo;
    } catch (error) {
      if (error.message && error.message.toLowerCase().includes('already multisig')) {
        console.log(
          '  Wallet is already multisig from old deployment. Creating a fresh base wallet...',
        );
        try {
          try {
            await this.monero.closeWallet();
          } catch (_ignored) {}
          await new Promise((r) => setTimeout(r, 500));

          const canonicalBase =
            process.env.MONERO_WALLET_NAME || `znode_${this.wallet.address.slice(2, 10)}`;
          const suffix = Math.floor(Date.now() / 1000)
            .toString(36)
            .slice(-4);
          this.baseWalletName = `${canonicalBase}_b${suffix}`;
          await this.monero.createWallet(this.baseWalletName, this.moneroPassword);
          console.log(`  [OK] New base wallet created: ${this.baseWalletName}`);

          try {
            const backupScript = path.join(__dirname, 'scripts', 'auto-backup-wallet.sh');
            if (fs.existsSync(backupScript)) {
              try {
                fs.accessSync(backupScript, fs.constants.X_OK);
              } catch {
                throw new Error('auto-backup-wallet.sh exists but is not executable');
              }

              let backupPass = process.env.WALLET_BACKUP_PASSPHRASE;
              try {
                const homeDir = process.env.HOME || process.cwd();
                const defaultPassFile = path.join(
                  homeDir,
                  '.znode-backup',
                  'wallet_backup_passphrase.txt',
                );
                const passFile = process.env.WALLET_BACKUP_PASSPHRASE_FILE || defaultPassFile;
                const passDir = path.dirname(passFile);
                try {
                  fs.mkdirSync(passDir, { recursive: true, mode: 0o700 });
                } catch (_ignored) {}

                if (!backupPass) {
                  if (fs.existsSync(passFile)) {
                    backupPass = fs.readFileSync(passFile, 'utf8').trim();
                    if (!backupPass) {
                      console.warn(
                        '[WARN]  Wallet backup passphrase file is empty, regenerating a new one',
                      );
                    } else {
                      console.log('[OK] Loaded existing wallet backup passphrase from disk');
                    }
                  }
                }

                if (!backupPass) {
                  backupPass = crypto.randomBytes(32).toString('hex');
                  fs.writeFileSync(passFile, backupPass + '\n', {
                    mode: 0o600,
                  });
                  console.log(
                    `[OK] Generated new wallet backup passphrase and saved to ${passFile}`,
                  );
                }
              } catch (passErr) {
                console.warn(
                  '[WARN]  Failed to initialize wallet backup passphrase:',
                  passErr.message || String(passErr),
                );
              }

              const env = { ...process.env };
              if (backupPass) {
                env.WALLET_BACKUP_PASSPHRASE = backupPass;
              }

              await execFileAsync(backupScript, [], {
                cwd: __dirname,
                timeout: 30000,
                env,
              });
              console.log('[OK] Base wallet backed up using auto-backup-wallet.sh');
            } else {
              console.log('[WARN]  auto-backup-wallet.sh not found, skipping automatic backup');
            }
          } catch (backupErr) {
            console.log(
              '[WARN]  Base wallet backup failed:',
              backupErr.message || String(backupErr),
            );
          }

          const info2 = await this.monero.prepareMultisig();
          this.multisigInfo = info2;
          console.log('[OK] Multisig info generated');
          const infoHash2 = crypto
            .createHash('sha256')
            .update(this.multisigInfo)
            .digest('hex')
            .slice(0, 10);
          console.log(`  Info hash: ${infoHash2}... (length: ${this.multisigInfo.length})`);
          return this.multisigInfo;
        } catch (e) {
          console.error('[ERROR] Failed to create fresh base wallet:', e.message);
          throw e;
        }
      }
      console.error('[ERROR] prepare_multisig failed:', error.message);
      throw error;
    }
  }
  async makeMultisig(multisigInfos, threshold) {
    console.log(
      `\n[INFO] Stage 1: make_multisig for ${threshold}-of-${multisigInfos.length + 1} multisig (intermediate wallet, finalization pending)...`,
    );

    try {
      const result = await this.monero.call(
        'make_multisig',
        {
          multisig_info: multisigInfos,
          threshold: threshold,
          password: this.moneroPassword,
        },
        180000,
      );

      console.log('[OK] make_multisig RPC succeeded (intermediate multisig wallet; not final yet)');
      console.log(`  Intermediate address (not final): ${result.address}`);

      return result;
    } catch (error) {
      console.error('[ERROR] make_multisig failed:', error.message);

      if (error.message && error.message.toLowerCase().includes('already multisig')) {
        try {
          console.log(
            '  Wallet is already multisig from old deployment. Rotating base wallet for next attempt...',
          );
          try {
            await this.monero.closeWallet();
          } catch (_ignored) {}
          await new Promise((r) => setTimeout(r, 500));

          const suffix = Math.floor(Date.now() / 1000)
            .toString(36)
            .slice(-4);
          this.baseWalletName = `${this.baseWalletName}_b${suffix}`;
          await this.monero.createWallet(this.baseWalletName, this.moneroPassword);
          console.log(`  [OK] New base wallet created for next attempt: ${this.baseWalletName}`);

          try {
            await this._enableMoneroMultisigExperimentalForWallet(
              this.baseWalletName,
              `rotated base wallet ${this.baseWalletName}`,
            );
          } catch (e) {
            console.log(
              '  [WARN] Failed to enable multisig experimental mode on rotated base wallet:',
              e.message || String(e),
            );
          }

          this.multisigInfo = null;
        } catch (rotateErr) {
          console.log(
            '  [WARN] Failed to rotate base wallet after make_multisig already-multisig error:',
            rotateErr.message || String(rotateErr),
          );
        }
      }

      throw error;
    }
  }

  _startCoordinatorHeartbeat(clusterId, members) {
    if (this._coordHeartbeatTimer) {
      clearInterval(this._coordHeartbeatTimer);
    }

    const intervalMs = Number(process.env.COORD_HEARTBEAT_INTERVAL_MS || 10000);
    this._lastCoordHeartbeatSent = Date.now();

    console.log(`[CoordHB] Starting coordinator heartbeat (interval: ${intervalMs / 1000}s)`);

    const sendHeartbeat = async () => {
      if (!this.p2p || !this._activeClusterId) {
        return;
      }

      try {
        await this.p2p.broadcastRoundData(
          clusterId,
          this._sessionId || 'coord',
          9999,
          JSON.stringify({
            type: 'coord-alive',
            timestamp: Date.now(),
            coordinator: this.wallet.address,
          }),
        );
        this._lastCoordHeartbeatSent = Date.now();
      } catch (e) {
        console.log('[CoordHB] Failed to send heartbeat:', e.message || String(e));
      }
    };

    sendHeartbeat();

    this._coordHeartbeatTimer = setInterval(sendHeartbeat, intervalMs);
  }

  _stopCoordinatorHeartbeat() {
    if (this._coordHeartbeatTimer) {
      clearInterval(this._coordHeartbeatTimer);
      this._coordHeartbeatTimer = null;
    }
  }

  _startCoordinatorMonitor(coordinator) {
    if (this._coordMonitorTimer) {
      clearInterval(this._coordMonitorTimer);
    }

    this._lastCoordHeartbeatReceived = Date.now();
    const timeoutMs = Number(process.env.COORD_HEARTBEAT_TIMEOUT_MS || 30000);
    const checkIntervalMs = 5000;

    console.log(`[CoordHB] Monitoring coordinator (timeout: ${timeoutMs / 1000}s)`);

    this._coordMonitorTimer = setInterval(() => {
      const elapsed = Date.now() - this._lastCoordHeartbeatReceived;

      if (elapsed > timeoutMs) {
        console.log(
          `[WARN]  [CoordHB] Coordinator silent for ${Math.round(elapsed / 1000)}s (>${timeoutMs / 1000}s)`,
        );
        try {
          if (this.stateMachine && typeof this.stateMachine.fail === 'function') {
            this.stateMachine.fail('coordinator_silent');
          }
          const cooldownMs = Number(process.env.CLUSTER_RETRY_COOLDOWN_MS || 300000);
          const now = Date.now();
          if (this.clusterState && typeof this.clusterState.setCooldownUntil === 'function') {
            this.clusterState.setCooldownUntil(now + cooldownMs);
            if (typeof this.clusterState.recordFailure === 'function') {
              this.clusterState.recordFailure('coordinator_silent');
            }
          }
        } catch (_ignored) {}
      }
    }, checkIntervalMs);
  }

  _stopCoordinatorMonitor() {
    if (this._coordMonitorTimer) {
      clearInterval(this._coordMonitorTimer);
      this._coordMonitorTimer = null;
    }
  }

  _handleCoordinatorHeartbeat(data) {
    if (data && data.type === 'coord-alive') {
      this._lastCoordHeartbeatReceived = Date.now();
    }
  }

  async startClusterMultisigV3(clusterId, members, isCoordinator, threshold = 7) {
    this.stateMachine.transition(
      'DISCOVERING',
      { clusterId, memberCount: members.length },
      'cluster formation started',
    );

    if (isCoordinator) {
      this._startCoordinatorHeartbeat(clusterId, members);
    } else {
      const canonicalForHB = [...members].map((a) => a.toLowerCase()).sort();
      const coordinatorAddr = members.find((m) => m.toLowerCase() === canonicalForHB[0]);
      if (coordinatorAddr) {
        this._startCoordinatorMonitor(coordinatorAddr);
      }
    }

    const sid = (Date.now().toString(36) + crypto.randomBytes(4).toString('hex')).slice(-12);
    this._sessionId = sid;

    const recordClusterFailure = (reason) => {
      this.stateMachine.fail(reason);
      const reachable = this.p2p ? this.p2p.countConnectedPeers() : 0;
      const currentCount = this._clusterFailures.get(clusterId) || 0;
      const newCount = currentCount + 1;
      this._clusterFailures.set(clusterId, newCount);
      this._clusterFailMeta[clusterId] = {
        failures: newCount,
        reachable,
        lastFailureAt: Date.now(),
        reason,
      };

      if (newCount >= 3) {
        if (!this._clusterBlacklist) this._clusterBlacklist = {};
        const baseCooldownMs = Number(process.env.CLUSTER_BLACKLIST_BASE_COOLDOWN_MS || 600000);
        const cooldownMs = computeAdaptiveBlacklistCooldownMs(newCount, baseCooldownMs);
        this._clusterBlacklist[clusterId] = Date.now() + cooldownMs;
        console.log(
          `[WARN] Cluster ${clusterId.slice(0, 10)} blacklisted for ${cooldownMs / 60000} minutes (${newCount} failures, adaptive cooldown)`,
        );
      }

      this._saveClusterBlacklist();
    };

    const roundTimeoutPrepare = Number(process.env.ROUND_TIMEOUT_PREPARE_MS || 300000);
    const roundTimeoutExchange = Number(process.env.ROUND_TIMEOUT_EXCHANGE_MS || 180000);

    try {
      const canonicalMembers = canonicalizeMembers(members);
      if (members.some((a, i) => canonicalMembers[i] !== a)) {
        console.log('[Multisig] Canonicalized member order for key exchange rounds.');
      }
      if (!this.p2p || !this.p2p.node) {
        console.log('[WARN] P2P not available - cannot perform multisig coordination');
        return false;
      }

      if (this.p2p && typeof this.p2p.clearClusterRounds === 'function') {
        try {
          this.p2p.clearClusterRounds(clusterId, this._sessionId, [0, 3, 4, 5, 6, 7, 9999]);
        } catch (e) {
          console.log(
            '  [WARN] Failed to clear cached P2P round data for cluster',
            String(clusterId),
            ':',
            e.message || String(e),
          );
        }
      }

      if (!this.multisigInfo) {
        await this.prepareMultisig();
      }

      let moneroPrimaryAddress = '';
      try {
        moneroPrimaryAddress = await this.monero.getAddress();
      } catch (e) {
        console.log(
          '[ERROR] Failed to get Monero primary address for monero-config:',
          e.message || String(e),
        );
        recordClusterFailure('monero_config_no_address');
        return false;
      }

      try {
        console.log('[MoneroConfig] Local Monero address:', moneroPrimaryAddress);
        console.log(
          '[MoneroConfig] Local wallet for this attempt:',
          this.currentAttemptWallet || this.baseWalletName || '(unset)',
        );
      } catch (_ignored) {}

      const moneroConfigData = JSON.stringify({
        members: canonicalMembers.map((a) => (a || '').toLowerCase()),
        threshold,
      });
      const moneroConfigHash = ethers.id(moneroConfigData);

      console.log('[INFO] PBFT Consensus: confirming monero-config across cluster...');
      let moneroConfigConsensus;
      try {
        moneroConfigConsensus = await this.p2p.runConsensus(
          clusterId,
          null,
          'monero-config',
          moneroConfigHash,
          canonicalMembers,
          roundTimeoutPrepare,
        );
      } catch (e) {
        console.log('[ERROR] PBFT monero-config consensus error:', e.message || String(e));
        recordClusterFailure('monero_config_pbft_error');
        return false;
      }
      if (!moneroConfigConsensus.success) {
        console.log(
          '[ERROR] PBFT monero-config consensus failed - missing:',
          (moneroConfigConsensus.missing || []).join(', '),
        );
        recordClusterFailure('monero_config_pbft_failed');
        return false;
      }
      console.log('[OK] PBFT monero-config consensus reached');

      const expectedPeerCount = members.length - 1;
      let peers = null;

      console.log('\n' + '[INFO] Round 0: exchanging prepare_multisig info via P2P (broadcast)...');

      try {
        await this.p2p.broadcastRoundData(clusterId, null, 0, this.multisigInfo);
      } catch (e) {
        console.log('  [WARN] Round 0 broadcast error:', e.message || String(e));
      }

      const complete0 = await this.p2p.waitForRoundCompletion(
        clusterId,
        null,
        0,
        canonicalMembers,
        roundTimeoutPrepare,
      );
      if (!complete0) {
        console.log('[ERROR] Round 0 incomplete within timeout');
        if (isCoordinator) {
          recordClusterFailure('round0_timeout');
        }
        return false;
      }

      peers = await this.p2p.getPeerPayloads(clusterId, null, 0, canonicalMembers);
      if (!Array.isArray(peers) || peers.length < expectedPeerCount) {
        const got = peers ? peers.length : 0;
        console.log(
          '[ERROR] Round 0: expected ' + expectedPeerCount + ' peer multisig infos, got ' + got,
        );
        if (isCoordinator) {
          recordClusterFailure('round0_peers_short');
        }
        return false;
      }

      const consensusHash0 = await this.p2p.computeConsensusHash(
        clusterId,
        null,
        0,
        canonicalMembers,
      );
      console.log(`Round 0 consensus hash: ${consensusHash0.substring(0, 18)}...`);
      console.log('[INFO] PBFT Consensus: confirming Round 0 complete...');
      let round0Consensus;
      try {
        round0Consensus = await this.p2p.runConsensus(
          clusterId,
          null,
          'round-0',
          consensusHash0,
          canonicalMembers,
          roundTimeoutPrepare,
        );
      } catch (e) {
        console.log(`[ERROR] PBFT Round 0 consensus error: ${e.message || String(e)}`);
        if (isCoordinator) {
          recordClusterFailure('round0_pbft_timeout');
        }
        return false;
      }
      if (!round0Consensus.success) {
        console.log(
          `[ERROR] PBFT Round 0 consensus failed - missing: ${(round0Consensus.missing || []).join(', ')}`,
        );
        if (isCoordinator) {
          recordClusterFailure('round0_pbft_timeout');
        }
        return false;
      }
      console.log('[OK] PBFT Round 0 consensus reached - all 11/11 nodes confirmed');
      this.stateMachine.transition('ROUND_0', { round: 0 }, 'round 0 consensus complete');
      try {
        const r0Hash = await this.p2p.computeConsensusHash(clusterId, null, 0, canonicalMembers);
        this._sessionId =
          r0Hash && r0Hash.startsWith('0x') ? r0Hash.slice(2, 14) : Date.now().toString(36);

        if (this.p2p && typeof this.p2p.joinCluster === 'function') {
          await this.p2p.joinCluster(
            `${clusterId}:${this._sessionId}`,
            canonicalMembers,
            isCoordinator,
          );
        }
        if (this.p2p && typeof this.p2p.clearClusterRounds === 'function') {
          await this.p2p.clearClusterRounds(clusterId, this._sessionId, [0, 3, 4, 5, 6, 7, 9999]);
        }
        console.log(`[Session] Using sessionId ${this._sessionId} for R3+`);
      } catch (e) {
        console.log('[Session] Failed to initialize session-scoped topic:', e.message || String(e));
      }

      console.log(
        `Using wallet for multisig attempt: ${this.currentAttemptWallet || this.baseWalletName}`,
      );

      this.stateMachine.transition('MAKE_MULTISIG', {}, 'making multisig wallet');
      const res = await this.makeMultisig(peers, threshold);

      try {
        const currentWallet =
          this.monero && this.monero._lastWallet && this.monero._lastWallet.filename
            ? this.monero._lastWallet.filename
            : this.baseWalletName;
        await this._enableMoneroMultisigExperimentalForWallet(
          currentWallet,
          `multisig wallet ${currentWallet}`,
        );
      } catch (e) {
        console.log(
          '  [WARN] Failed to ensure multisig experimental mode after make_multisig:',
          e.message || String(e),
        );
      }
      if (res && (res.multisig_info || res.multisigInfo)) {
        this._pendingR3 = res.multisig_info || res.multisigInfo;
      } else {
        this._pendingR3 = '';
      }

      if (!this._pendingR3 || this._pendingR3.length === 0) {
        console.log('[ERROR] No Round 3 payload available after make_multisig');
        return false;
      }

      console.log('\n[INFO] Round 3: broadcasting key exchange payload via P2P...');
      await this.p2p.broadcastRoundData(clusterId, this._sessionId, 3, this._pendingR3);
      const complete3 = await this.p2p.waitForRoundCompletion(
        clusterId,
        this._sessionId,
        3,
        canonicalMembers,
        roundTimeoutExchange,
      );
      if (!complete3) {
        console.log('[ERROR] Round 3 incomplete - not all nodes submitted within timeout');
        recordClusterFailure('round3_timeout');
        return false;
      }

      await this.p2p.getPeerPayloads(clusterId, this._sessionId, 3, canonicalMembers);

      const consensusHash3 = await this.p2p.computeConsensusHash(
        clusterId,
        this._sessionId,
        3,
        canonicalMembers,
      );
      console.log(`Round 3 consensus hash: ${consensusHash3.substring(0, 18)}...`);
      console.log('[INFO] PBFT Consensus: confirming Round 3 complete...');
      let round3Consensus;
      try {
        round3Consensus = await this.p2p.runConsensus(
          clusterId,
          this._sessionId,
          'round-3',
          consensusHash3,
          canonicalMembers,
          roundTimeoutExchange,
        );
      } catch (e) {
        console.log(`[ERROR] PBFT Round 3 consensus error: ${e.message || String(e)}`);
        recordClusterFailure('round3_pbft_timeout');
        return false;
      }
      if (!round3Consensus.success) {
        console.log(
          `[ERROR] PBFT Round 3 consensus failed - missing: ${(round3Consensus.missing || []).join(', ')}`,
        );
        recordClusterFailure('round3_pbft_timeout');
        return false;
      }
      console.log('[OK] PBFT Round 3 consensus reached - all 11/11 nodes confirmed');
      this.stateMachine.transition('ROUND_3', { round: 3 }, 'round 3 consensus complete');
      this.stateMachine.transition(
        'KEY_EXCHANGE',
        { startingRound: 4 },
        'entering key exchange rounds',
      );

      const { success, lastRound, lastPeerPayloads } = await this.runKeyExchangeRounds(
        clusterId,
        canonicalMembers,
        4,
      );
      if (!success) {
        console.log(`[ERROR] Key exchange failed at round ${lastRound}`);
        recordClusterFailure('key_exchange_failed');
        return false;
      }

      console.log('[INFO] PBFT Consensus: confirming all nodes ready to finalize...');
      this.stateMachine.transition('FINALIZING_MONERO', {}, 'finalizing monero multisig');
      let finalizeConsensus;
      try {
        finalizeConsensus = await this.p2p.runConsensus(
          clusterId,
          this._sessionId,
          'finalize',
          `ready-${lastRound}`,
          canonicalMembers,
          120000,
        );
      } catch (e) {
        console.log(`[ERROR] PBFT finalize consensus error: ${e.message || String(e)}`);
        recordClusterFailure('finalize_pbft_timeout');
        return false;
      }
      if (!finalizeConsensus.success) {
        console.log(
          `[ERROR] PBFT finalize consensus failed - missing: ${(finalizeConsensus.missing || []).join(', ')}`,
        );
        recordClusterFailure('finalize_pbft_timeout');
        return false;
      }
      console.log('[OK] PBFT finalize consensus reached - all 11/11 nodes ready');
      console.log('  [INFO] Finalizing multisig with peer keys...');
      try {
        await this.monero.finalizeMultisig(lastPeerPayloads);
      } catch {
        try {
          await this.monero.finalizeMultisig();
        } catch (e2) {
          console.log('  [WARN]  Finalize multisig error:', e2.message || String(e2));
        }
      }

      const finalizeRetries = Number(process.env.FINALIZE_READY_RETRIES || 10);
      const finalizeDelayMs = Number(process.env.FINALIZE_READY_DELAY_MS || 5000);

      let info;
      let retries = finalizeRetries;
      while (retries > 0) {
        try {
          info = await this.monero.call('is_multisig');
          if (info && info.ready) {
            break;
          }
          if (retries > 1) {
            console.log(
              `  [INFO] Multisig not ready yet, waiting ${finalizeDelayMs}ms before retry (${retries - 1} retries left)...`,
            );
            await new Promise((resolve) => setTimeout(resolve, finalizeDelayMs));
          }
        } catch (e) {
          console.log('  [WARN]  is_multisig check failed:', e.message || String(e));
          if (retries > 1) {
            await new Promise((resolve) => setTimeout(resolve, finalizeDelayMs));
          }
        }
        retries--;
      }

      if (!info || !info.ready) {
        console.log('[ERROR] Multisig still not ready after finalize and retries');
        recordClusterFailure('final_multisig_not_ready');
        return false;
      }

      const getAddrResult = await this.monero.call('get_address');
      const finalAddr = getAddrResult.address;

      try {
        const backupScript = path.join(__dirname, 'scripts', 'auto-backup-wallet.sh');
        if (fs.existsSync(backupScript)) {
          await execFileAsync(backupScript, [], {
            cwd: __dirname,
            timeout: 30000,
          });
          console.log('[OK] Multisig wallet backed up automatically using auto-backup-wallet.sh');
        } else {
          console.log('[WARN]  auto-backup-wallet.sh not found, skipping automatic backup');
        }
      } catch (backupErr) {
        console.log('[WARN]  Wallet backup failed:', backupErr.message || String(backupErr));
      }
      console.log(`
[OK] Final multisig address: ${finalAddr}`);

      try {
        const currentWallet =
          this.monero && this.monero._lastWallet && this.monero._lastWallet.filename
            ? this.monero._lastWallet.filename
            : this.baseWalletName;
        this.clusterWalletName = currentWallet;
        console.log('[Cluster] Active cluster wallet file:', this.clusterWalletName || '(unknown)');
      } catch (_ignored) {}

      this._clusterFinalAddress = finalAddr;
      this._clusterFinalizationStartAt = Date.now();

      if (isCoordinator) {
        const jitterMs = Math.floor(Math.random() * 2000);
        if (jitterMs > 0) {
          console.log(`  [INFO] Adding ${jitterMs}ms jitter to reduce coordinator collision`);
          await new Promise((resolve) => setTimeout(resolve, jitterMs));
        }

        const maxRetries = 3;
        let attempt = 0;
        let finalized = false;

        while (attempt < maxRetries && !finalized) {
          attempt++;
          try {
            const alreadyFinalized = await this.isClusterFinalized(clusterId);
            if (alreadyFinalized) {
              console.log('[OK] Cluster already finalized on-chain (by another coordinator)');
              finalized = true;
              this._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
              break;
            }

            console.log(
              `  [INFO] Attempt ${attempt}/${maxRetries}: finalizeCluster([${members.length} members, canonical order], ${finalAddr})`,
            );
            if (DRY_RUN) {
              console.log('  [DRY_RUN] Would send finalizeCluster transaction');
              finalized = true;
            } else {
              const tx = await this.registry.finalizeCluster(canonicalMembers, finalAddr);
              await tx.wait();
              console.log('[OK] Cluster finalized on-chain (v3)');
              finalized = true;

              if (this.p2p) {
                try {
                  await this.p2p.broadcastRoundData(
                    clusterId,
                    this._sessionId || 'coord',
                    9999,
                    JSON.stringify({
                      type: 'cluster-finalized',
                      clusterId: clusterId,
                      moneroAddress: finalAddr,
                      members: canonicalMembers,
                      timestamp: Date.now(),
                    }),
                  );
                  console.log('[Coordinator] Broadcast finalization to cluster members');
                } catch (e) {
                  console.log('[Coordinator] Failed to broadcast finalization:', e.message);
                }
              }

              this._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
            }
          } catch (e) {
            const msg = e && e.message ? e.message : String(e);

            if (/already finalized|already exists|cluster.*finalized/i.test(msg)) {
              console.log('[OK] Cluster already finalized on-chain (race condition handled)');
              finalized = true;
              this._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
              break;
            }

            if (/nonce.*too low|replacement.*underpriced|already known/i.test(msg)) {
              console.log(
                `  [WARN]  Nonce/replacement error (attempt ${attempt}/${maxRetries}): ${msg}`,
              );
              if (attempt < maxRetries) {
                const backoffMs = 1000 * Math.pow(2, attempt - 1);
                console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
                await new Promise((resolve) => setTimeout(resolve, backoffMs));
                continue;
              }
            }

            console.log(
              `  [WARN]  finalizeCluster error (attempt ${attempt}/${maxRetries}): ${msg}`,
            );
            const nowFinalized = await this.isClusterFinalized(clusterId);
            if (nowFinalized) {
              console.log('[OK] Cluster finalized despite error (transaction succeeded)');
              finalized = true;
              this._onClusterFinalized(clusterId, canonicalMembers, finalAddr);
              break;
            }

            if (attempt >= maxRetries) {
              console.log('[ERROR] finalizeCluster() failed after all retries');
              return false;
            }

            const backoffMs = 1000 * Math.pow(2, attempt - 1);
            console.log(`  [INFO] Waiting ${backoffMs}ms before retry...`);
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
          }
        }
      } else {
        console.log('[INFO] Waiting for coordinator to finalize cluster on-chain...');
        try {
          await this._waitForOnChainFinalizationForSelf(finalAddr, canonicalMembers);
        } catch (e) {
          console.log(
            '[Cluster] Error while waiting for on-chain finalization:',
            e.message || String(e),
          );
        }
      }
      return true;
    } catch (e) {
      console.log('[ERROR] Cluster multisig v3 error:', e.message || String(e));
      recordClusterFailure('exception');

      if (this.p2p && typeof this.p2p.leaveCluster === 'function') {
        try {
          await this.p2p.leaveCluster(clusterId);
          this.p2p.cleanupOldMessages();
        } catch (cleanupErr) {
          console.log('[WARN]  P2P cleanup error:', cleanupErr.message);
        }
      }

      return false;
    }
  }
  async runKeyExchangeRounds(clusterId, clusterNodes, startRound = 4) {
    const maxRounds = Number(process.env.MAX_KEY_EXCHANGE_ROUNDS || 7);
    const roundTimeoutExchange = Number(process.env.ROUND_TIMEOUT_EXCHANGE_MS || 180000);
    let round = startRound;
    let prevRound = 3;

    for (let i = 0; i < maxRounds; i++) {
      console.log(`  [INFO] Key exchange round ${round} (${i + 1}/${maxRounds})`);

      const peersPrev = await this.p2p.getPeerPayloads(
        clusterId,
        this._sessionId,
        prevRound,
        clusterNodes,
      );
      const expectedPeers = clusterNodes.length - 1;
      const actualPeers = Array.isArray(peersPrev) ? peersPrev.length : 0;

      if (!Array.isArray(peersPrev) || actualPeers === 0) {
        console.log(
          `  [ERROR] Round ${prevRound}: no peer payloads found; expected ${expectedPeers}`,
        );
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev || [],
        };
      }

      if (actualPeers !== expectedPeers) {
        console.log(
          `  [ERROR] Round ${prevRound}: expected ${expectedPeers} peer multisig infos, got ${actualPeers}`,
        );

        try {
          const hashes = peersPrev.map((s, idx) => {
            const h = crypto.createHash('sha256').update(String(s)).digest('hex').slice(0, 10);
            return `#${idx}:${h}`;
          });
          console.log(`  ↳ Partial peer payload hashes (round ${prevRound}): ${hashes.join(', ')}`);
        } catch (_ignored) {}

        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }

      try {
        const fullHashes = peersPrev.map((s) =>
          crypto.createHash('sha256').update(String(s)).digest('hex'),
        );
        const short = fullHashes.map((h, idx) => `#${idx}:${h.slice(0, 10)}`);
        console.log(
          `  [INFO] Round ${prevRound}: using ${actualPeers} peer payloads for exchange_multisig_keys:`,
        );
        console.log(`    Payload hashes: ${short.join(', ')}`);

        const unique = new Set(fullHashes);
        if (unique.size !== fullHashes.length) {
          const seen = new Map();
          const dups = [];
          fullHashes.forEach((h, i) => {
            if (seen.has(h)) dups.push(`${seen.get(h)}<->${i}:${h.slice(0, 10)}`);
            else seen.set(h, i);
          });
          console.log(
            `  [ERROR] Duplicate peer payloads detected for round ${prevRound}: ${dups.join(', ')}`,
          );
          return {
            success: false,
            lastRound: prevRound,
            lastPeerPayloads: peersPrev,
          };
        }
      } catch (_ignored) {}

      let preKexHash = null;
      try {
        preKexHash = await this.p2p.computeConsensusHash(
          clusterId,
          this._sessionId,
          prevRound,
          clusterNodes,
        );
      } catch (e) {
        console.log(
          `  [ERROR] Failed to compute consensus hash for pre-kex Round ${prevRound}:`,
          e.message || String(e),
        );
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      if (!preKexHash) {
        console.log(`  [ERROR] Empty consensus hash for pre-kex Round ${prevRound}`);
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      console.log(
        `  [INFO] PBFT Consensus: confirming Round ${prevRound} state before KEX (hash=${preKexHash.substring(0, 18)}...)`,
      );
      let preKexConsensus;
      try {
        preKexConsensus = await this.p2p.runConsensus(
          clusterId,
          this._sessionId,
          `pre-exchange-${round}`,
          preKexHash,
          clusterNodes,
          roundTimeoutExchange,
        );
      } catch (e) {
        console.log(
          `  [ERROR] PBFT pre-exchange consensus error for Round ${round}:`,
          e.message || String(e),
        );
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      if (!preKexConsensus.success) {
        console.log(
          `  [ERROR] PBFT pre-exchange consensus failed for Round ${round} - missing: ${(preKexConsensus.missing || []).join(', ')}`,
        );
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }
      console.log(`  [OK] PBFT pre-exchange consensus reached for Round ${round}`);
      try {
        console.log(
          '  [INFO] Pre-KEX base wallet for this node:',
          this.baseWalletName || '(unset)',
        );
      } catch (_ignored) {}

      let myPayload = '';
      try {
        const infoBefore = await this.monero.call('is_multisig');
        console.log(
          `  [Debug] is_multisig RAW before Round ${round}: ` + JSON.stringify(infoBefore),
        );
      } catch (e) {
        console.log(
          `  [WARN]  is_multisig pre-KEX check failed for Round ${round}:`,
          e.message || String(e),
        );
      }
      try {
        const res = await this.monero.exchangeMultisigKeys(peersPrev, this.moneroPassword);

        try {
          const infoAfter = await this.monero.call('is_multisig');
          console.log(
            `  [Debug] is_multisig RAW after Round ${round}: ` + JSON.stringify(infoAfter),
          );
        } catch (e) {
          console.log(
            `  [WARN]  is_multisig post-KEX check failed for Round ${round}:`,
            e.message || String(e),
          );
        }

        if (res && typeof res === 'object') {
          if (typeof res.multisig_info === 'string') {
            myPayload = res.multisig_info;
          } else if (typeof res.multisigInfo === 'string') {
            myPayload = res.multisigInfo;
          } else {
            myPayload = '';
          }
        } else if (typeof res === 'string') {
          myPayload = res;
        }

        if (!myPayload) {
          console.log(
            '  [WARN]  exchangeMultisigKeys returned no multisig_info; treating as failure',
          );
          return {
            success: false,
            lastRound: round - 1,
            lastPeerPayloads: peersPrev,
          };
        }
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        if (/Messages don't have the expected kex round number/i.test(msg)) {
          console.log(
            '  [ERROR] Monero KEX round mismatch detected; marking Monero health for global reset',
          );
          this._noteMoneroKexRoundMismatch(round, msg);
          return {
            success: false,
            lastRound: prevRound,
            lastPeerPayloads: peersPrev,
          };
        }

        if (/kex is already complete/i.test(msg) || /already complete/i.test(msg)) {
          console.log('  [OK] Multisig key exchange already complete at wallet level');
          try {
            const info = await this.monero.call('is_multisig');
            if (info && info.ready) {
              return {
                success: true,
                lastRound: prevRound,
                lastPeerPayloads: peersPrev,
              };
            }
            console.log(
              '  [WARN]  Wallet reports exchange complete but multisig not ready; rotating wallet.',
            );
            await this._rotateBaseWalletAfterKexError('kex complete but not ready');
          } catch (e2) {
            console.log(
              '  [WARN]  is_multisig check failed during already-complete handling:',
              e2.message || String(e2),
            );
            await this._rotateBaseWalletAfterKexError('kex complete; is_multisig failed');
          }
          return {
            success: false,
            lastRound: prevRound,
            lastPeerPayloads: peersPrev,
          };
        }

        console.log(`  [ERROR] exchange_multisig_keys error in round ${round}:`, msg);
        this._noteMoneroRpcFailure('exchange_multisig_keys', msg);
        await this._rotateBaseWalletAfterKexError(msg || 'unknown Monero KEX error');
        return {
          success: false,
          lastRound: prevRound,
          lastPeerPayloads: peersPrev,
        };
      }

      console.log(`  [INFO] Broadcasting Round ${round} via P2P...`);
      await this.p2p.broadcastRoundData(clusterId, this._sessionId, round, myPayload);
      console.log(`  [OK] Round ${round} broadcast complete`);

      console.log(`  [INFO] Waiting for Round ${round} completion...`);
      const complete = await this.p2p.waitForRoundCompletion(
        clusterId,
        this._sessionId,
        round,
        clusterNodes,
        roundTimeoutExchange,
      );
      if (!complete) {
        console.log(`  [ERROR] Round ${round} incomplete - not all nodes submitted within timeout`);
        this._noteMoneroKexTimeout(round);
        const peersCurrent = await this.p2p.getPeerPayloads(
          clusterId,
          this._sessionId,
          round,
          clusterNodes,
        );
        return {
          success: false,
          lastRound: round,
          lastPeerPayloads: peersCurrent,
        };
      }

      const peersCurrent = await this.p2p.getPeerPayloads(
        clusterId,
        this._sessionId,
        round,
        clusterNodes,
      );

      const consensusHashN = await this.p2p.computeConsensusHash(
        clusterId,
        this._sessionId,
        round,
        clusterNodes,
      );
      console.log(`  [INFO] PBFT Consensus: confirming Round ${round} complete...`);
      let roundNConsensus;
      try {
        roundNConsensus = await this.p2p.runConsensus(
          clusterId,
          this._sessionId,
          `round-${round}`,
          consensusHashN,
          clusterNodes,
          roundTimeoutExchange,
        );
      } catch (e) {
        console.log(`  [ERROR] PBFT Round ${round} consensus error: ${e.message || String(e)}`);
        return {
          success: false,
          lastRound: round,
          lastPeerPayloads: peersCurrent,
        };
      }
      if (!roundNConsensus.success) {
        console.log(
          `  [ERROR] PBFT Round ${round} consensus failed - missing: ${(roundNConsensus.missing || []).join(', ')}`,
        );
        return {
          success: false,
          lastRound: round,
          lastPeerPayloads: peersCurrent,
        };
      }
      console.log(`  [OK] PBFT Round ${round} consensus reached`);

      try {
        const info = await this.monero.call('is_multisig');
        if (info && info.ready) {
          console.log(`  [OK] Multisig is ready after Round ${round}`);
          return {
            success: true,
            lastRound: round,
            lastPeerPayloads: peersCurrent,
          };
        }
      } catch (e) {
        console.log('  [WARN]  is_multisig check failed:', e.message || String(e));
      }

      prevRound = round;
      round += 1;
    }

    console.log('  [WARN]  Max key exchange rounds reached without ready multisig');
    const lastPayloads = await this.p2p.getPeerPayloads(
      clusterId,
      this._sessionId,
      prevRound,
      clusterNodes,
    );
    return {
      success: false,
      lastRound: prevRound,
      lastPeerPayloads: lastPayloads,
    };
  }

  async ensureRegistered() {
    console.log('[INFO] Registering to network (v3)...');

    if (!this.multisigInfo) {
      try {
        await this.prepareMultisig();
      } catch (e) {
        console.log(
          '[ERROR] Failed to prepare multisig during registration:',
          e.message || String(e),
        );
      }
    }

    let registered = false;
    try {
      const info = await this.registry.nodes(this.wallet.address);
      if (info && info.registered !== undefined) {
        console.log('[Registry] Using named field ABI decoding for nodes()');
        registered = !!info.registered;
      } else if (Array.isArray(info) && info.length >= 2) {
        console.log('[Registry] Using tuple ABI decoding for nodes() (fallback)');
        registered = !!info[1];
      }
    } catch {
      console.log('[Registry] nodes() call failed, assuming not registered');
      registered = false;
    }

    if (registered) {
      console.log('[OK] Already registered\n');
      return;
    }

    const codeHash = ethers.id('znode-v3');
    console.log(`  [INFO] registerNode(${codeHash})`);
    if (DRY_RUN) {
      console.log('  [DRY_RUN] Would send registerNode transaction');
    } else {
      const tx = await this.registry.registerNode(codeHash);
      await tx.wait();
      console.log('[OK] Registered\n');
    }
  }

  async isClusterFinalized(clusterId) {
    try {
      const clusterInfo = await this.registry.clusters(clusterId);
      return !!(clusterInfo && clusterInfo[2]);
    } catch {
      return false;
    }
  }

  startHeartbeatLoop() {
    return startHeartbeatLoop(this, DRY_RUN);
  }

  startSlashingLoop() {
    return startSlashingLoop(this, DRY_RUN);
  }

  async monitorNetwork() {
    return monitorNetwork(this, DRY_RUN);
  }

  startBridgeAPI() {
    return startBridgeAPI(this);
  }

  stopBridgeAPI() {
    return stopBridgeAPI(this);
  }

  async startDepositMonitor() {
    return startDepositMonitor(this);
  }

  stopDepositMonitor() {
    return stopDepositMonitor(this);
  }

  async startWithdrawalMonitor() {
    return startWithdrawalMonitor(this);
  }

  stopWithdrawalMonitor() {
    return stopWithdrawalMonitor(this);
  }

  getMintSignature(xmrTxid) {
    return getMintSignature(this, xmrTxid);
  }

  getAllPendingSignatures() {
    return getAllPendingSignatures(this);
  }

  registerDepositRequest(ethAddress, paymentId) {
    return registerDepositRequest(this, ethAddress, paymentId);
  }

  getWithdrawalStatus(ethTxHash) {
    return getWithdrawalStatus(this, ethTxHash);
  }

  getAllPendingWithdrawals() {
    return getAllPendingWithdrawals(this);
  }

  async _handleWithdrawalSignRequest(data) {
    return handleWithdrawalSignRequest(this, data);
  }

  async checkEmergencySweep() {
    return checkEmergencySweep(this);
  }

  async dissolveStuckFinalizedCluster(clusterId) {
    return dissolveStuckFinalizedCluster(this, clusterId);
  }

  _updateEnv(key, value) {
    try {
      const envPath = path.join(__dirname, '.env');
      let envContent = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
      const regex = new RegExp(`^${key}=.*`, 'm');
      if (regex.test(envContent)) {
        envContent = envContent.replace(regex, `${key}=${value}`);
      } else {
        envContent += `\n${key}=${value}`;
      }
      fs.writeFileSync(envPath, envContent, { mode: 0o600 });
      process.env[key] = value;
      console.log(`[OK] Updated .env: ${key}=${value}`);
    } catch (e) {
      console.warn(`[WARN] Failed to update .env: ${e.message}`);
    }
  }

  async _maybeApplyMoneroHealthAdminCommand() {
    const flag = process.env.MONERO_HEALTH_CLEAR_QUARANTINE;
    if (!flag || flag === '0') {
      return;
    }

    console.log(
      `[MoneroHealth] Admin clear-quarantine flag detected: MONERO_HEALTH_CLEAR_QUARANTINE=${flag}`,
    );
    const wasQuarantined = this.moneroHealth === MoneroHealth.QUARANTINED;
    this._resetMoneroHealth();
    if (this.moneroErrorCounts) {
      this.moneroErrorCounts.kexRoundMismatch = 0;
      this.moneroErrorCounts.kexTimeout = 0;
      this.moneroErrorCounts.rpcFailure = 0;
    }
    if (wasQuarantined) {
      console.log('[MoneroHealth] Node was quarantined; clearing to HEALTHY via admin override.');
    } else {
      console.log(`[MoneroHealth] Admin override applied; health is now ${this.moneroHealth}.`);
    }

    process.env.MONERO_HEALTH_CLEAR_QUARANTINE = '0';
  }

  logClusterHealth() {
    try {
      console.log('[health] Cluster state snapshot:');
      console.log(`  Active cluster: ${this._activeClusterId || 'none'}`);
      console.log(`  Active members: ${this._clusterMembers ? this._clusterMembers.length : 0}`);
      console.log(`  Finalized: ${this._clusterFinalized ? 'yes' : 'no'}`);

      const failCount = Object.fromEntries(this._clusterFailures || new Map());
      const failMeta = this._clusterFailMeta || {};
      const blacklist = this._clusterBlacklist || {};
      const now = Date.now();

      const failEntries = Object.entries(failCount);
      if (!failEntries.length) {
        console.log('  Failures: none');
      } else {
        console.log('  Failures:');
        for (const [cid, count] of failEntries) {
          const meta = failMeta[cid] || {};
          const reachable = meta.reachable != null ? meta.reachable : 'unknown';
          const reason = meta.reason || 'unknown';
          console.log(`    ${cid}: ${count} failures (reachable=${reachable}, reason=${reason})`);
        }
      }

      const blEntries = Object.entries(blacklist);
      if (!blEntries.length) {
        console.log('  Blacklist: empty');
      } else {
        console.log('  Blacklist:');
        for (const [cid, until] of blEntries) {
          const remainingMs = until - now;
          const remainingMin = remainingMs > 0 ? Math.round(remainingMs / 60000) : 0;
          console.log(`    ${cid}: ${remainingMin}m remaining`);
        }
      }

      try {
        const health = this.moneroHealth || MoneroHealth.HEALTHY;
        console.log(`  Monero health: ${health}`);
        console.log('  Monero error counts:', JSON.stringify(this.moneroErrorCounts || {}));
      } catch (_ignored) {}
    } catch (_ignored) {}
  }
}

const isMainModule = import.meta.url === `file://${process.argv[1]}`;

if (isMainModule) {
  const node = new ZNode();

  const shutdown = async (signal) => {
    console.log(`\n[signal] ${signal} received, shutting down...`);
    try {
      await node.stop();
    } catch (e) {
      console.error('Shutdown error:', e.message || String(e));
    } finally {
      process.exit(0);
    }
  };

  process.on('uncaughtException', (err) => {
    if (err && (err.name === 'StreamStateError' || err.message?.includes('stream that is closed'))) {
      console.warn('[P2P] Stream closed during write (transient, ignoring):', err.message);
      return;
    }
    const errMsg = err instanceof Error ? err.message : String(err);
    const errStack = err instanceof Error ? err.stack : '';
    const errName = err instanceof Error ? err.name : typeof err;
    console.error(`Uncaught exception [${errName}]: ${errMsg}`);
    if (errStack) {
      console.error(errStack);
    }
    process.exit(1);
  });

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  node.start().catch((error) => {
    console.error('\n[ERROR] Fatal error:', error.message);
    process.exit(1);
  });
}

export default ZNode;
