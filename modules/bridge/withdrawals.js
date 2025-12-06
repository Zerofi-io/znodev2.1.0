import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { ethers } from 'ethers';
import { isValidMoneroAddress } from './bridge-service.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function normalizeEthersEvent(rawEvent) {
  if (!rawEvent) return null;

  const eventLike = rawEvent;
  const maybeLog = eventLike.log;

  const log = maybeLog && typeof maybeLog === 'object' && 'transactionHash' in maybeLog
    ? maybeLog
    : eventLike;

  const args = eventLike.args ?? log.args ?? null;

  const txHash = log && log.transactionHash ? log.transactionHash : null;
  const blockNumber =
    log && (typeof log.blockNumber === 'number' || typeof log.blockNumber === 'bigint')
      ? log.blockNumber
      : null;

  if (!txHash || !args) {
    return null;
  }

  return { log, args, txHash, blockNumber };
}

function describeEventForDebug(rawEvent) {
  try {
    if (!rawEvent || typeof rawEvent !== 'object') return String(rawEvent);
    const summary = {
      keys: Object.keys(rawEvent),
      hasLog: !!rawEvent.log,
      hasArgs: !!rawEvent.args,
    };
    if (rawEvent.log && typeof rawEvent.log === 'object') {
      summary.logKeys = Object.keys(rawEvent.log);
    }
    return JSON.stringify(summary);
  } catch (_ignored) {
    return '[unserializable event]';
  }
}

function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}

const REQUIRED_WITHDRAWAL_SIGNATURES = 7;
const MULTISIG_SYNC_ROUND = Number(process.env.MULTISIG_SYNC_ROUND || 9800);
const MULTISIG_SYNC_TIMEOUT_MS = Number(process.env.MULTISIG_SYNC_TIMEOUT_MS || 180000);
const WITHDRAWAL_SIGN_STEP_TIMEOUT_MS = Number(process.env.WITHDRAWAL_SIGN_STEP_TIMEOUT_MS || 20000);
const WITHDRAWAL_MAX_RETRY_WINDOW_MS = Number(process.env.WITHDRAWAL_MAX_RETRY_WINDOW_MS || 600000);
const WITHDRAWAL_RETRY_INTERVAL_MS = Number(process.env.WITHDRAWAL_RETRY_INTERVAL_MS || 30000);
const WITHDRAWAL_UNSIGNED_INFO_ROUND = Number(process.env.WITHDRAWAL_UNSIGNED_INFO_ROUND || 9898);

const WITHDRAWAL_PROCESSED_STORE_PATH = path.join(__dirname, '.withdrawals-processed.json');
let _processedWithdrawalsStore = null;
let _processedWithdrawalsSavePending = false;

function loadProcessedWithdrawalsStore() {
  if (_processedWithdrawalsStore) return _processedWithdrawalsStore;
  try {
    const raw = fs.readFileSync(WITHDRAWAL_PROCESSED_STORE_PATH, 'utf8');
    const arr = JSON.parse(raw);
    if (Array.isArray(arr)) {
      _processedWithdrawalsStore = new Set(arr.map((v) => String(v || '').toLowerCase()));
    } else {
      _processedWithdrawalsStore = new Set();
    }
  } catch (_ignored) {
    _processedWithdrawalsStore = new Set();
  }
  return _processedWithdrawalsStore;
}

function scheduleProcessedWithdrawalsStoreSave() {
  if (!_processedWithdrawalsStore || _processedWithdrawalsSavePending) return;
  _processedWithdrawalsSavePending = true;
  setTimeout(() => {
    try {
      const list = Array.from(_processedWithdrawalsStore);
      fs.writeFileSync(WITHDRAWAL_PROCESSED_STORE_PATH, JSON.stringify(list), { mode: 0o600 });
    } catch (e) {
      console.log('[Withdrawal] Failed to persist processed withdrawals:', e.message || String(e));
    }
    _processedWithdrawalsSavePending = false;
  }, 50);
}

function markWithdrawalProcessedPersistent(txHash) {
  const store = loadProcessedWithdrawalsStore();
  const key = String(txHash || '').toLowerCase();
  if (!key) return;
  if (store.has(key)) return;
  store.add(key);
  scheduleProcessedWithdrawalsStoreSave();
}

function markWithdrawalProcessedInMemory(node, txHash) {
  if (!node) return;
  const key = String(txHash || '').toLowerCase();
  if (!key) return;
  node._processedWithdrawals = node._processedWithdrawals || new Set();
  if (!node._processedWithdrawals.has(key)) {
    node._processedWithdrawals.add(key);
    markWithdrawalProcessedPersistent(key);
  }
}

const WITHDRAWAL_PENDING_STORE_PATH = path.join(__dirname, '.withdrawals-pending.json');
let _pendingWithdrawalsSavePending = false;

function loadPendingWithdrawalsStoreRaw() {
  try {
    const raw = fs.readFileSync(WITHDRAWAL_PENDING_STORE_PATH, 'utf8');
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch (_ignored) {
    return [];
  }
}

function hydratePendingWithdrawalsFromStore(node) {
  if (!node) return;
  const items = loadPendingWithdrawalsStoreRaw();
  if (!items || items.length === 0) return;
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  for (const item of items) {
    if (!item || !item.txHash || !item.data) continue;
    const key = String(item.txHash || '').toLowerCase();
    if (!key) continue;
    const data = { ...item.data };
    if (data && typeof data.xmrAmountAtomic === 'string') {
      try {
        // Store as BigInt for in-memory operations like retries
        data.xmrAmountAtomic = BigInt(data.xmrAmountAtomic);
      } catch (_ignored) {}
    }
    // Do not resurrect already completed withdrawals into the pending map
    if (data && data.status === 'completed') {
      markWithdrawalProcessedInMemory(node, key);
      continue;
    }
    node._pendingWithdrawals.set(key, data);
  }
}

function schedulePendingWithdrawalsStoreSave(node) {
  if (!node || !node._pendingWithdrawals || _pendingWithdrawalsSavePending) return;
  _pendingWithdrawalsSavePending = true;
  setTimeout(() => {
    try {
      const list = [];
      for (const [txHash, value] of node._pendingWithdrawals) {
        if (!value || typeof value !== 'object') continue;
        const entry = {};
        for (const [k, v] of Object.entries(value)) {
          if (typeof v === 'bigint') {
            entry[k] = v.toString();
          } else {
            entry[k] = v;
          }
        }
        list.push({ txHash, data: entry });
      }
      fs.writeFileSync(WITHDRAWAL_PENDING_STORE_PATH, JSON.stringify(list), { mode: 0o600 });
    } catch (e) {
      console.log('[Withdrawal] Failed to persist pending withdrawals:', e.message || String(e));
    }
    _pendingWithdrawalsSavePending = false;
  }, 50);
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
  try {
    const persisted = loadProcessedWithdrawalsStore();
    for (const h of persisted) node._processedWithdrawals.add(h);
  } catch (_ignored) {}
  hydratePendingWithdrawalsFromStore(node);
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  if (node._withdrawalRetryTimer) clearInterval(node._withdrawalRetryTimer);
  node._withdrawalRetryTimer = setInterval(() => { tickPendingWithdrawals(node); }, WITHDRAWAL_RETRY_INTERVAL_MS);
  console.log('[Withdrawal] Starting withdrawal event monitor...');
  try {
    const activeClusterId = node._activeClusterId;
    if (!activeClusterId) {
      console.log('[Withdrawal] Cannot start withdrawal monitor: missing activeClusterId');
      node._withdrawalMonitorRunning = false;
      return;
    }
    const filter = node.bridge.filters.TokensBurned();
    const currentBlock = await node.provider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - 300);
    try {
      const pastEvents = await node.bridge.queryFilter(filter, fromBlock, currentBlock);
      for (const event of pastEvents) await handleBurnEvent(node, event);
    } catch (e) {
      console.log('[Withdrawal] Failed to query past burn events:', e.message || String(e));
    }
    node.bridge.on('TokensBurned', async (...args) => {
      const event = args[args.length - 1];
      await handleBurnEvent(node, event);
    });
    console.log('[Withdrawal] Now listening for TokensBurned events');
    setupWithdrawalClaimListener(node);
  } catch (e) {
    console.log('[Withdrawal] Failed to setup event listener:', e.message || String(e));
    node._withdrawalMonitorRunning = false;
  }
}

export function stopWithdrawalMonitor(node) {
  node._withdrawalMonitorRunning = false;
  if (node._withdrawalRetryTimer) {
    clearInterval(node._withdrawalRetryTimer);
    node._withdrawalRetryTimer = null;
  }
  if (node.bridge) {
    try { node.bridge.removeAllListeners('TokensBurned'); }
    catch (_ignored) {}
  }
  console.log('[Withdrawal] Withdrawal monitor stopped');
}

function setupWithdrawalClaimListener(node) {
  if (!node.p2p || node._withdrawalClaimListenerSetup) return;
  node._withdrawalClaimListenerSetup = true;
  // Legacy: this function used to process withdrawal-claim broadcasts on round 9899.
  // The current pipeline relies on deterministic executor selection and multisig/PBFT,
  // so we only need to ensure the related listeners are wired once.
  setupMultisigSyncResponder(node);
  setupWithdrawalSignStepListener(node);
  setupWithdrawalUnsignedProposalListener(node);
}

function setupWithdrawalUnsignedProposalListener(node) {
  if (!node.p2p || node._withdrawalUnsignedProposalListenerSetup) return;
  node._withdrawalUnsignedProposalListenerSetup = true;

  const pollUnsignedProposals = async () => {
    if (!node._withdrawalMonitorRunning) return;
    if (!node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
      if (node._withdrawalMonitorRunning) setTimeout(pollUnsignedProposals, 3000);
      return;
    }
    try {
      const payloads = await node.p2p.getPeerPayloads(
        node._activeClusterId,
        node._sessionId || 'bridge',
        WITHDRAWAL_UNSIGNED_INFO_ROUND,
        node._clusterMembers,
      );
      if (payloads && payloads.length > 0) {
        for (const raw of payloads) {
          let data;
          try {
            data = JSON.parse(raw);
          } catch (_ignored) {
            continue;
          }
          if (!data || data.type !== 'withdrawal-unsigned-proposal' || !data.txHash) continue;
          const txHash = String(data.txHash);
          if (!txHash) continue;
          const unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(raw));
          (async () => {
            try {
              await runUnsignedWithdrawalTxConsensus(node, txHash, unsignedHash);
            } catch (e) {
              console.log('[Withdrawal] Unsigned-tx PBFT error (listener):', e.message || String(e));
            }
          })();
        }
      }
    } catch (_ignored) {}
    if (node._withdrawalMonitorRunning) setTimeout(pollUnsignedProposals, 3000);
  };
  pollUnsignedProposals();
}

function setupMultisigSyncResponder(node) {
  if (!node.p2p || node._multisigSyncResponderSetup) return;
  node._multisigSyncResponderSetup = true;

  const pollMultisigSync = async () => {
    if (!node._withdrawalMonitorRunning) return;
    if (node._withdrawalMultisigLock) {
      if (node._withdrawalMonitorRunning) setTimeout(pollMultisigSync, 3000);
      return;
    }
    try {
      const payloads = await node.p2p.getPeerPayloads(
        node._activeClusterId,
        node._sessionId || 'bridge',
        MULTISIG_SYNC_ROUND,
        node._clusterMembers,
      );

      let shouldRespond = false;
      if (payloads && payloads.length > 0 && node.wallet && node.wallet.address && node.monero) {
        const self = String(node.wallet.address).toLowerCase();
        for (const raw of payloads) {
          let data;
          try {
            data = JSON.parse(raw);
          } catch (_ignored) {
            continue;
          }
          if (!data || data.type !== 'multisig-info' || !data.sender) continue;
          const senderLc = String(data.sender).toLowerCase();
          if (!senderLc || senderLc === self) continue;
          shouldRespond = true;
          break;
        }
      }

      if (shouldRespond) {
        try {
          const localInfo = await node.monero.exportMultisigInfo();
          const payload = JSON.stringify({
            type: 'multisig-info',
            clusterId: node._activeClusterId,
            info: localInfo,
            sender: node.wallet.address,
          });
          await node.p2p.broadcastRoundData(
            node._activeClusterId,
            node._sessionId || 'bridge',
            MULTISIG_SYNC_ROUND,
            payload,
          );
          console.log('[MultisigSync] Responded to multisig sync request from peer');
        } catch (e) {
          console.log('[MultisigSync] Failed to respond to sync request:', e.message || String(e));
        }
      }
    } catch (_ignored) {}
    if (node._withdrawalMonitorRunning) setTimeout(pollMultisigSync, 3000);
  };
  pollMultisigSync();
}

function setupWithdrawalSignStepListener(node) {
  if (!node.p2p || node._withdrawalSignStepListenerSetup) return;
  node._withdrawalSignStepListenerSetup = true;
  node._handledSignSteps = node._handledSignSteps || new Set();
  const pollSignSteps = async () => {
    if (!node._withdrawalMonitorRunning) return;
    if (!node._withdrawalSignInitialSyncDone) {
      node._withdrawalSignInitialSyncDone = true;
      try {
        console.log('[Withdrawal] Running initial multisig sync before servicing sign-step requests...');
        const syncResult = await runMultisigInfoSync(node);
        if (!syncResult || !syncResult.success) {
          console.log(
            '[Withdrawal] Initial multisig sync before sign-step handling failed:',
            syncResult && syncResult.reason ? syncResult.reason : 'unknown',
          );
        } else {
          console.log(
            `[Withdrawal] Initial multisig sync before sign-step handling completed; importedOutputs=${
              typeof syncResult.importedOutputs === 'number'
                ? syncResult.importedOutputs
                : 'unknown'
            }`,
          );
        }
      } catch (e) {
        console.log(
          '[Withdrawal] Initial multisig sync before sign-step handling error:',
          e.message || String(e),
        );
      }
    }
    try {
      const payloads = await node.p2p.getPeerPayloads(
        node._activeClusterId,
        node._sessionId || 'bridge',
        9900,
        node._clusterMembers,
      );
      if (payloads && payloads.length > 0) {
        const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : '';
        for (const raw of payloads) {
          try {
            const data = JSON.parse(raw);
            if (!data || data.type !== 'withdrawal-sign-step-request') continue;
            if (!data.withdrawalTxHash || !data.signer || !data.txDataHex) continue;
            const signerLc = String(data.signer).toLowerCase();
            if (!self || signerLc !== self) continue;
            const idx = Number(data.stepIndex);
            if (!Number.isFinite(idx) || idx < 0) continue;
            const key = `${String(data.withdrawalTxHash).toLowerCase()}:${idx}`;
            if (node._handledSignSteps.has(key)) continue;

            // Ensure we have a fresh, per-withdrawal multisig sync on this node before signing.
            try {
              const syncBeforeSign = await runWithdrawalMultisigSync(node, data.withdrawalTxHash);
              if (!syncBeforeSign || !syncBeforeSign.success) {
                console.log(
                  '[Withdrawal] Multisig sync before sign-step handling failed:',
                  syncBeforeSign && syncBeforeSign.reason ? syncBeforeSign.reason : 'unknown',
                );
              }
            } catch (e) {
              console.log(
                '[Withdrawal] Multisig sync before sign-step handling error:',
                e.message || String(e),
              );
            }

            let signedTx;
            try {
              signedTx = await node.monero.signMultisig(data.txDataHex);
            } catch (e) {
              const msg = e.message || String(e);
              if (msg.includes('stale data') || msg.includes('export fresh multisig data')) {
                console.log('[Withdrawal] Sign step stale multisig data, notifying coordinator');
                try {
                  const notice = JSON.stringify({
                    type: 'withdrawal-sign-step-stale',
                    withdrawalTxHash: data.withdrawalTxHash,
                    stepIndex: idx,
                    signer: node.wallet.address,
                  });
                  await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', 9901, notice);
                } catch (_ignored) {}
              } else {
                console.log('[Withdrawal] Failed to sign step tx:', msg);
              }
              node._handledSignSteps.add(key);
              continue;
            }
            if (!signedTx || !signedTx.txDataHex) {
              node._handledSignSteps.add(key);
              continue;
            }
            const response = JSON.stringify({
              type: 'withdrawal-sign-step-signed',
              withdrawalTxHash: data.withdrawalTxHash,
              stepIndex: idx,
              signer: node.wallet.address,
              txDataHex: signedTx.txDataHex,
            });
            try {
              await node.p2p.broadcastRoundData(node._activeClusterId, node._sessionId || 'bridge', 9901, response);
            } catch (_ignored) {}
            recordLocalSignStepResponse(node, data.withdrawalTxHash, idx, node.wallet.address, signedTx.txDataHex);
            node._handledSignSteps.add(key);
          } catch (_ignored) {}
        }
      }
    } catch (_ignored) {}
    if (node._withdrawalMonitorRunning) setTimeout(pollSignSteps, 3000);
  };
  pollSignSteps();
}

async function handleBurnEvent(node, event) {
  try {
    const normalized = normalizeEthersEvent(event);
    if (!normalized) {
      console.log('[Withdrawal] Burn event missing expected fields after normalization, skipping');
      console.log('[Withdrawal] Burn event debug:', describeEventForDebug(event));
      return;
    }
    const { log, args, txHash, blockNumber } = normalized;
    node._processedWithdrawals = node._processedWithdrawals || new Set();
    const key = txHash.toLowerCase();
    if (node._processedWithdrawals.has(key)) return;
    const activeClusterId = node._activeClusterId;
    if (!activeClusterId) {
      console.log('[Withdrawal] No activeClusterId set, skipping burn event');
      return;
    }
    const user = args[0] ?? args.user;
    const clusterIdFromEvent = args[1] ?? args.clusterId;
    const xmrAddress = args[2] ?? args.xmrAddress;
    const amount = args[3] ?? args.burnAmount ?? args.amount;
    const fee = args[4] ?? args.fee ?? 0n;
    let markProcessed = false;
    if (amount == null) {
      console.log('[Withdrawal] Burn event missing amount, skipping');
      markWithdrawalProcessedInMemory(node, key);
      return;
    }
    console.log('[Withdrawal] TokensBurned event detected:');
    console.log(`  TX: ${txHash}`);
    console.log(`  User: ${user}`);
    console.log(`  Cluster ID: ${clusterIdFromEvent}`);
    console.log(`  XMR Address: ${xmrAddress}`);
    try {
      console.log(`  Amount: ${ethers.formatEther(amount)} zXMR`);
    } catch (e) {
      console.log('[Withdrawal] Failed to format amount for logging:', e.message || String(e));
    }
    try {
      console.log(`  Fee: ${ethers.formatEther(fee)} zXMR`);
    } catch (e) {
      console.log('[Withdrawal] Failed to format fee for logging:', e.message || String(e));
    }
    if (
      typeof clusterIdFromEvent === 'string' &&
      typeof activeClusterId === 'string' &&
      clusterIdFromEvent.toLowerCase() !== activeClusterId.toLowerCase()
    ) {
      console.log(
        `[Withdrawal] Burn event clusterId mismatch (event=${clusterIdFromEvent}, local=${activeClusterId}), skipping`,
      );

      markWithdrawalProcessedInMemory(node, key);
      return;
    }
    if (!isValidMoneroAddress(xmrAddress)) {
      console.log(`[Withdrawal] Invalid Monero address, skipping: ${xmrAddress}`);
      markWithdrawalProcessedInMemory(node, key);
      return;
    }
    const xmrAtomicAmount = BigInt(amount) / BigInt(1e6);
    const withdrawal = {
      txHash,
      user,
      xmrAddress,
      zxmrAmount: amount.toString(),
      xmrAmount: xmrAtomicAmount.toString(),
      xmrAmountAtomic: xmrAtomicAmount,
      blockNumber,
      timestamp: Date.now(),
    };
    try {
      const consensusResult = await runWithdrawalConsensus(node, withdrawal);
      if (consensusResult.success) {
        console.log(`[Withdrawal] Consensus reached for withdrawal ${txHash.slice(0, 18)}...`);
        await executeWithdrawalWithCascade(node, withdrawal);
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

async function runUnsignedWithdrawalTxConsensus(node, txHash, unsignedHash) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no cluster members' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const shortEth = getWithdrawalShortHash(txHash);
  const phase = `withdrawal-tx-${shortEth}`;
  node._unsignedTxConsensusStarted = node._unsignedTxConsensusStarted || new Set();
  if (node._unsignedTxConsensusStarted.has(phase)) {
    return { success: true, reason: 'already_started' };
  }
  node._unsignedTxConsensusStarted.add(phase);
  const timeoutMs = Number(process.env.WITHDRAWAL_TX_PBFT_TIMEOUT_MS || process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);
  try {
    const result = await node.p2p.runConsensus(
      clusterId,
      sessionId,
      phase,
      unsignedHash,
      node._clusterMembers,
      timeoutMs,
    );
    if (result && result.success) return { success: true };
    return { success: false, reason: result ? result.reason : 'unknown' };
  } catch (e) {
    return { success: false, reason: e.message || String(e) };
  }
}

function getWithdrawalTurnIndex(node) {
  if (!node._clusterMembers || node._clusterMembers.length === 0 || !node.wallet || !node.wallet.address) return -1;
  const self = String(node.wallet.address || '').toLowerCase();
  const members = node._clusterMembers.map((a) => String(a || '').toLowerCase());
  return members.indexOf(self);
}

function getCanonicalSignerSet(node) {
  if (!node._clusterMembers || node._clusterMembers.length === 0) return [];
  return node._clusterMembers.map((a) => String(a || '').toLowerCase());
}

function getWithdrawalShortHash(txHash) {
  if (!txHash) return '';
  return String(txHash).slice(0, 18);
}

async function getHealthyClusterMembers(node) {
  if (!node || !node.p2p || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) return [];
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const members = node._clusterMembers.map((a) => String(a || '').toLowerCase());
  const seen = new Set();
  const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : null;
  if (self && members.includes(self)) seen.add(self);

  const collectFromRound = async (round, extractAddress) => {
    try {
      const payloads = await node.p2p.getPeerPayloads(clusterId, sessionId, round, node._clusterMembers);
      if (payloads && payloads.length > 0) {
        for (const raw of payloads) {
          try {
            const data = JSON.parse(raw);
            const addr = extractAddress(data);
            if (addr) {
              const lc = String(addr).toLowerCase();
              if (members.includes(lc)) seen.add(lc);
            }
          } catch (_ignored) {}
        }
      }
    } catch (_ignored) {}
  };

  await collectFromRound(
    MULTISIG_SYNC_ROUND,
    (data) => (data && data.type === 'multisig-info' && data.sender ? data.sender : null),
  );
  const healthy = [];
  for (const addr of node._clusterMembers) {
    const lc = String(addr || '').toLowerCase();
    if (seen.has(lc)) healthy.push(addr);
  }
  return healthy;
}

function isRetriableWithdrawal(pending) {
  if (!pending) return false;
  const status = String(pending.status || '');
  const err = String(pending.error || '');
  if (status === 'completed' || status === 'pending') return false;
  if (status === 'signing_failed' || status === 'sign_failed') return true;
  if (err === 'tx_pbft_error' || err === 'tx_pbft_failed') return true;
  if (err.startsWith('multisig_sync_failed:')) return true;
  if (!err && (status === 'failed' || status === 'submit_failed' || status === 'error')) return true;
  return false;
}

function buildWithdrawalFromPending(key, data) {
  const txHash = data.txHash || key;
  let xmrAmountAtomic = data.xmrAmountAtomic;
  if (typeof xmrAmountAtomic === 'string') {
    try { xmrAmountAtomic = BigInt(xmrAmountAtomic); } catch (_ignored) { xmrAmountAtomic = 0n; }
  }
  if (typeof xmrAmountAtomic !== 'bigint') {
    try { xmrAmountAtomic = BigInt(data.xmrAmount || '0'); } catch (_ignored) { xmrAmountAtomic = 0n; }
  }
  const xmrAmountStr = typeof data.xmrAmount === 'string' ? data.xmrAmount : xmrAmountAtomic.toString();
  return {
    txHash,
    user: data.user,
    xmrAddress: data.xmrAddress,
    zxmrAmount: data.zxmrAmount || xmrAmountStr,
    xmrAmount: xmrAmountStr,
    xmrAmountAtomic,
    blockNumber: data.blockNumber,
    timestamp: data.timestamp || data.startedAt || Date.now(),
  };
}

function recordLocalSignStepResponse(node, withdrawalTxHash, stepIndex, signer, txDataHex) {
  if (!node) return;
  const k = `${String(withdrawalTxHash || '').toLowerCase()}:${Number(stepIndex)}`;
  if (!k) return;
  if (!node._localSignStepResponses) node._localSignStepResponses = new Map();
  node._localSignStepResponses.set(k, { signer, txDataHex, timestamp: Date.now() });
}

function clearLocalSignStepCacheForWithdrawal(node, withdrawalTxHash) {
  if (!node || !withdrawalTxHash) return;
  const prefix = `${String(withdrawalTxHash || '').toLowerCase()}:`;
  if (node._localSignStepResponses) {
    for (const k of node._localSignStepResponses.keys()) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._localSignStepResponses.delete(k);
      }
    }
  }
  if (node._handledSignSteps) {
    for (const k of Array.from(node._handledSignSteps)) {
      if (typeof k === 'string' && k.startsWith(prefix)) {
        node._handledSignSteps.delete(k);
      }
    }
  }
}

function tickPendingWithdrawals(node) {
  if (!node || !node._pendingWithdrawals || node._pendingWithdrawals.size === 0) return;
  const now = Date.now();
  for (const [key, data] of node._pendingWithdrawals) {
    if (!data) continue;
    if (data.status === 'completed') {
      clearLocalSignStepCacheForWithdrawal(node, key);
      markWithdrawalProcessedInMemory(node, key);
      node._pendingWithdrawals.delete(key);
      continue;
    }
    const started = typeof data.startedAt === 'number' ? data.startedAt : now;
    if (typeof data.firstAttemptAt !== 'number') {
      data.firstAttemptAt = started;
    }
    const ageMs = now - data.firstAttemptAt;
    if (ageMs > WITHDRAWAL_MAX_RETRY_WINDOW_MS) {
      if (!data.permanentFailure) {
        data.permanentFailure = true;
        if (!data.error) data.error = 'withdrawal_failed_after_retry_window';
        if (!data.status || data.status === 'pending') data.status = 'failed_permanent';
        console.log(
          `[Withdrawal] Giving up on withdrawal ${key.slice(0, 18)} after ${Math.floor(
            ageMs / 1000,
          )}s; marking permanently failed`,
        );
      }
      clearLocalSignStepCacheForWithdrawal(node, key);
      markWithdrawalProcessedInMemory(node, key);
      continue;
    }
    if (!isRetriableWithdrawal(data)) continue;
    if (data.retryInProgress) continue;
    const last = typeof data.lastAttemptAt === 'number' ? data.lastAttemptAt : started;
    if (now - last < WITHDRAWAL_RETRY_INTERVAL_MS) continue;
    const withdrawal = buildWithdrawalFromPending(key, data);
    data.retryInProgress = true;
    data.lastAttemptAt = now;
    (async () => {
      try {
        await executeWithdrawalWithCascade(node, withdrawal);
      } catch (e) {
        console.log(
          '[Withdrawal] Retry execution error for withdrawal',
          key.slice(0, 18),
          e.message || String(e),
        );
      } finally {
        const current = node._pendingWithdrawals && node._pendingWithdrawals.get(key);
        if (current) current.retryInProgress = false;
      }
    })();
  }
  schedulePendingWithdrawalsStoreSave(node);
}

async function runMultisigInfoSync(node) {
  if (!node || !node.monero || !node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster_or_monero' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const totalMembers = node._clusterMembers.length;
  const minSigners = REQUIRED_WITHDRAWAL_SIGNATURES;
  const windowMs = Number(process.env.MULTISIG_SYNC_WINDOW_MS || 10000);

  if (totalMembers < minSigners) {
    console.log(
      `[MultisigSync] Cluster has too few members for required withdrawal signatures: total=${totalMembers}, required=${minSigners}`,
    );
    const responders = new Set();
    if (node.wallet && node.wallet.address) {
      responders.add(node.wallet.address.toLowerCase());
    }
    return {
      success: false,
      reason: 'cluster_too_small',
      importedOutputs: 0,
      responderCount: responders.size,
      totalMembers,
      responders: Array.from(responders),
    };
  }

  let localInfo;
  try {
    localInfo = await node.monero.exportMultisigInfo();
  } catch (e) {
    console.log('[MultisigSync] export_multisig_info failed:', e.message || String(e));
    const responders = [];
    if (node.wallet && node.wallet.address) {
      responders.push(node.wallet.address.toLowerCase());
    }
    return { success: false, reason: 'export_failed', importedOutputs: 0, responderCount: responders.length, totalMembers, responders };
  }

  const selfAddress = node.wallet && node.wallet.address ? node.wallet.address.toLowerCase() : null;
  const responders = new Set();
  if (selfAddress) responders.add(selfAddress);

  const payload = JSON.stringify({
    type: 'multisig-info',
    clusterId,
    info: localInfo,
    sender: node.wallet && node.wallet.address ? node.wallet.address : null,
  });

  try {
    await node.p2p.broadcastRoundData(clusterId, sessionId, MULTISIG_SYNC_ROUND, payload);
  } catch (e) {
    console.log('[MultisigSync] Failed to broadcast multisig info:', e.message || String(e));
  }

  const seenInfosBySender = new Map();
  const start = Date.now();

  while (true) {
    const elapsed = Date.now() - start;
    try {
      const peerPayloads = await node.p2p.getPeerPayloads(
        clusterId,
        sessionId,
        MULTISIG_SYNC_ROUND,
        node._clusterMembers,
      );
      if (peerPayloads && peerPayloads.length > 0) {
        for (const raw of peerPayloads) {
          try {
            const data = JSON.parse(raw);
            if (data && data.type === 'multisig-info' && typeof data.info === 'string' && data.info.length > 0) {
              const sender = (data.sender || '').toLowerCase();
              if (sender) {
                responders.add(sender);
                if (!seenInfosBySender.has(sender)) {
                  seenInfosBySender.set(sender, data.info);
                }
              }
            }
          } catch (_ignored) {}
        }
      }
    } catch (e) {
      console.log('[MultisigSync] Error while polling multisig info payloads:', e.message || String(e));
    }

    const responderCount = responders.size;
    if (responderCount >= totalMembers) {
      console.log(`[MultisigSync] All ${totalMembers}/${totalMembers} participants responded; finishing early`);
      break;
    }
    if (elapsed >= windowMs) {
      console.log(`[MultisigSync] Sync window expired after ${Math.round(elapsed / 1000)}s: responders=${responderCount}/${totalMembers}`);
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  const responderCount = responders.size;
  if (responderCount < minSigners) {
    console.log(
      `[MultisigSync] Not enough participants for multisig sync: responders=${responderCount}/${totalMembers}, required=${minSigners}`,
    );
    return {
      success: false,
      reason: 'not_enough_participants',
      importedOutputs: 0,
      responderCount,
      totalMembers,
      responders: Array.from(responders),
    };
  }

  const infos = Array.from(seenInfosBySender.values());
  if (infos.length === 0) {
    console.log('[MultisigSync] No valid multisig info entries parsed from peer payloads');
    return {
      success: false,
      reason: 'no_valid_infos',
      importedOutputs: 0,
      responderCount,
      totalMembers,
      responders: Array.from(responders),
    };
  }

  let importedOutputs = 0;
  try {
    const n = await node.monero.importMultisigInfo(infos);
    if (typeof n === 'number') importedOutputs = n;
  } catch (e) {
    console.log('[MultisigSync] import_multisig_info failed:', e.message || String(e));
    return {
      success: false,
      reason: 'import_failed',
      importedOutputs: 0,
      responderCount,
      totalMembers,
      responders: Array.from(responders),
    };
  }

  let finalBalance = null;
  try {
    finalBalance = await node.monero.getBalance();
  } catch (e) {
    console.log('[MultisigSync] Failed to get balance after multisig sync:', e.message || String(e));
  }

  if (finalBalance && finalBalance.multisigImportNeeded) {
    console.log('[MultisigSync] multisig_import_needed still true after sync attempt');
    return {
      success: false,
      reason: 'multisig_import_needed_still_true',
      importedOutputs,
      responderCount,
      totalMembers,
      responders: Array.from(responders),
    };
  }

  return {
    success: true,
    importedOutputs,
    responderCount,
    totalMembers,
    responders: Array.from(responders),
  };
}


async function runMultisigSyncConsensus(node, withdrawalTxHash, timeoutMs) {
  if (!node || !node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return { success: false, reason: 'no_cluster' };
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(withdrawalTxHash || '').toLowerCase();
  const shortEth = getWithdrawalShortHash(key);
  const payload = JSON.stringify({
    type: 'withdrawal-multisig-sync',
    withdrawalTxHash: key,
  });
  const digest = ethers.keccak256(ethers.toUtf8Bytes(payload));
  const phase = `withdrawal-ms-sync-${shortEth}`;
  const msTimeout = typeof timeoutMs === 'number' && Number.isFinite(timeoutMs) ? timeoutMs : MULTISIG_SYNC_TIMEOUT_MS;
  try {
    const result = await node.p2p.runConsensus(
      clusterId,
      sessionId,
      phase,
      digest,
      node._clusterMembers,
      msTimeout,
    );
    if (result && result.success) return { success: true };
    return { success: false, reason: result && result.reason ? result.reason : 'pbft_failed' };
  } catch (e) {
    console.log('[MultisigSync] PBFT error for multisig sync:', e.message || String(e));
    return { success: false, reason: 'pbft_error' };
  }
}

async function runWithdrawalMultisigSync(node, withdrawalTxHash) {
  if (!node) return { success: false, reason: 'no_node' };
  const key = String(withdrawalTxHash || '').toLowerCase();
  if (!key) return { success: false, reason: 'no_tx_hash' };

  const consensus = await runMultisigSyncConsensus(node, key, MULTISIG_SYNC_TIMEOUT_MS);
  if (!consensus || !consensus.success) {
    return { success: false, reason: (consensus && consensus.reason) || 'multisig_sync_pbft_failed' };
  }

  const syncResult = await runMultisigInfoSync(node);
  if (!syncResult || !syncResult.success) {
    return { success: false, reason: (syncResult && syncResult.reason) || 'multisig_sync_failed' };
  }

  const respondersArr = Array.isArray(syncResult.responders) ? syncResult.responders : [];
  const respondersLc = respondersArr
    .map((a) => (a ? String(a).toLowerCase() : ''))
    .filter((a) => a && typeof a === 'string');
  const responderSet = new Set(respondersLc);
  const members = Array.isArray(node._clusterMembers)
    ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
    : [];
  const signerSet = [];
  for (const m of members) {
    if (responderSet.has(m)) signerSet.push(m);
  }

  const minSigners = REQUIRED_WITHDRAWAL_SIGNATURES;
  if (!signerSet.length || signerSet.length < minSigners) {
    return {
      success: false,
      reason: 'not_enough_synced_signers',
      signerSet,
      importedOutputs: syncResult.importedOutputs || 0,
      responderCount: syncResult.responderCount || signerSet.length,
      totalMembers: syncResult.totalMembers || members.length,
    };
  }

  return {
    success: true,
    signerSet,
    importedOutputs: syncResult.importedOutputs || 0,
    responderCount: syncResult.responderCount || signerSet.length,
    totalMembers: syncResult.totalMembers || members.length,
  };
}

async function waitForSignStepResponse(node, withdrawalTxHash, stepIndex, signer, timeoutMs) {
  if (!node.p2p || !node._activeClusterId || !Array.isArray(node._clusterMembers) || node._clusterMembers.length === 0) {
    return null;
  }
  const clusterId = node._activeClusterId;
  const sessionId = node._sessionId || 'bridge';
  const key = String(withdrawalTxHash || '').toLowerCase();
  const signerLc = String(signer || '').toLowerCase();
  const start = Date.now();
  const localKey = `${key}:${Number(stepIndex)}`;

  while (Date.now() - start < timeoutMs) {

    if (node._localSignStepResponses && node._localSignStepResponses.has(localKey)) {
      const cached = node._localSignStepResponses.get(localKey);
      if (cached && cached.txDataHex && typeof cached.txDataHex === 'string' && cached.txDataHex.length) {
        const s2 = cached.signer ? String(cached.signer).toLowerCase() : '';
        if (!signerLc || !s2 || s2 === signerLc) {
          return { txDataHex: cached.txDataHex };
        }
      }
    }

    let payloads = [];
    try {
      payloads = await node.p2p.getPeerPayloads(
        clusterId,
        sessionId,
        9901,
        node._clusterMembers,
      );
    } catch (_ignored) {}
    if (payloads && payloads.length > 0) {
      for (const raw of payloads) {
        try {
          const data = JSON.parse(raw);
          if (!data || !data.type) continue;
          if (data.type === 'withdrawal-sign-step-stale') {
            if (!data.withdrawalTxHash) continue;
            if (String(data.withdrawalTxHash).toLowerCase() !== key) continue;
            const idx = Number(data.stepIndex);
            if (!Number.isFinite(idx) || idx !== stepIndex) continue;
            const s2 = data.signer ? String(data.signer).toLowerCase() : '';
            if (!s2 || (signerLc && s2 !== signerLc)) continue;
            return { stale: true };
          }
          if (data.type !== 'withdrawal-sign-step-signed') continue;
          if (!data.withdrawalTxHash) continue;
          if (String(data.withdrawalTxHash).toLowerCase() !== key) continue;
          const idx = Number(data.stepIndex);
          if (!Number.isFinite(idx) || idx !== stepIndex) continue;
          const s2 = data.signer ? String(data.signer).toLowerCase() : '';
          if (!s2 || (signerLc && s2 !== signerLc)) continue;
          if (!data.txDataHex || typeof data.txDataHex !== 'string' || !data.txDataHex.length) continue;
          return { txDataHex: data.txDataHex };
        } catch (_ignored) {}
      }
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return null;
}

async function executeWithdrawalWithCascade(node, withdrawal) {
  const healthyMembers = await getHealthyClusterMembers(node);
  const members = Array.isArray(healthyMembers) && healthyMembers.length > 0
    ? healthyMembers.map((a) => String(a || '').toLowerCase())
    : (Array.isArray(node._clusterMembers)
        ? node._clusterMembers.map((a) => String(a || '').toLowerCase())
        : []);
  if (!Array.isArray(members) || members.length === 0) return;

  const self = node.wallet && node.wallet.address ? String(node.wallet.address).toLowerCase() : null;
  if (!self || !members.includes(self)) return;

  const key = withdrawal.txHash.toLowerCase();
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const existing = node._pendingWithdrawals.get(key) || {};
  node._pendingWithdrawals.set(key, existing);
  schedulePendingWithdrawalsStoreSave(node);

  console.log(
    `[Withdrawal] Starting withdrawal execution attempt on this node for ${withdrawal.txHash.slice(0, 18)}...`,
  );
  try {
    await executeWithdrawal(node, withdrawal);
  } catch (e) {
    console.log('[Withdrawal] Error executing withdrawal:', e.message || String(e));
  }
}

async function executeWithdrawal(node, withdrawal) {
  console.log(`[Withdrawal] Executing withdrawal to ${withdrawal.xmrAddress}`);
  console.log(`  Amount: ${Number(withdrawal.xmrAmount) / 1e12} XMR`);
  node._pendingWithdrawals = node._pendingWithdrawals || new Map();
  const key = withdrawal.txHash.toLowerCase();
  node._withdrawalMultisigLock = key;
  const shortEth = getWithdrawalShortHash(withdrawal.txHash);

  try {
    clearLocalSignStepCacheForWithdrawal(node, key);
    const existing = node._pendingWithdrawals.get(key);
    const firstAttemptAt =
      existing && typeof existing.firstAttemptAt === 'number'
        ? existing.firstAttemptAt
        : existing && typeof existing.startedAt === 'number'
          ? existing.startedAt
          : Date.now();
    node._pendingWithdrawals.set(key, {
      ...withdrawal,
      status: 'pending',
      startedAt: Date.now(),
      firstAttemptAt,
    });
    const pending0 = node._pendingWithdrawals.get(key);
    if (pending0) {
      pending0.signerSet = null;
      pending0.txDataHexUnsigned = null;
      pending0.txDataHexSigned = null;
      pending0.pbftUnsignedHash = null;
      pending0.pbftSignedHash = null;
    }

    const syncResult = await runWithdrawalMultisigSync(node, withdrawal.txHash);
    if (!syncResult || !syncResult.success) {
      console.log(
        '[Withdrawal] Withdrawal multisig sync failed (PBFT-coordinated multisig-info sync):',
        syncResult && syncResult.reason ? syncResult.reason : 'unknown',
      );
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = `multisig_sync_failed:${(syncResult && syncResult.reason) || 'unknown'}`;
      }
      return;
    }
    console.log(
      `[Withdrawal] Withdrawal multisig sync completed before transfer; importedOutputs=${
        typeof syncResult.importedOutputs === 'number' ? syncResult.importedOutputs : 'unknown'
      }`,
    );
    const pendingAfterSync = node._pendingWithdrawals.get(key);
    if (pendingAfterSync && Array.isArray(syncResult.signerSet) && syncResult.signerSet.length > 0) {
      pendingAfterSync.signerSet = syncResult.signerSet.map((a) => String(a || '').toLowerCase());
    }

    console.log('[Withdrawal] Creating multisig transfer transaction...');
    const destinations = [{ address: withdrawal.xmrAddress, amount: withdrawal.xmrAmountAtomic }];
    let txData;
    try {
      txData = await node.monero.transfer(destinations);
      console.log('[Withdrawal] Transfer transaction created');
      const baseSignerSet =
        pendingAfterSync && Array.isArray(pendingAfterSync.signerSet) && pendingAfterSync.signerSet.length > 0
          ? pendingAfterSync.signerSet.map((a) => String(a || '').toLowerCase())
          : getCanonicalSignerSet(node);
      const signerSet = baseSignerSet;
      if (!signerSet || signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
        console.log('[Withdrawal] Not enough signers available for PBFT tx commit');
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = 'not_enough_signers_for_tx_pbft';
        }
        return;
      }
      const unsignedPayload = JSON.stringify({
        type: 'withdrawal-unsigned-proposal',
        txHash: withdrawal.txHash,
        txDataHex: txData.txDataHex,
        signerSet,
        threshold: REQUIRED_WITHDRAWAL_SIGNATURES,
      });
      const unsignedHash = ethers.keccak256(ethers.toUtf8Bytes(unsignedPayload));
      const clusterId = node._activeClusterId;
      const sessionId = node._sessionId || 'bridge';
      const pendingUnsigned = node._pendingWithdrawals.get(key);
      if (pendingUnsigned) {
        pendingUnsigned.signerSet = signerSet;
        pendingUnsigned.txDataHexUnsigned = txData.txDataHex;
        pendingUnsigned.pbftUnsignedHash = unsignedHash;
      }
      try {
        try {
          await node.p2p.broadcastRoundData(
            clusterId,
            sessionId,
            WITHDRAWAL_UNSIGNED_INFO_ROUND,
            unsignedPayload,
          );
        } catch (_ignored) {}
        const txConsensus = await runUnsignedWithdrawalTxConsensus(
          node,
          withdrawal.txHash,
          unsignedHash,
        );
        if (!txConsensus || !txConsensus.success) {
          const reason = txConsensus && txConsensus.reason ? txConsensus.reason : 'tx_pbft_failed';
          const pending = node._pendingWithdrawals.get(key);
          if (pending) {
            pending.status = 'failed';
            pending.error = reason;
          }
          return;
        }
      } catch (err) {
        const pending = node._pendingWithdrawals.get(key);
        if (pending) {
          pending.status = 'failed';
          pending.error = (err && err.message) || 'tx_pbft_error';
        }
        return;
      }
    } catch (e) {
      console.log('[Withdrawal] Failed to create transfer:', e.message || String(e));
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = e.message;
      }
      return;
    }

    if (!txData || !txData.txDataHex) {
      console.log('[Withdrawal] No transaction data returned from transfer');
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'failed';
        pending.error = 'No transaction data returned from transfer';
      }
      return;
    }

    const pendingSigner = node._pendingWithdrawals.get(key);
    const canonicalSignerSet = getCanonicalSignerSet(node);
    const signerSet =
      pendingSigner && Array.isArray(pendingSigner.signerSet) && pendingSigner.signerSet.length > 0
        ? pendingSigner.signerSet.map((a) => String(a || '').toLowerCase())
        : canonicalSignerSet;
    if (!signerSet || signerSet.length < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log('[Withdrawal] Not enough signers available for sequential signing');
      if (pendingSigner) pendingSigner.status = 'signing_failed';
      return;
    }

    const stepTimeoutMs = WITHDRAWAL_SIGN_STEP_TIMEOUT_MS;
    let currentTxDataHex = txData.txDataHex;
    let signedSteps = 0;

    try {
      const localInfo = await node.monero.exportMultisigInfo();
      const msPayload = JSON.stringify({
        type: 'multisig-info',
        clusterId: node._activeClusterId,
        info: localInfo,
        sender: node.wallet && node.wallet.address ? node.wallet.address : null,
      });
      await node.p2p.broadcastRoundData(
        node._activeClusterId,
        node._sessionId || 'bridge',
        MULTISIG_SYNC_ROUND,
        msPayload,
      );
      console.log('[Withdrawal] Broadcasted coordinator multisig info before signing loop');
    } catch (e) {
      console.log('[Withdrawal] Failed to broadcast coordinator multisig info:', e.message || String(e));
    }

    if (pendingSigner) {
      pendingSigner.currentStepIndex = 0;
      pendingSigner.currentTxDataHex = currentTxDataHex;
    }

    for (let i = 0; i < signerSet.length && signedSteps < REQUIRED_WITHDRAWAL_SIGNATURES; i++) {
      const stepSigner = signerSet[i];
      const requestPayload = JSON.stringify({
        type: 'withdrawal-sign-step-request',
        withdrawalTxHash: withdrawal.txHash,
        stepIndex: i,
        signer: stepSigner,
        txDataHex: currentTxDataHex,
      });
      try {
        await node.p2p.broadcastRoundData(
          node._activeClusterId,
          node._sessionId || 'bridge',
          9900,
          requestPayload,
        );
      } catch (_ignored) {}
      const response = await waitForSignStepResponse(
        node,
        withdrawal.txHash,
        i,
        stepSigner,
        stepTimeoutMs,
      );
      if (!response || (!response.txDataHex && !response.stale)) {
        console.log('[Withdrawal] Sign step timeout or invalid response, skipping signer');
        continue;
      }
      if (response.stale) {
        console.log('[Withdrawal] Sign step reported stale multisig state, aborting transaction for rebuild');
        const p = node._pendingWithdrawals.get(key);
        if (p) {
          p.status = 'sign_failed';
          p.error = 'stale_multisig_tx';
        }
        return;
      }
      const newTxDataHex = response.txDataHex;
      const stepPayload = JSON.stringify({
        withdrawalTxHash: withdrawal.txHash,
        stepIndex: i,
        signer: stepSigner,
        txDataHexHash: ethers.keccak256(ethers.toUtf8Bytes(newTxDataHex)),
      });
      const stepHash = ethers.keccak256(ethers.toUtf8Bytes(stepPayload));
      const stepTopic = `withdrawal-sign-step-${i}-${shortEth}`;
      let stepConsensus = null;
      try {
        stepConsensus = await node.p2p.runConsensus(
          node._activeClusterId,
          node._sessionId || 'bridge',
          stepTopic,
          stepHash,
          node._clusterMembers,
          stepTimeoutMs,
        );
      } catch (err) {
        console.log('[Withdrawal] PBFT error for sign step:', err.message || String(err));
        const p = node._pendingWithdrawals.get(key);
        if (p) p.status = 'sign_failed';
        return;
      }
      if (!stepConsensus || !stepConsensus.success) {
        console.log('[Withdrawal] PBFT consensus failed for sign step');
        const p = node._pendingWithdrawals.get(key);
        if (p) p.status = 'sign_failed';
        return;
      }
      currentTxDataHex = newTxDataHex;
      signedSteps += 1;
      const p2 = node._pendingWithdrawals.get(key);
      if (p2) {
        p2.currentStepIndex = i;
        p2.currentTxDataHex = currentTxDataHex;
      }
    }

    if (signedSteps < REQUIRED_WITHDRAWAL_SIGNATURES) {
      console.log('[Withdrawal] Not enough sequential sign steps completed');
      const p = node._pendingWithdrawals.get(key);
      if (p) p.status = 'signing_failed';
      return;
    }

    console.log('[Withdrawal] Submitting sequentially signed transaction...');
    try {
      const finalTxHex = currentTxDataHex;
      const submitResult = await node.monero.call('submit_multisig', { tx_data_hex: finalTxHex }, 180000);
      console.log('[Withdrawal] Transaction submitted successfully');
      console.log(`  TX Hash(es): ${submitResult.tx_hash_list ? submitResult.tx_hash_list.join(', ') : 'unknown'}`);
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'completed';
        pending.xmrTxHashes = submitResult.tx_hash_list;
        pending.txDataHexSigned = finalTxHex;
      }
      const signedPayload = JSON.stringify({ txDataHex: finalTxHex, txHashList: submitResult.tx_hash_list || [] });
      const signedHash = ethers.keccak256(ethers.toUtf8Bytes(signedPayload));
      const txPbftTimeoutMs = Number(process.env.WITHDRAWAL_TX_PBFT_TIMEOUT_MS || process.env.WITHDRAWAL_SIGN_TIMEOUT_MS || 300000);
      let signedConsensus = null;
      try {
        signedConsensus = await node.p2p.runConsensus(
          node._activeClusterId,
          node._sessionId || 'bridge',
          `withdrawal-signed-${shortEth}`,
          signedHash,
          node._clusterMembers,
          txPbftTimeoutMs,
        );
      } catch (err) {
        console.log('[Withdrawal] PBFT error for signed tx:', err.message || String(err));
      }
      const pending2 = node._pendingWithdrawals.get(key);
      if (pending2) {
        pending2.pbftSignedHash = signedHash;
        pending2.signedPbftSuccess = !!(signedConsensus && signedConsensus.success);
      }
      await broadcastUnsignedInfoIfNeeded(node, key);
      const finalPending = node._pendingWithdrawals.get(key);
      if (finalPending && finalPending.status === 'completed') {
        clearLocalSignStepCacheForWithdrawal(node, key);
        markWithdrawalProcessedInMemory(node, key);
        node._pendingWithdrawals.delete(key);
      }
    } catch (e) {
      console.log('[Withdrawal] Failed to submit transaction:', e.message || String(e));
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'submit_failed';
        pending.error = e.message;
      }
    }
  } catch (e) {
    console.log('[Withdrawal] Withdrawal execution error:', e.message || String(e));
    if (node._pendingWithdrawals) {
      const pending = node._pendingWithdrawals.get(key);
      if (pending) {
        pending.status = 'error';
        pending.error = e.message;
      }
    }
  } finally {
    node._withdrawalMultisigLock = null;
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
      signer: node.wallet && node.wallet.address ? node.wallet.address : null,
    }));
    console.log(`[Withdrawal] Broadcasted signature for ${data.withdrawalTxHash.slice(0, 18)}`);
  } catch (e) {
    const msg = e.message || String(e);
    if (msg.includes('stale data') || msg.includes('export fresh multisig data')) {
      console.log('[Withdrawal] Failed to sign withdrawal tx due to stale multisig data');
    } else {
      console.log('[Withdrawal] Failed to sign withdrawal tx:', msg);
    }
  }
}

export function getWithdrawalStatus(node, ethTxHash) {
  if (!node._pendingWithdrawals || !ethTxHash) return null;
  const key = String(ethTxHash).toLowerCase();
  return node._pendingWithdrawals.get(key) || null;
}

export function getAllPendingWithdrawals(node) {
  if (!node._pendingWithdrawals) return [];
  const result = [];
  for (const [txHash, data] of node._pendingWithdrawals) {
    result.push({ ethTxHash: txHash, ...data });
  }
  return result;
}
