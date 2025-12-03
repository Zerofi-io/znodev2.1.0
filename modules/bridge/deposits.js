import crypto from 'crypto';
import { ethers } from 'ethers';
import { loadBridgeState, saveBridgeState, startBridgeStateSaver, startCleanupTimer } from './bridge-state.js';

const DEPOSIT_REQUEST_ROUND = 9700;
const GLOBAL_BRIDGE_CLUSTER_ID = 'GLOBAL_BRIDGE';
const GLOBAL_BRIDGE_SESSION = 'bridge-global';
const MINT_SIGNATURE_ROUND = 9800;
const MINT_SIGNATURE_COLLECT_ROUND = 9801;
const REQUIRED_MINT_SIGNATURES = 7;
const MINT_SIGNATURE_TIMEOUT_MS = Number(process.env.MINT_SIGNATURE_TIMEOUT_MS || 120000);
const MAX_STAKE_QUERY_FAILURES = 3;
const DEPOSIT_RECIPIENT_GRACE_MS = Number(process.env.DEPOSIT_RECIPIENT_GRACE_MS || 300000);

function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}

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

export async function syncDepositRequests(node) {
  if (!node.p2p || !node.staking) return;
  const testMode = process.env.TEST_MODE === '1';
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
        let normalizedEthAddress;
        try {
          normalizedEthAddress = ethers.getAddress(data.ethAddress.toLowerCase());
        } catch (e) {
          continue;
        }
        data.ethAddress = normalizedEthAddress;
        if (!verifyDepositRequestSignature(data.paymentId, data.ethAddress, data.clusterId, data.requestKey, data.signature, data.signer)) continue;
        const signerStaked = await isNodeStaked(node, data.signer);
        if (!testMode && !signerStaked) continue;
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
    // Skip if already being processed (consensus in progress)
    if (!node._inProgressDeposits) node._inProgressDeposits = new Set();
    if (node._inProgressDeposits.has(txid)) continue;
    console.log(`[Bridge] New deposit detected: ${txid}`);
    console.log(`  Amount: ${deposit.amount / 1e12} XMR`);
    console.log(`  Confirmations: ${deposit.confirmations}`);
    console.log(`  Payment ID: ${deposit.paymentId || 'none'}`);
    const recipient = parseRecipientFromPaymentId(node, deposit.paymentId);
    if (!recipient) {
      const now = Date.now();
      if (!node._unresolvedDeposits) node._unresolvedDeposits = new Map();
      const first = node._unresolvedDeposits.get(txid) || now;
      node._unresolvedDeposits.set(txid, first);
      if (now - first >= DEPOSIT_RECIPIENT_GRACE_MS) {
        console.log(`[Bridge] Marking deposit ${txid} as orphan: no valid recipient in payment ID`);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        node._processedDeposits.add(txid);
        node._unresolvedDeposits.delete(txid);
        saveBridgeState(node);
      } else {
        console.log(`[Bridge] Skipping deposit ${txid}: no valid recipient in payment ID (retrying for ${Math.floor((DEPOSIT_RECIPIENT_GRACE_MS - (now - first)) / 1000)}s more)`);
      }
      continue;
    }
    // Mark as in-progress to prevent concurrent processing
    node._inProgressDeposits.add(txid);
    let consensusSuccess = false;
    try {
      const consensusResult = await runDepositConsensus(node, deposit, recipient);
      if (consensusResult.success) {
        console.log(`[Bridge] Consensus reached for deposit ${txid}`);
        await generateAndShareMintSignature(node, deposit, recipient);
        await collectMintSignatures(node, deposit.txid);
        consensusSuccess = true;
      } else {
        console.log(`[Bridge] Consensus failed for deposit ${txid}: ${consensusResult.reason}`);
      }
    } catch (e) {
      console.log(`[Bridge] Consensus error for deposit ${txid}:`, e.message || String(e));
    }
    node._inProgressDeposits.delete(txid);
    if (consensusSuccess) {
      // Success - mark as processed
      if (!node._processedDeposits) node._processedDeposits = new Set();
      node._processedDeposits.add(txid);
      if (node._unresolvedDeposits) node._unresolvedDeposits.delete(txid);
      if (node._failedConsensusDeposits) node._failedConsensusDeposits.delete(txid);
      saveBridgeState(node);
    } else {
      // Track failed consensus attempts with grace period
      const now = Date.now();
      if (!node._failedConsensusDeposits) node._failedConsensusDeposits = new Map();
      const firstAttempt = node._failedConsensusDeposits.get(txid) || now;
      node._failedConsensusDeposits.set(txid, firstAttempt);
      if (now - firstAttempt >= DEPOSIT_RECIPIENT_GRACE_MS) {
        console.log(`[Bridge] Marking deposit ${txid} as orphan after repeated consensus failures`);
        if (!node._processedDeposits) node._processedDeposits = new Set();
        node._processedDeposits.add(txid);
        node._failedConsensusDeposits.delete(txid);
        saveBridgeState(node);
      } else {
        console.log(`[Bridge] Will retry consensus for deposit ${txid} (retrying for ${Math.floor((DEPOSIT_RECIPIENT_GRACE_MS - (now - firstAttempt)) / 1000)}s more)`);
      }
    }
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
