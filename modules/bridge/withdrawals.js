import { ethers } from 'ethers';
import { isValidMoneroAddress } from './bridge-service.js';

function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}

const WITHDRAWAL_CASCADE_INTERVAL_MS = Number(process.env.WITHDRAWAL_CASCADE_INTERVAL_MS || 60000);

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
    node.bridge.on(filter, async (...args) => { const event = args[args.length - 1]; await handleBurnEvent(node, event); });
    console.log('[Withdrawal] Now listening for TokensBurned events');
    setupWithdrawalClaimListener(node);
  } catch (e) {
    console.log('[Withdrawal] Failed to setup event listener:', e.message || String(e));
    node._withdrawalMonitorRunning = false;
  }
}

export function stopWithdrawalMonitor(node) {
  node._withdrawalMonitorRunning = false;
  if (node.bridge) {
    try { node.bridge.removeAllListeners('TokensBurned'); }
    catch (_ignored) {}
  }
  console.log('[Withdrawal] Withdrawal monitor stopped');
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

async function handleBurnEvent(node, event) {
  try {
    if (!event || !event.transactionHash || !event.args) {
      console.log('[Withdrawal] Burn event missing expected fields, skipping');
      return;
    }
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
    console.log(`  Fee: ${ethers.formatEther(fee)} zXMR`);
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
      console.log('[Withdrawal] Another node claimed during wait, aborting');
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
