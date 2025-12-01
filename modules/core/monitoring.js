import { ethers } from 'ethers';
import { currentSlashEpoch, selectSlashLeader } from '../cluster/slash-utils.js';

export function startHeartbeatLoop(node, DRY_RUN) {
  const intervalSec = Number(process.env.HEARTBEAT_INTERVAL || 30);
  if (node._heartbeatTimer) return;

  node._heartbeatFailures = 0;
  node._heartbeatBackoffUntil = 0;

  const tick = async () => {
    try {
      const now = Date.now();

      if (!node.p2p || !node.p2p.node) {
        if (!node._hbP2PNotReadyLogged) {
          console.log('[Heartbeat] P2P not connected; skipping heartbeat until network is ready.');
          node._hbP2PNotReadyLogged = true;
        }
        return;
      }

      if (now < node._heartbeatBackoffUntil) {
        const remaining = Math.ceil((node._heartbeatBackoffUntil - now) / 1000);
        console.log(
          `[Heartbeat] Backing off for ${remaining}s after ${node._heartbeatFailures} consecutive failures`,
        );
        return;
      }

      if (DRY_RUN) {
        if (!node._hbLogOnce) {
          console.log(
            '[OK] P2P Heartbeat enabled (interval',
            intervalSec,
            's) [DRY_RUN - not sending]',
          );
          node._hbLogOnce = true;
        }
      } else {
        await node.p2p.broadcastHeartbeat();
        node._lastHeartbeatAt = Date.now();
        node._heartbeatFailures = 0;
        node._heartbeatBackoffUntil = 0;
        node._hbP2PNotReadyLogged = false;
        if (!node._hbLogOnce) {
          console.log('[OK] P2P Heartbeat enabled (interval', intervalSec, 's)');
          node._hbLogOnce = true;
        }
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (/Not connected to p2p-daemon/i.test(msg)) {
        if (!node._hbP2PNotReadyLogged) {
          console.warn(
            '[Heartbeat] P2P client not connected to daemon; skipping heartbeats until connected.',
          );
          node._hbP2PNotReadyLogged = true;
        }
        return;
      }
      console.warn('P2P heartbeat() error:', msg);
      node._heartbeatFailures++;

      if (node._heartbeatFailures >= 3) {
        const exponent = Math.min(node._heartbeatFailures - 3, 6);
        const backoffMs = Math.min(60000 * Math.pow(2, exponent), 3600000);
        node._heartbeatBackoffUntil = Date.now() + backoffMs;
        console.warn(
          `[Heartbeat] ${node._heartbeatFailures} consecutive failures, backing off for ${
            backoffMs / 1000
          }s`,
        );
      }
    }
  };

  node._heartbeatTimer = setInterval(tick, intervalSec * 1000);
  setTimeout(tick, 10_000);
}

export function startSlashingLoop(node, DRY_RUN) {
  if (node._slashingTimer) return;
  if (!node.staking || !node.provider || !node.p2p) return;

  const epochRaw = process.env.SLASH_EPOCH_SECONDS;
  const epochParsed = epochRaw != null ? Number(epochRaw) : NaN;
  const epochSec = Number.isFinite(epochParsed) && epochParsed > 0 ? epochParsed : 600;

  const offlineRaw = process.env.SLASH_OFFLINE_THRESHOLD_HOURS;
  const offlineParsed = offlineRaw != null ? Number(offlineRaw) : NaN;
  const offlineHours = Number.isFinite(offlineParsed) && offlineParsed > 0 ? offlineParsed : 48;

  const loopRaw = process.env.SLASH_LOOP_INTERVAL_MS;
  const loopParsed = loopRaw != null ? Number(loopRaw) : NaN;
  const loopMs = Number.isFinite(loopParsed) && loopParsed > 0 ? loopParsed : 24 * 60 * 60 * 1000;

  const stakingAddr = (node.staking.target || node.staking.address || '').toString();
  const salt = stakingAddr && stakingAddr !== ethers.ZeroAddress ? stakingAddr : ethers.ZeroHash;

  const selfAddr = node.wallet.address.toLowerCase();

  const tick = async () => {
    try {
      if (!node.p2p || !node.p2p.node) {
        if (!node._slashingP2PNotReadyLogged) {
          console.log('[SLASH] P2P not connected; skipping slashing loop until network is ready.');
          node._slashingP2PNotReadyLogged = true;
        }
        return;
      }
      node._slashingP2PNotReadyLogged = false;
      if (!node.staking || !node.provider || !node.p2p) return;

      const latest = await node.provider.getBlock('latest');
      if (!latest || latest.timestamp == null) return;

      const epoch = currentSlashEpoch(latest.timestamp, epochSec);
      const activeNodes = await node.staking.getActiveNodes();
      if (!Array.isArray(activeNodes) || activeNodes.length === 0) return;

      const nowSec = Math.floor(Date.now() / 1000);

      for (const nodeAddr of activeNodes) {
        if (!nodeAddr) continue;
        const lower = nodeAddr.toLowerCase();
        if (lower === selfAddr) continue;

        const hb = await node.p2p.getLastHeartbeat(nodeAddr);
        if (!hb || hb.timestamp == null) continue;

        const tsSec = Number(hb.timestamp);
        if (!Number.isFinite(tsSec) || tsSec <= 0) continue;

        const ageSec = Math.max(0, nowSec - tsSec);
        const hoursOffline = ageSec / 3600;
        if (hoursOffline < offlineHours) continue;

        const leader = selectSlashLeader(activeNodes, nodeAddr, epoch, salt);
        if (!leader || leader.toLowerCase() !== selfAddr) continue;

        const minGasWeiRaw = process.env.SLASH_MIN_GAS_WEI;
        if (minGasWeiRaw) {
          try {
            const balance = await node.provider.getBalance(node.wallet.address);
            const minGasWei = BigInt(minGasWeiRaw);
            if (balance < minGasWei) {
              continue;
            }
          } catch (_ignored) {}
        }

        const tsArg = BigInt(tsSec);

        if (DRY_RUN) {
          console.log(
            `[SLASH-DRY] Would call slashForDowntimeWithProof for ${nodeAddr} (offline ~${hoursOffline.toFixed(
              2,
            )}h)`,
          );
          continue;
        }

        try {
          const gasLimitRaw = process.env.SLASH_GAS_LIMIT;
          const gasLimitParsed = gasLimitRaw != null ? Number(gasLimitRaw) : NaN;
          const gasOverride =
            Number.isFinite(gasLimitParsed) && gasLimitParsed > 0
              ? { gasLimit: BigInt(Math.floor(gasLimitParsed)) }
              : {};

          const tx = await node.staking.slashForDowntimeWithProof(
            nodeAddr,
            tsArg,
            hb.signature,
            gasOverride,
          );
          console.log(
            `[SLASH] Submitted slashForDowntimeWithProof for ${nodeAddr} (tx: ${tx.hash})`,
          );
          await tx.wait();
          console.log(`[SLASH] SlashForDowntimeWithProof confirmed for ${nodeAddr}`);
        } catch (e) {
          const msg = e && e.message ? e.message : String(e);
          console.warn(`[SLASH] Failed to slash ${nodeAddr}:`, msg);
        }
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.warn('[SLASH] Slashing loop error:', msg);
    }
  };

  node._slashingTimer = setInterval(tick, loopMs);
  setTimeout(tick, 30_000);
}

export async function monitorNetwork(node, DRY_RUN) {
  console.log('[INFO] Monitoring network...');
  console.log('[INFO] Monero multisig is WORKING!');
  console.log('Wallet has password and multisig is enabled.\n');

  node._clusterFinalized = node._clusterFinalized || false;

  const selfAddr = node.wallet.address.toLowerCase();

  const monitorIntervalRaw = process.env.MONITOR_LOOP_INTERVAL_MS;
  const monitorIntervalParsed = monitorIntervalRaw != null ? Number(monitorIntervalRaw) : NaN;
  const monitorIntervalMs =
    Number.isFinite(monitorIntervalParsed) && monitorIntervalParsed > 0
      ? monitorIntervalParsed
      : 60 * 1000;

  const rawHealthInterval = process.env.HEALTH_LOG_INTERVAL_MS;
  const parsedHealthInterval = rawHealthInterval != null ? Number(rawHealthInterval) : NaN;
  const healthIntervalMs =
    Number.isFinite(parsedHealthInterval) && parsedHealthInterval > 0
      ? parsedHealthInterval
      : 5 * 60 * 1000;

  const clusterSizeRaw = process.env.CLUSTER_SIZE;
  const clusterSizeParsed = clusterSizeRaw != null ? Number(clusterSizeRaw) : NaN;
  const clusterSize =
    Number.isFinite(clusterSizeParsed) && clusterSizeParsed > 0 ? clusterSizeParsed : 11;

  const clusterThresholdRaw = process.env.CLUSTER_THRESHOLD;
  const clusterThresholdParsed = clusterThresholdRaw != null ? Number(clusterThresholdRaw) : NaN;
  const clusterThreshold =
    Number.isFinite(clusterThresholdParsed) && clusterThresholdParsed > 0
      ? clusterThresholdParsed
      : 7;

  const computeCandidateCluster = async () => {
    try {
      const maxScanRaw = process.env.MAX_REGISTERED_SCAN;
      const maxScanParsed = maxScanRaw != null ? Number(maxScanRaw) : NaN;
      const maxScan = Number.isFinite(maxScanParsed) && maxScanParsed > 0 ? maxScanParsed : 256;

      const maxPagesRaw = process.env.MAX_REGISTERED_PAGES;
      const maxPagesParsed = maxPagesRaw != null ? Number(maxPagesRaw) : NaN;
      const maxPages =
        Number.isFinite(maxPagesParsed) && maxPagesParsed > 0 ? maxPagesParsed : 10;

      let candidates = [];
      let offset = 0;
      let pagesScanned = 0;

      while (pagesScanned < maxPages) {
        const page = await node.registry.getRegisteredNodes(offset, maxScan);
        if (!page || page.length === 0) break;

        for (const addr of page) {
          if (!addr || addr === ethers.ZeroAddress) continue;
          try {
            const can = await node.registry.canParticipate(addr);
            if (!can) continue;
            candidates.push(addr);
          } catch (_ignored) {}
        }

        if (page.length < maxScan) break;
        offset += maxScan;
        pagesScanned++;
      }

      if (candidates.length < clusterSize) {
        return null;
      }

      const useP2PFilter = process.env.ENABLE_P2P_HEARTBEAT_CLUSTER_FILTER === '1';
      if (useP2PFilter && node.p2p && typeof node.p2p.getHeartbeats === 'function') {
        try {
          const ttlRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
          const ttlParsed = ttlRaw != null ? Number(ttlRaw) : NaN;
          const ttlMs = Number.isFinite(ttlParsed) && ttlParsed > 0 ? ttlParsed : 1800000;
          const hbMap = await node.p2p.getHeartbeats(ttlMs);
          const p2pLive = new Set([node.wallet.address.toLowerCase()]);
          for (const [addr, rec] of hbMap.entries()) {
            if (rec && rec.timestamp != null) {
              p2pLive.add(addr.toLowerCase());
            }
          }

          const p2pFiltered = candidates.filter((a) => p2pLive.has(a.toLowerCase()));
          if (p2pFiltered.length >= clusterSize) {
            candidates = p2pFiltered;
          }
        } catch (_ignored) {}
      }

      let blockNumber = 0;
      let epochSeed = ethers.ZeroHash;
      try {
        blockNumber = await node.provider.getBlockNumber();
        blockNumber = Math.max(0, blockNumber - 5); // Safe block for consensus
        const rawSpan = process.env.SELECTION_EPOCH_BLOCKS;
        const parsedSpan = rawSpan != null ? Number(rawSpan) : NaN;
        const epochSpan = Number.isFinite(parsedSpan) && parsedSpan > 0 ? parsedSpan : 100;
        const epoch = (Number(blockNumber) / epochSpan) | 0;
        epochSeed = ethers.keccak256(ethers.solidityPacked(['uint256'], [epoch]));
      } catch (_ignored) {}

      const uniqueCandidates = [...new Set(candidates.map((a) => a.toLowerCase()))];
      if (uniqueCandidates.length < clusterSize) {
        return null;
      }

      const scored = uniqueCandidates.map((lower) => {
        const score = ethers.keccak256(
          ethers.solidityPacked(['bytes32', 'address'], [epochSeed, lower]),
        );
        return { lower, score };
      });

      scored.sort((a, b) => a.score.localeCompare(b.score));
      const chosen = scored.slice(0, clusterSize);
      const membersLower = chosen.map((x) => x.lower);

      if (membersLower.length !== clusterSize) {
        console.log(
          `[WARN]  Cluster member count mismatch: expected ${clusterSize}, got ${membersLower.length}`,
        );
        return null;
      }

      const members = membersLower.map((addr) => ethers.getAddress(addr));
      const sortedMembersLower = [...membersLower].sort();
      const addressTypes = Array(clusterSize).fill('address');
      const clusterId = ethers.keccak256(ethers.solidityPacked(addressTypes, sortedMembersLower));

      if (
        node._clusterBlacklist &&
        node._clusterBlacklist[clusterId] &&
        Date.now() < node._clusterBlacklist[clusterId]
      ) {
        return null;
      }

      try {
        const info = await node.registry.clusters(clusterId);
        const finalized = info && info[3];
        if (finalized) {
          return null;
        }
      } catch (_ignored) {}

      return { members, clusterId, blockNumber, epochSeed, uniqueCandidates };
    } catch (e) {
      console.log('Cluster candidate compute error:', e.message || String(e));
      return null;
    }
  };



  // Pre-selection consensus to agree on cluster members before formation
  const runPreSelectionConsensus = async (localCandidates, blockNumber, epochSeed) => {
    if (!node.p2p || typeof node.p2p.broadcastPreSelection !== 'function') {
      return { success: false, candidates: localCandidates };
    }
    const sortedLocal = [...localCandidates].sort();
    const selfAddrLower = selfAddr.toLowerCase();
    const preSelCoordinator = sortedLocal[0];
    const isPreSelCoord = preSelCoordinator === selfAddrLower;
    const preSelTimeoutRaw = process.env.PRESELECTION_TIMEOUT_MS;
    const preSelTimeoutParsed = preSelTimeoutRaw != null ? Number(preSelTimeoutRaw) : NaN;
    const preSelTimeoutMs = Number.isFinite(preSelTimeoutParsed) && preSelTimeoutParsed > 0
      ? preSelTimeoutParsed : 30000;
    try {
      if (isPreSelCoord) {
        console.log('[PreSelection] Broadcasting proposal as coordinator...');
        const result = await node.p2p.broadcastPreSelection(blockNumber, epochSeed, localCandidates);
        if (!result || !result.proposalId) {
          console.log('[PreSelection] Failed to broadcast proposal');
          return { success: false, candidates: localCandidates };
        }
        const consensus = await node.p2p.waitPreSelection(result.proposalId, localCandidates, preSelTimeoutMs);
        if (consensus && consensus.success) {
          console.log(`[PreSelection] Consensus reached (${consensus.approved}/${consensus.total})`);
          return { success: true, candidates: localCandidates, proposalId: result.proposalId };
        }
        console.log(`[PreSelection] Consensus failed - missing: ${(consensus?.missing || []).join(', ')}`);
        return { success: false, candidates: localCandidates };
      } else {
        console.log(`[PreSelection] Waiting for proposal from ${preSelCoordinator.substring(0, 10)}...`);
        const waitStart = Date.now();
        let proposal = null;
        while (Date.now() - waitStart < preSelTimeoutMs) {
          proposal = await node.p2p.getPreSelectionProposal();
          if (proposal && proposal.found) break;
          await new Promise((r) => setTimeout(r, 500));
        }
        if (!proposal || !proposal.found) {
          console.log('[PreSelection] Timeout waiting for proposal');
          return { success: false, candidates: localCandidates };
        }
        const receivedCandidates = (proposal.candidates || []).map((a) => a.toLowerCase()).sort();
        const localSet = new Set(sortedLocal);
        const matchCount = receivedCandidates.filter((c) => localSet.has(c)).length;
        const matchRatio = matchCount / Math.max(receivedCandidates.length, sortedLocal.length);
        const approved = matchRatio >= 0.8;
        console.log(`[PreSelection] Match ratio: ${(matchRatio * 100).toFixed(1)}% (${approved ? 'approving' : 'rejecting'})`);
        await node.p2p.votePreSelection(proposal.proposalId, approved, localCandidates);
        if (!approved) return { success: false, candidates: localCandidates };
        const consensus = await node.p2p.waitPreSelection(proposal.proposalId, receivedCandidates, preSelTimeoutMs);
        if (consensus && consensus.success) {
          console.log('[PreSelection] Consensus reached, using coordinator candidates');
          return { success: true, candidates: proposal.candidates, proposalId: proposal.proposalId,
            blockNumber: proposal.blockNumber, epochSeed: proposal.epochSeed };
        }
        return { success: false, candidates: localCandidates };
      }
    } catch (e) {
      console.log('[PreSelection] Error:', e.message || String(e));
      return { success: false, candidates: localCandidates };
    }
  };

  const loop = async () => {
    if (node._monitorLoopRunning) {
      return;
    }
    if (!node._activeClusterId && node.clusterState && typeof node.clusterState.state === 'function') {
      const state = node.clusterState.state;
      const cooldownUntil = state.cooldownUntil || null;
      if (cooldownUntil && Date.now() < cooldownUntil) {
        const remainingSec = Math.ceil((cooldownUntil - Date.now()) / 1000);
        console.log(`[Cluster] Cooldown active after failed attempt; next formation in ${remainingSec}s`);
        return;
      }
    }
    node._monitorLoopRunning = true;

    try {
      try {
        await node._checkSelfStakeHealth();
      } catch (e) {
        console.log(
          '[Slashed] Error during periodic self stake health check:',
          e.message || String(e),
        );
      }
      if (node._selfSlashed) {
        return;
      }
      if (node._clusterFinalized) {
        return;
      }

      if (node._orchestrationMutex.isLocked()) {
        return;
      }

      try {
        if (typeof node._maybeApplyMoneroHealthAdminCommand === 'function') {
          await node._maybeApplyMoneroHealthAdminCommand();
        }
      } catch (e) {
        console.log('[MoneroHealth] Admin override processing error:', e.message || String(e));
      }

      if (
        healthIntervalMs > 0 &&
        (!node._lastHealthLogTs || Date.now() - node._lastHealthLogTs > healthIntervalMs)
      ) {
        try {
          node.logClusterHealth();
        } catch (_ignored) {}
        node._lastHealthLogTs = Date.now();
      }

      if (
        node._activeClusterId &&
        node._clusterMembers &&
        node._clusterMembers.length === clusterSize
      ) {
        try {
          const info = await node.registry.clusters(node._activeClusterId);
          const finalized = info && info[2];
          if (finalized) {
            const moneroAddress = info && info[0];
            let membersOnChain = null;
            try {
              membersOnChain = await node.registry.getClusterMembers(node._activeClusterId);
            } catch (e) {}
            if (typeof node._onClusterFinalized === 'function') {
              node._onClusterFinalized(node._activeClusterId, membersOnChain, moneroAddress);
            }
          } else {
            if (!node._clusterFinalized && node.p2p && node._sessionId) {
              try {
                const r9999 = await node.p2p.getRoundData(node._activeClusterId, node._sessionId, 9999);
                for (const payload of Object.values(r9999 || {})) {
                  try {
                    const data = JSON.parse(payload);
                    if (typeof node._handleCoordinatorHeartbeat === 'function') {
                      node._handleCoordinatorHeartbeat(data);
                    }
                    if (data.type === 'cluster-finalized' && data.clusterId) {
                      const clusterInfo = await node.registry.clusters(data.clusterId);
                      const onChainFinalized = clusterInfo && clusterInfo[2];
                      if (!onChainFinalized) {
                        continue;
                      }
                      let membersOnChain = null;
                      try {
                        membersOnChain = await node.registry.getClusterMembers(data.clusterId);
                      } catch (e) {}
                      const moneroAddress = clusterInfo && clusterInfo[0];
                      if (typeof node._onClusterFinalized === 'function') {
                        console.log('[Cluster] Finalization confirmed on-chain via coordinator broadcast');
                        node._onClusterFinalized(data.clusterId, membersOnChain, moneroAddress);
                      }
                      break;
                    }
                  } catch (e) {}
                }
              } catch (e) {}
            }
            const failoverRaw = process.env.FINALIZE_FAILOVER_MS;
            const failoverParsed = failoverRaw != null ? Number(failoverRaw) : NaN;
            const failoverMs =
              Number.isFinite(failoverParsed) && failoverParsed > 0
                ? failoverParsed
                : 15 * 60 * 1000;
            if (
              !node._clusterFailoverAttempted &&
              node._clusterFinalizationStartAt &&
              node._clusterFinalAddress &&
              failoverMs > 0
            ) {
              const elapsed = Date.now() - node._clusterFinalizationStartAt;
              if (elapsed > failoverMs) {
                const membersLowerLocal = node._clusterMembers.map((a) => a.toLowerCase());
                const sortedLower = [...membersLowerLocal].sort();
                const myIndex = sortedLower.indexOf(selfAddr);
                const failoverIndexRaw = process.env.FAILOVER_COORDINATOR_INDEX;
                const failoverIndexParsed =
                  failoverIndexRaw != null ? Number(failoverIndexRaw) : NaN;
                let failoverIndex =
                  Number.isFinite(failoverIndexParsed) && failoverIndexParsed > 0
                    ? failoverIndexParsed
                    : 1;

                if (failoverIndex >= clusterSize) {
                  console.warn(
                    `[WARN]  FAILOVER_COORDINATOR_INDEX (${failoverIndex}) >= cluster size (${clusterSize}), using index 1`,
                  );
                  failoverIndex = 1;
                }

                if (myIndex === failoverIndex) {
                  console.log(
                    `[WARN] Coordinator did not finalize in time; attempting fallback finalizeCluster as coordinator #${failoverIndex}`,
                  );
                  try {
                    const clusterInfo = await node.registry.clusters(node._activeClusterId);
                    const alreadyFinalized = clusterInfo && clusterInfo[2];
                    if (alreadyFinalized) {
                      console.log(
                        '  [INFO]  Cluster already finalized by another node; skipping failover',
                      );
                    } else {
                      console.log(
                        `  [INFO] finalizeCluster([${node._clusterMembers.length} members], ${node._clusterFinalAddress})`,
                      );
                      if (DRY_RUN) {
                        console.log(
                          '  [DRY_RUN] Would send fallback finalizeCluster transaction',
                        );
                      } else {
                        const tx = await node.registry.finalizeCluster(
                          node._clusterMembers,
                          node._clusterFinalAddress,
                        );
                        await tx.wait();
                        console.log('[OK] Cluster finalized on-chain (v3, fallback coordinator)');
                      }
                    }
                  } catch (e2) {
                    console.log(
                      '[ERROR] Fallback finalizeCluster() on-chain failed:',
                      e2.message || String(e2),
                    );
                  } finally {
                    node._clusterFailoverAttempted = true;
                  }
                }
              }
            }
          }
          node._clusterStatusErrorCount = 0;
        } catch (e) {
          console.log('Cluster status read error:', e.message || String(e));
          node._clusterStatusErrorCount = (node._clusterStatusErrorCount || 0) + 1;
          if (node._clusterStatusErrorCount > 5) {
            console.log('[WARN] Repeated cluster status errors; resetting active cluster state');
            node._activeClusterId = null;
            if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
              node.p2p.setActiveCluster(null);
            }
            node._clusterMembers = null;
            node._clusterStatusErrorCount = 0;
          }
        }

        try {
          await node.checkEmergencySweep();
        } catch (e) {
          console.log('[Sweep] Emergency sweep check error:', e.message || String(e));
        }

        return;
      }

      const candidate = await computeCandidateCluster();

      try {
        const maxScan = Number(process.env.MAX_REGISTERED_SCAN || 256);
        let offset = 0;
        const eligible = [];
        const eligibleSet = new Set();
        while (eligible.length < clusterSize) {
          const page = await node.registry.getRegisteredNodes(offset, maxScan);
          if (!Array.isArray(page) || page.length === 0) break;
          for (const addr of page) {
            if (!addr || addr === ethers.ZeroAddress) continue;
            const addrLower = addr.toLowerCase();
            if (eligibleSet.has(addrLower)) continue;
            try {
              const ok = await node.registry.canParticipate(addr);
              if (ok) {
                eligible.push(addr);
                eligibleSet.add(addrLower);
              }
            } catch (_ignored) {}
          }
          offset += maxScan;
        }

        if (eligible.length === 0) {
          const selfAddress = node.wallet.address;
          if (selfAddress) {
            eligible.push(selfAddress);
            eligibleSet.add(selfAddress.toLowerCase());
          }
        }

        let onchainOnlineCount = 0;
        const selfAddrLower = node.wallet.address.toLowerCase();
        const selfIsEligible = eligible.some((a) => a.toLowerCase() === selfAddrLower);
        if (node.p2p && typeof node.p2p.getHeartbeats === 'function' && eligible.length > 0) {
          try {
            const ttlRaw = process.env.HEARTBEAT_ONLINE_TTL_MS;
            const ttlParsed = ttlRaw != null ? Number(ttlRaw) : NaN;
            const ttlMs = Number.isFinite(ttlParsed) && ttlParsed > 0 ? ttlParsed : undefined;
            const hbMap = await node.p2p.getHeartbeats(ttlMs);
            const counted = new Set();
            for (const addr of eligible) {
              if (!addr) continue;
              const addrLower = addr.toLowerCase();
              const rec = hbMap.get(addrLower);
              if (rec && rec.timestamp != null) {
                onchainOnlineCount++;
                counted.add(addrLower);
              }
            }
            if (selfIsEligible && !counted.has(selfAddrLower)) {
              onchainOnlineCount++;
            }
          } catch {
            onchainOnlineCount = selfIsEligible ? 1 : 0;
          }
        } else if (selfIsEligible) {
          onchainOnlineCount = 1;
        }

        if (eligible.length > 0) {
          console.log(
            'Online Members in Queue (on-chain): ' + onchainOnlineCount + '/' + eligible.length,
          );
        } else {
          console.log('Online Members in Queue (on-chain): 0/0');
        }

        let p2pOnlineCount = 0;
        if (node.p2p && typeof node.p2p.getQueuePeers === 'function' && eligible.length > 0) {
          try {
            const selfAddr = node.wallet.address.toLowerCase();
            const recent = await node.p2p.getQueuePeers();
            const addrSet = new Set(recent.map((a) => a.toLowerCase()));
            addrSet.add(selfAddr);
            for (const addr of eligible) {
              if (addrSet.has(addr.toLowerCase())) {
                p2pOnlineCount++;
              }
            }
          } catch {
            const selfAddr = node.wallet.address.toLowerCase();
            if (eligible.some((a) => a.toLowerCase() === selfAddr)) {
              p2pOnlineCount = 1;
            }
          }
        } else if (eligible.length > 0) {
          const selfAddr = node.wallet.address.toLowerCase();
          if (eligible.some((a) => a.toLowerCase() === selfAddr)) {
            p2pOnlineCount = 1;
          }
        }

        if (eligible.length > 0) {
          console.log('P2P-Online Members in Queue: ' + p2pOnlineCount + '/' + eligible.length);
        } else {
          console.log('P2P-Online Members in Queue: 0/0');
        }

        const minP2PVisibility = Number(process.env.MIN_P2P_VISIBILITY || clusterSize);

        if (p2pOnlineCount < minP2PVisibility) {
          console.log(
            `  [WARN] P2P visibility too low (${p2pOnlineCount}/${minP2PVisibility}); waiting for more peers...`,
          );
          return;
        }
      } catch (e) {
        console.log('Queue status log error:', e.message || String(e));
      }

      if (!candidate) {
        return;
      }


      // Pre-selection consensus (enabled via ENABLE_PRESELECTION=1)
      const enablePreSel = process.env.ENABLE_PRESELECTION === '1';
      if (enablePreSel && candidate.uniqueCandidates && candidate.blockNumber) {
        console.log('[PreSelection] Running pre-selection consensus...');
        const preSelResult = await runPreSelectionConsensus(
          candidate.uniqueCandidates,
          candidate.blockNumber,
          candidate.epochSeed,
        );
        if (!preSelResult.success) {
          console.log('[PreSelection] Pre-selection consensus failed, aborting cluster formation');
          return;
        }
        // If coordinator's candidates differ, recompute members from consensus candidates
        if (preSelResult.candidates && preSelResult.epochSeed) {
          const consensusCandidates = preSelResult.candidates.map((a) => a.toLowerCase());
          const scored = consensusCandidates.map((lower) => {
            const score = ethers.keccak256(
              ethers.solidityPacked(['bytes32', 'address'], [preSelResult.epochSeed, lower]),
            );
            return { lower, score };
          });
          scored.sort((a, b) => a.score.localeCompare(b.score));
          const chosen = scored.slice(0, clusterSize);
          const newMembersLower = chosen.map((x) => x.lower);
          const newMembers = newMembersLower.map((addr) => ethers.getAddress(addr));
          const sortedNew = [...newMembersLower].sort();
          const addressTypes = Array(clusterSize).fill('address');
          const newClusterId = ethers.keccak256(ethers.solidityPacked(addressTypes, sortedNew));
          candidate = { members: newMembers, clusterId: newClusterId };
          console.log('[PreSelection] Using consensus-derived members, clusterId:', newClusterId.substring(0, 18));
        }
      }

      const { members, clusterId } = candidate;
      const membersLower = members.map((a) => a.toLowerCase());
      if (membersLower.length !== clusterSize) {
        console.log(
          `[WARN] Candidate cluster has ${membersLower.length} members, expected ${clusterSize}; skipping`,
        );
        return;
      }
      if (!membersLower.includes(selfAddr)) {
        console.log(`[WARN] Self (${selfAddr}) not in computed cluster members`);
        console.log(`[DEBUG] Members: ${membersLower.slice(0, 3).join(', ')}...`);
        return;
      }

      const sortedMembersLower = [...membersLower].sort();
      let coordinatorIndex = 0;
      try {
        const seed = ethers.keccak256(ethers.solidityPacked(['bytes32'], [clusterId]));
        const seedBig = BigInt(seed);
        coordinatorIndex = Number(seedBig % BigInt(sortedMembersLower.length));
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        console.log('[WARN] Coordinator selection failed, falling back to index 0:', msg);
      }
      const coordinator = sortedMembersLower[coordinatorIndex];
      const isCoordinator = selfAddr === coordinator;
      const myIndex = sortedMembersLower.indexOf(selfAddr);

      console.log('[INFO] New candidate cluster discovered...');
      console.log(`  ClusterId: ${clusterId}`);
      console.log(
        `  Members: ${members.length} (myIndex=${myIndex}, coordinator=${coordinator})`,
      );

      const p2pOk = await node.initClusterP2P(clusterId, members, isCoordinator);
      if (!p2pOk) {
        console.log('[WARN] P2P init failed for cluster; will retry later');
        return;
      }

      if (!node.p2p || !node.p2p.node) {
        console.log('[WARN] P2P not available for liveness check; skipping candidate cluster');
        return;
      }

      const LIVENESS_ROUND = 9999;
      const liveKeyInitial = `${clusterId}_${LIVENESS_ROUND}`;
      const livenessQuorumRaw = process.env.LIVENESS_QUORUM;
      const livenessQuorumParsed = livenessQuorumRaw != null ? Number(livenessQuorumRaw) : NaN;
      let livenessQuorum =
        Number.isFinite(livenessQuorumParsed) && livenessQuorumParsed > 0
          ? livenessQuorumParsed
          : clusterSize;

      if (livenessQuorum < clusterSize) {
        console.warn(
          `[WARN]  LIVENESS_QUORUM (${livenessQuorum}) < CLUSTER_SIZE (${clusterSize}). Enforcing full quorum to prevent guaranteed formation failures.`,
        );
        livenessQuorum = clusterSize;
      } else if (livenessQuorum > clusterSize) {
        console.warn(
          `[WARN]  LIVENESS_QUORUM (${livenessQuorum}) > CLUSTER_SIZE (${clusterSize}). Clamping to cluster size.`,
        );
        livenessQuorum = clusterSize;
      }

      console.log(
        `  [INFO] Proceeding to R1 signature barrier for liveness (${clusterSize} nodes expected)...`,
      );

      if (node._orchestrationMutex.isLocked()) {
        console.log('[WARN] Cluster orchestration already in progress, skipping');
        return;
      }

      await node._orchestrationMutex.acquire();
      try {
        node._activeClusterId = clusterId;
        if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
          node.p2p.setActiveCluster(clusterId);
        }
        node._clusterMembers = members;

        console.log('[INFO] Starting identity exchange (PBFT consensus will gate progress)...');

        const identityTimeoutMs = Number(process.env.PBFT_IDENTITY_TIMEOUT_MS || 300000);
        const identityPromise = node.p2p.waitForIdentities(clusterId, members, identityTimeoutMs);

        const canonicalIdentityMembers = [...members].map((a) => (a || '').toLowerCase()).sort();
        const identityData = JSON.stringify(canonicalIdentityMembers);

        console.log(`[INFO] PBFT Consensus: waiting for all ${clusterSize} nodes to be ready...`);
        const identityConsensusTimeout = Number(process.env.PBFT_IDENTITY_TIMEOUT_MS || 300000);
        let identityConsensus;
        try {
          identityConsensus = await node.p2p.runConsensus(
            clusterId,
            null,
            'identity',
            identityData,
            canonicalIdentityMembers,
            identityConsensusTimeout,
          );
        } catch (e) {
          console.log(`[ERROR] PBFT "identity" consensus error: ${e.message || String(e)}`);
          console.log('[INFO] Cleaning up and will retry...');
          await node.p2p.leaveCluster(clusterId);
          node._activeClusterId = null;
          node._clusterMembers = null;
          return;
        }
        if (!identityConsensus.success) {
          console.log(
            `[ERROR] PBFT "identity" consensus failed - missing: ${(identityConsensus.missing || []).join(', ')}`,
          );
          console.log('[INFO] Cleaning up and will retry...');
          await node.p2p.leaveCluster(clusterId);
          node._activeClusterId = null;
          node._clusterMembers = null;
          return;
        }
        console.log(`[OK] PBFT identity consensus reached - all ${clusterSize}/${clusterSize} nodes ready\n`);

        const identityResult = await identityPromise.catch(() => null);
        if (!identityResult?.complete) {
          const bindings = await node.p2p.getPeerBindings(clusterId);
          const bindingCount = Object.keys(bindings || {}).length;
          if (bindingCount < members.length) {
            console.log(
              `[WARN] Warning: Only ${bindingCount}/${members.length} peer bindings established`,
            );
          }
        }

        const maxMultisigAttempts = Number(process.env.CLUSTER_MULTISIG_MAX_ATTEMPTS || 3);
        let attempt = 1;
        let ok = false;
        while (attempt <= maxMultisigAttempts) {
          console.log(
            `[Retry] Cluster ${clusterId.substring(0, 10)} multisig - attempt ${attempt}/${maxMultisigAttempts}`,
          );

          const preflightOk = await node._preflightMoneroForClusterAttempt(clusterId, attempt);
          if (!preflightOk) {
            console.log(
              '[Retry] Monero preflight for cluster attempt failed; aborting multisig retries',
            );
            break;
          }

          ok = await node.startClusterMultisigV3(
            clusterId,
            members,
            isCoordinator,
            clusterThreshold,
          );
          await node._postAttemptMoneroCleanup(clusterId, attempt, ok);

          if (ok) {
            break;
          }
          console.log(
            `[Retry] Cluster ${clusterId.substring(0, 10)} multisig - attempt ${attempt} returned falsy result`,
          );
          if (attempt >= maxMultisigAttempts) {
            break;
          }
          const delayMs = Math.min(15000 * Math.pow(2, attempt - 1), 60000);
          console.log(`[Retry] Waiting ${delayMs / 1000}s before retry...`);
          await new Promise((r) => setTimeout(r, delayMs));

          try {
            if (node._activeClusterId) {
              await node._cleanupClusterAttempt(node._activeClusterId);
            }
          } catch (cleanupErr) {
            console.log(`[Retry] Cleanup error: ${cleanupErr.message || String(cleanupErr)}`);
          }

          const p2pOkRetry = await node.initClusterP2P(clusterId, members, isCoordinator);
          if (!p2pOkRetry) {
            console.log('[WARN] P2P re-init failed for cluster; aborting retries');
            break;
          }

          attempt += 1;
        }

        if (!ok) {
          console.log('[ERROR] Cluster multisig flow failed after all retry attempts');
          try {
            await node._cleanupClusterAttempt(clusterId);
          } catch (e) {
            console.log(
              '[WARN]  Cluster cleanup error after failed multisig flow:',
              e.message || String(e),
            );
          }
          if (isCoordinator && typeof node.dissolveStuckFinalizedCluster === 'function') {
            try {
              await node.dissolveStuckFinalizedCluster(clusterId);
            } catch (e) {
              const msg = e && e.message ? e.message : String(e);
              console.log(`[Dissolve] Error attempting stuck-cluster dissolution: ${msg}`);
            }
          }
          node._activeClusterId = null;
          if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
            node.p2p.setActiveCluster(null);
          }
          node._clusterMembers = null;
          try {
            if (node.p2p && node.p2p.roundData && node.p2p.roundData.has(liveKeyInitial)) {
              node.p2p.roundData.delete(liveKeyInitial);
            }
          } catch (_ignored) {}
          try {
            const cooldownMs = Number(process.env.CLUSTER_RETRY_COOLDOWN_MS || 300000);
            const now = Date.now();
            if (node.clusterState && typeof node.clusterState.setCooldownUntil === 'function') {
              node.clusterState.setCooldownUntil(now + cooldownMs);
              if (typeof node.clusterState.recordFailure === 'function') {
                node.clusterState.recordFailure('multisig_failed');
              }
            }
          } catch (_ignored) {}
        }
      } finally {
        node._orchestrationMutex.release();
      }
    } catch (e) {
      console.log('Status error:', e.message || String(e));
    } finally {
      node._monitorLoopRunning = false;
    }
  };

  node._monitorTimer = setInterval(loop, monitorIntervalMs);
}

