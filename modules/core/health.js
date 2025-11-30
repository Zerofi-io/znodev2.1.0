import { ethers } from 'ethers';

export async function checkSelfStakeHealth(node) {
  try {
    if (node._selfSlashed) {
      return;
    }
    if (!node.staking || !node.wallet) {
      return;
    }

    const addr = node.wallet.address;
    let info;
    try {
      info = await node.staking.getNodeInfo(addr);
    } catch (e) {
      console.log('[Slashed] Failed to read self getNodeInfo:', e.message || String(e));
      return;
    }

    let stakedAmt = 0n;
    if (Array.isArray(info) && info.length >= 1) {
      try {
        stakedAmt = BigInt(info[0]);
      } catch (_ignored) {}
    }

    if (stakedAmt === undefined || stakedAmt === null) {
      stakedAmt = 0n;
    }

    const zfiDecimals = Number(process.env.ZFI_DECIMALS || 18);
    const requiredStake = BigInt(ethers.parseUnits('1000000', zfiDecimals));

    if (stakedAmt >= requiredStake || stakedAmt === 0n) {
      return;
    }

    console.log(
      '[Slashed] Detected self stake below minimum:',
      'current=',
      ethers.formatUnits(stakedAmt, zfiDecimals),
      'required=',
      ethers.formatUnits(requiredStake, zfiDecimals),
    );

    try {
      await node._retireClusterWalletAndBackups('self_slashed_below_min_stake');
    } catch (e) {
      console.log(
        '[Slashed] Error retiring cluster wallet/backups after self-slash:',
        e.message || String(e),
      );
    }

    try {
      await node._hardResetMoneroWalletState('self_slashed_below_min_stake');
    } catch (e) {
      console.log(
        '[Slashed] Error hard-resetting Monero wallet after self-slash:',
        e.message || String(e),
      );
    }

    node._selfSlashed = true;
  } catch (e) {
    console.log('[Slashed] Unexpected error in _checkSelfStakeHealth:', e.message || String(e));
  }
}

export async function checkEmergencySweep(node) {
  try {
    const enableSweep = process.env.ENABLE_EMERGENCY_SWEEP !== '0';
    if (!enableSweep) {
      return;
    }

    if (!node._activeClusterId || !node._clusterMembers || node._clusterMembers.length === 0) {
      return;
    }

    if (node._sweepInProgress) {
      return;
    }

    const sweepGracePeriodMs = Number(process.env.SWEEP_GRACE_PERIOD_MS || 3600000);
    if (node._clusterFinalizedAt && Date.now() - node._clusterFinalizedAt < sweepGracePeriodMs) {
      return;
    }

    const clusterThresholdRaw = process.env.CLUSTER_THRESHOLD;
    const clusterThresholdParsed = clusterThresholdRaw != null ? Number(clusterThresholdRaw) : NaN;
    const clusterThreshold =
      Number.isFinite(clusterThresholdParsed) && clusterThresholdParsed > 0
        ? clusterThresholdParsed
        : 7;

    const sweepOfflineMs = Number(process.env.SWEEP_OFFLINE_MS || 172800000);
    const sweepOfflineHours = sweepOfflineMs / 3600000;

    let onlineCount = 0;
    let offline48hCount = 0;
    const offlineMembers = [];

    for (const member of node._clusterMembers) {
      try {
        const canParticipate = await node.registry.canParticipate(member);
        if (!canParticipate) {
          offline48hCount += 1;
          offlineMembers.push(member);
          continue;
        }

        const info = await node.staking.getNodeInfo(member);
        if (!Array.isArray(info) || info.length < 7) {
          continue;
        }

        const active = info[3];
        const hoursOffline = Number(info[6]);

        if (!active || hoursOffline >= sweepOfflineHours) {
          offline48hCount += 1;
          offlineMembers.push(member);
        } else if (active && hoursOffline === 0) {
          onlineCount += 1;
        }
      } catch (e) {
        console.log(`[Sweep] Error checking member ${member.slice(0, 8)}: ${e.message}`);
      }
    }

    const triggerThreshold = clusterThreshold + 1;
    const shouldTrigger = onlineCount <= triggerThreshold || offline48hCount >= 3;

    if (!shouldTrigger) {
      return;
    }

    console.log(`\n[WARN]  EMERGENCY SWEEP CONDITION DETECTED`);
    console.log(`  Online nodes: ${onlineCount}/${node._clusterMembers.length}`);
    console.log(`  Offline 48h+: ${offline48hCount}/${node._clusterMembers.length}`);
    console.log(`  Trigger threshold: ${triggerThreshold} online nodes OR 3+ offline`);

    if (offline48hCount >= 3) {
      try {
        await node._retireClusterWalletAndBackups('cluster_dead_offline_48h');
      } catch (e) {
        console.log(
          '[Cluster] Failed to retire dead cluster wallet/backups:',
          e.message || String(e),
        );
      }

      try {
        if (typeof node._clearClusterState === 'function') {
          node._clearClusterState();
        }
        if (node._activeClusterId) {
          try {
            await node._cleanupClusterAttempt(node._activeClusterId);
          } catch (cleanupErr) {
            console.log(
              '[Cluster] Cleanup error while retiring dead cluster:',
              cleanupErr.message || String(cleanupErr),
            );
          }
        }
        node._activeClusterId = null;
        node._clusterMembers = null;
        node._clusterFinalAddress = null;
        node._clusterFinalizationStartAt = null;
        node._clusterFinalized = false;
        node.clusterWalletName = null;
        if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
          node.p2p.setActiveCluster(null);
        }
      } catch (stateErr) {
        console.log(
          '[Cluster] Error while clearing dead cluster state:',
          stateErr.message || String(stateErr),
        );
      }
    }

    const recoveryAddress = process.env.RECOVERY_SWEEP_ADDRESS;
    if (!recoveryAddress) {
      console.log(
        `[ERROR] RECOVERY_SWEEP_ADDRESS not configured, cannot perform emergency sweep`,
      );
      return;
    }

    console.log(`  [INFO] Checking for other live clusters to sweep to...`);
    const otherLiveCluster = await findOtherLiveCluster(node, node._activeClusterId, clusterThreshold);

    let destinationAddress;
    let destinationType;

    if (otherLiveCluster) {
      destinationAddress = otherLiveCluster.moneroAddress;
      destinationType = 'cluster';
      console.log(
        `  [OK] Found live cluster ${otherLiveCluster.clusterId.slice(0, 16)}... with ${otherLiveCluster.onlineCount} online nodes`,
      );
      console.log(
        `  [INFO] Will sweep to cluster Monero address: ${destinationAddress.slice(0, 20)}...`,
      );
    } else {
      destinationAddress = recoveryAddress;
      destinationType = 'recovery';
      console.log(`  [INFO]  No other live clusters found`);
      console.log(
        `  [INFO] Will sweep to recovery address: ${destinationAddress.slice(0, 20)}...`,
      );
    }

    console.log(`\n[Sweep] Emergency sweep framework ready`);
    console.log(
      `[Sweep] Destination: ${destinationType} - ${destinationAddress.slice(0, 20)}...`,
    );
    console.log(
      `[Sweep] Offline members: ${offlineMembers.map((m) => m.slice(0, 8)).join(', ')}`,
    );
    console.log(
      `[Sweep] Note: P2P consensus and multisig signing to be implemented in future iteration`,
    );
  } catch (e) {
    console.log(`[Sweep] Emergency sweep check error: ${e.message}`);
  }
}

async function findOtherLiveCluster(node, currentClusterId, clusterThreshold) {
  try {
    const clusterSizeRaw = process.env.CLUSTER_SIZE;
    const clusterSizeParsed = clusterSizeRaw != null ? Number(clusterSizeRaw) : NaN;
    const clusterSize =
      Number.isFinite(clusterSizeParsed) && clusterSizeParsed > 0 ? clusterSizeParsed : 11;

    const maxScan = Number(process.env.MAX_REGISTERED_SCAN || 256);
    const registeredNodes = [];
    let offset = 0;

    while (registeredNodes.length < 1000) {
      const page = await node.registry.getRegisteredNodes(offset, maxScan);
      if (!Array.isArray(page) || page.length === 0) {
        break;
      }

      for (const addr of page) {
        if (addr && addr !== ethers.ZeroAddress) {
          registeredNodes.push(addr);
        }
      }

      if (page.length < maxScan) {
        break;
      }
      offset += maxScan;
    }

    const eligibleNodes = [];
    for (const addr of registeredNodes) {
      try {
        const canParticipate = await node.registry.canParticipate(addr);
        if (canParticipate) {
          eligibleNodes.push(addr);
        }
      } catch (_ignored) {}
    }

    if (eligibleNodes.length < clusterSize) {
      return null;
    }

    const clusterCandidates = [];
    const blockNumber = await node.provider.getBlockNumber();
    const rawSpan = process.env.SELECTION_EPOCH_BLOCKS;
    const parsedSpan = rawSpan != null ? Number(rawSpan) : NaN;
    const epochSpan = Number.isFinite(parsedSpan) && parsedSpan > 0 ? parsedSpan : 20;

    for (let epochOffset = 0; epochOffset < 10; epochOffset += 1) {
      const epoch = ((Number(blockNumber) / epochSpan) | 0) - epochOffset;
      const epochSeed = ethers.keccak256(ethers.solidityPacked(['uint256'], [epoch]));

      const uniqueCandidates = [...new Set(eligibleNodes.map((a) => a.toLowerCase()))];
      const scored = uniqueCandidates.map((lower) => {
        const score = ethers.keccak256(
          ethers.solidityPacked(['bytes32', 'address'], [epochSeed, lower]),
        );
        return { lower, score };
      });

      scored.sort((a, b) => a.score.localeCompare(b.score));
      const chosen = scored.slice(0, clusterSize);
      const membersLower = chosen.map((x) => x.lower);
      const members = membersLower.map((addr) => ethers.getAddress(addr));
      const sortedMembersLower = [...membersLower].sort();
      const addressTypes = Array(clusterSize).fill('address');
      const clusterId = ethers.keccak256(ethers.solidityPacked(addressTypes, sortedMembersLower));

      if (clusterId === currentClusterId) {
        continue;
      }

      try {
        const clusterInfo = await node.registry.clusters(clusterId);
        const finalized = clusterInfo && clusterInfo[3];
        if (!finalized) {
          continue;
        }

        const moneroAddress = clusterInfo[1];
        if (!moneroAddress || moneroAddress.length < 20) {
          continue;
        }

        let onlineCount = 0;
        for (const member of members) {
          try {
            const info = await node.staking.getNodeInfo(member);
            if (Array.isArray(info) && info.length >= 7) {
              const active = info[3];
              const hoursOffline = Number(info[6]);
              if (active && hoursOffline === 0) {
                onlineCount += 1;
              }
            }
          } catch (_ignored) {}
        }

        if (onlineCount >= clusterThreshold + 1) {
          clusterCandidates.push({
            clusterId,
            moneroAddress,
            onlineCount,
            createdAt: clusterInfo[2],
            members,
          });
        }
      } catch (_ignored) {}
    }

    if (clusterCandidates.length === 0) {
      return null;
    }

    clusterCandidates.sort((a, b) => Number(b.createdAt) - Number(a.createdAt));
    return clusterCandidates[0];
  } catch (e) {
    console.log(`[Sweep] Error finding other live clusters: ${e.message}`);
    return null;
  }
}

