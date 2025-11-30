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

    // Node is slashed but not fully - don't retire wallet, just log
    console.log(
      '[Slashed] Detected self stake below minimum:',
      'current=',
      ethers.formatUnits(stakedAmt, zfiDecimals),
      'required=',
      ethers.formatUnits(requiredStake, zfiDecimals),
    );
    console.log('[Slashed] Node can rejoin cluster after topping up stake to 1M ZFI');

    // Check if blacklisted (fully slashed) - only then retire
    let isBlacklisted = false;
    try {
      isBlacklisted = await node.staking.isBlacklisted(addr);
    } catch (_ignored) {}

    if (isBlacklisted) {
      console.log('[Slashed] Node is BLACKLISTED - retiring cluster wallet');
      try {
        await node._retireClusterWalletAndBackups('blacklisted');
      } catch (e) {
        console.log('[Slashed] Error retiring cluster wallet:', e.message || String(e));
      }
      try {
        await node._hardResetMoneroWalletState('blacklisted');
      } catch (e) {
        console.log('[Slashed] Error resetting Monero wallet:', e.message || String(e));
      }
      node._selfSlashed = true;
    } else {
      // Slashed but not blacklisted - keep wallet and cluster state
      // Node operator should top up stake to rejoin
      console.log('[Slashed] Keeping cluster wallet and keys - top up stake to rejoin');
    }
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

    let healthyCount = 0;
    let slashedCount = 0;
    let blacklistedCount = 0;
    const healthyMembers = [];
    const slashedMembers = [];
    const blacklistedMembers = [];

    for (const member of node._clusterMembers) {
      try {
        const isBlacklisted = await node.staking.isBlacklisted(member);
        if (isBlacklisted) {
          blacklistedCount += 1;
          blacklistedMembers.push(member);
          continue;
        }

        const info = await node.staking.getNodeInfo(member);
        if (!Array.isArray(info) || info.length < 7) {
          continue;
        }

        const stakedAmount = BigInt(info[0]);
        const active = info[3];
        const hoursOffline = Number(info[6]);
        const zfiDecimals = Number(process.env.ZFI_DECIMALS || 18);
        const requiredStake = BigInt(ethers.parseUnits('1000000', zfiDecimals));

        if (!active || stakedAmount < requiredStake) {
          slashedCount += 1;
          slashedMembers.push(member);
        } else if (hoursOffline === 0) {
          healthyCount += 1;
          healthyMembers.push(member);
        } else {
          // Active but offline - could go either way
          healthyCount += 1;
          healthyMembers.push(member);
        }
      } catch (e) {
        console.log(`[Sweep] Error checking member ${member.slice(0, 8)}: ${e.message}`);
      }
    }

    const clusterId = node._activeClusterId;

    // Log cluster health periodically
    if (slashedCount > 0 || blacklistedCount > 0) {
      console.log(`[Cluster] Health: ${healthyCount} healthy, ${slashedCount} slashed, ${blacklistedCount} blacklisted`);
    }

    // Only dissolve when 3+ members are BLACKLISTED (permanent)
    // Slashed members can still top up and rejoin
    if (blacklistedCount >= 3) {
      console.log(`\n[WARN] CLUSTER DISSOLUTION CONDITION DETECTED`);
      console.log(`  Blacklisted members: ${blacklistedCount}/11`);
      console.log(`  Slashed members: ${slashedCount}/11`);
      console.log(`  Healthy members: ${healthyCount}/11`);

      // Attempt on-chain cluster dissolution
      await attemptClusterDissolution(node, clusterId, healthyMembers);

      // Only retire wallet AFTER successful dissolution
      const isActive = await node.registry.isClusterActive(clusterId);
      if (!isActive) {
        console.log('[Cluster] Cluster dissolved on-chain, retiring wallet');
        try {
          await node._retireClusterWalletAndBackups('cluster_dissolved');
        } catch (e) {
          console.log('[Cluster] Failed to retire wallet:', e.message || String(e));
        }

        try {
          if (typeof node._clearClusterState === 'function') {
            node._clearClusterState();
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
          console.log('[Cluster] Error clearing state:', stateErr.message || String(stateErr));
        }
      }
      return;
    }

    // If many nodes are slashed (but not blacklisted), log warning but keep operating
    // They can top up and rejoin
    if (slashedCount >= 3) {
      console.log(`[WARN] ${slashedCount} cluster members are slashed but can rejoin after topping up`);
      console.log(`[INFO] Cluster continues operating with ${healthyCount} healthy members`);
    }

    // Emergency sweep to recovery address if below signing threshold
    // But DON'T retire wallet or clear state - slashed nodes can rejoin
    const signingThreshold = clusterThreshold;
    if (healthyCount < signingThreshold) {
      console.log(`\n[WARN] BELOW SIGNING THRESHOLD`);
      console.log(`  Healthy: ${healthyCount}, Need: ${signingThreshold}`);
      console.log(`  Slashed members can rejoin after topping up stake`);

      const recoveryAddress = process.env.RECOVERY_SWEEP_ADDRESS;
      if (recoveryAddress) {
        console.log(`[Sweep] Consider sweeping funds to recovery: ${recoveryAddress.slice(0, 20)}...`);
      }
    }
  } catch (e) {
    console.log(`[Sweep] Emergency sweep check error: ${e.message}`);
  }
}

/**
 * Attempt to dissolve a dead cluster on-chain.
 * Only called when 3+ members are BLACKLISTED (not just slashed).
 */
async function attemptClusterDissolution(node, clusterId, healthyMembers) {
  if (!clusterId || !node.registry) {
    return;
  }

  console.log(`[Dissolve] Checking on-chain dissolution eligibility...`);

  try {
    const [canDissolveBool, blacklistedCount] = await node.registry.canDissolve(clusterId);
    if (!canDissolveBool) {
      console.log(`[Dissolve] Cluster not eligible (${blacklistedCount} blacklisted, need 3+)`);
      return;
    }
    console.log(`[Dissolve] Cluster eligible: ${blacklistedCount} blacklisted members`);
  } catch (e) {
    console.log(`[Dissolve] Error checking eligibility: ${e.message}`);
    return;
  }

  const shuffled = shuffleArray([...healthyMembers]);
  const selfAddress = node.wallet?.address?.toLowerCase();

  if (selfAddress) {
    const selfIdx = shuffled.findIndex((m) => m.toLowerCase() === selfAddress);
    if (selfIdx > 0) {
      shuffled.splice(selfIdx, 1);
      shuffled.unshift(node.wallet.address);
    }
  }

  console.log(`[Dissolve] Attempting with ${shuffled.length} healthy nodes...`);

  for (let i = 0; i < shuffled.length; i++) {
    const candidate = shuffled[i];
    const isSelf = selfAddress && candidate.toLowerCase() === selfAddress;

    if (isSelf) {
      const success = await tryDissolveSelf(node, clusterId);
      if (success) {
        console.log(`[Dissolve] Successfully dissolved cluster on-chain`);
        return;
      }
      console.log(`[Dissolve] Self dissolution failed, trying next...`);
    } else {
      const success = await requestPeerDissolution(node, clusterId, candidate);
      if (success) {
        console.log(`[Dissolve] Peer ${candidate.slice(0, 8)} dissolved cluster`);
        return;
      }
      console.log(`[Dissolve] Peer ${candidate.slice(0, 8)} failed, trying next...`);
    }
  }

  console.log(`[Dissolve] All nodes failed. Cluster dissolvable by anyone for reward.`);
}

async function tryDissolveSelf(node, clusterId) {
  try {
    const gasPrice = await node.provider.getFeeData();
    const estimatedGas = await node.registry.dissolveCluster.estimateGas(clusterId);
    const gasCost = estimatedGas * (gasPrice.gasPrice || 0n);

    const balance = await node.provider.getBalance(node.wallet.address);
    if (balance < gasCost * 12n / 10n) {
      console.log(`[Dissolve] Insufficient gas balance`);
      return false;
    }

    console.log(`[Dissolve] Submitting dissolution transaction...`);
    const tx = await node.registry.dissolveCluster(clusterId);
    const receipt = await tx.wait();

    if (receipt.status === 1) {
      console.log(`[Dissolve] Confirmed in block ${receipt.blockNumber}`);
      return true;
    }

    console.log(`[Dissolve] Transaction failed`);
    return false;
  } catch (e) {
    const msg = e.message || String(e);
    if (msg.includes('insufficient funds')) {
      console.log(`[Dissolve] Insufficient funds for gas`);
    } else if (msg.includes('already dissolved')) {
      console.log(`[Dissolve] Cluster already dissolved`);
      return true;
    } else {
      console.log(`[Dissolve] Error: ${msg.slice(0, 100)}`);
    }
    return false;
  }
}

async function requestPeerDissolution(node, clusterId, peerAddress) {
  if (!node.p2p || typeof node.p2p.sendDirectMessage !== 'function') {
    return false;
  }

  try {
    const response = await node.p2p.sendDirectMessage(peerAddress, {
      type: 'cluster/dissolve-request',
      clusterId,
      requester: node.wallet?.address,
      timestamp: Date.now(),
    }, 30000);

    return response && response.success === true;
  } catch (e) {
    console.log(`[Dissolve] P2P request failed: ${e.message}`);
    return false;
  }
}

export async function handleDissolutionRequest(node, message) {
  const { clusterId, requester } = message;

  if (!clusterId || !node.registry) {
    return { success: false, error: 'invalid_request' };
  }

  if (node._activeClusterId !== clusterId) {
    return { success: false, error: 'not_in_cluster' };
  }

  console.log(`[Dissolve] Received request from ${requester?.slice(0, 8) || 'unknown'}`);

  const success = await tryDissolveSelf(node, clusterId);
  return { success, error: success ? null : 'dissolution_failed' };
}

function shuffleArray(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}
