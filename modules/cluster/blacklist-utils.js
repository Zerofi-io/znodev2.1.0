export function pruneBlacklistEntries(blacklist, now) {
  const input = blacklist || {};
  const pruned = {};
  let active = 0;
  for (const [clusterId, expiryTime] of Object.entries(input)) {
    if (expiryTime > now) {
      pruned[clusterId] = expiryTime;
      active++;
    }
  }
  return { pruned, active };
}

export function computeAdaptiveBlacklistCooldownMs(failureCount, baseCooldownMs) {
  return baseCooldownMs;
}
