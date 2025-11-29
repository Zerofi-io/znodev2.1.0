import { ethers } from 'ethers';

export function currentSlashEpoch(blockTimestamp, epochSec) {
  const ts =
    typeof blockTimestamp === 'bigint' ? Number(blockTimestamp) : Number(blockTimestamp || 0);
  if (!Number.isFinite(ts) || ts <= 0) return 0;
  return Math.floor(ts / epochSec);
}

export function selectSlashLeader(activeNodes, targetNode, epoch, salt) {
  if (!activeNodes || activeNodes.length === 0) return null;
  const seed = ethers.keccak256(
    ethers.solidityPacked(['address', 'uint256', 'bytes32'], [targetNode, epoch, salt]),
  );
  const asBigInt = BigInt(seed);
  const idx = Number(asBigInt % BigInt(activeNodes.length));
  return activeNodes[idx];
}
