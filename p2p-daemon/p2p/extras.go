package p2p

import (
	"strings"
	"time"
)

// ClearClusterSignatures clears signatures for specific rounds.
// If rounds is empty, clears all signature rounds for the cluster.
func (h *Host) ClearClusterSignatures(clusterID string, rounds []int) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if len(rounds) == 0 {
		// Clear all signature rounds for this cluster.
		for round := range cs.Signatures {
			delete(cs.Signatures, round)
		}
		return
	}

	for _, round := range rounds {
		delete(cs.Signatures, round)
	}
}

// ClearClusterRounds clears round data for specific rounds.
// If rounds is empty, clears all round data for the cluster.
func (h *Host) ClearClusterRounds(clusterID string, rounds []int) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if len(rounds) == 0 {
		// Clear all round data for this cluster.
		for round := range cs.RoundData {
			delete(cs.RoundData, round)
		}
		return
	}

	for _, round := range rounds {
		delete(cs.RoundData, round)
	}
}

// GetLastHeartbeat returns the last heartbeat timestamp for a specific address (Unix ms)
func (h *Host) GetLastHeartbeat(address string) int64 {
	hm := h.GetHeartbeatManager()
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	addrLower := strings.ToLower(address)
	if presence, ok := hm.recentPeers[addrLower]; ok {
		return presence.LastSeen.UnixMilli()
	}
	return 0
}

// GetHeartbeats returns heartbeat timestamps for recent peers (Unix ms)
func (h *Host) GetHeartbeats(ttlMs int) map[string]int64 {
	hm := h.GetHeartbeatManager()
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	if ttlMs <= 0 {
		ttlMs = hm.ttlMs
	}

	cutoff := time.Now().Add(-time.Duration(ttlMs) * time.Millisecond)
	result := make(map[string]int64)

	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.After(cutoff) {
			result[addr] = presence.LastSeen.UnixMilli()
		}
	}
	return result
}
