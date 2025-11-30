package p2p

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const ProtocolPreSelection = "/znode/preselection/1.0.0"
// safeTruncate safely truncates a string to maxLen
func safeTruncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}


// PreSelectionProposal represents a coordinator's proposed cluster formation
type PreSelectionProposal struct {
	ProposalID  string   `json:"proposalId"`
	BlockNumber uint64   `json:"blockNumber"`
	EpochSeed   string   `json:"epochSeed"`
	Candidates  []string `json:"candidates"`
	Coordinator string   `json:"coordinator"`
	Timestamp   int64    `json:"timestamp"`
	Signature   string   `json:"signature"`
}

// PreSelectionVote represents a node's vote on a proposal
type PreSelectionVote struct {
	ProposalID string `json:"proposalId"`
	Voter      string `json:"voter"`
	Approved   bool   `json:"approved"`
	Timestamp  int64  `json:"timestamp"`
	Signature  string `json:"signature"`
}

// PreSelectionState tracks consensus state for a pre-selection round
type PreSelectionState struct {
	mu         sync.RWMutex
	ProposalID string
	Proposal   *PreSelectionProposal
	Votes      map[string]*PreSelectionVote
	Completed  bool
	Approved   bool
	StartTime  time.Time
}

// PreSelectionManager handles pre-selection consensus
type PreSelectionManager struct {
	mu      sync.RWMutex
	host    *Host
	states  map[string]*PreSelectionState
	pending map[string]*PreSelectionProposal
}

// NewPreSelectionManager creates a new pre-selection manager
func NewPreSelectionManager(h *Host) *PreSelectionManager {
	return &PreSelectionManager{
		host:    h,
		states:  make(map[string]*PreSelectionState),
		pending: make(map[string]*PreSelectionProposal),
	}
}

// SetupHandler registers the pre-selection protocol handler
func (psm *PreSelectionManager) SetupHandler() {
	psm.host.host.SetStreamHandler(ProtocolPreSelection, psm.handleStream)
	log.Printf("[PreSelection] Protocol handler registered for %s", ProtocolPreSelection)
}

// computeProposalID generates a deterministic proposal ID from block and candidates
func computeProposalID(blockNumber uint64, candidates []string) string {
	sorted := make([]string, len(candidates))
	copy(sorted, candidates)
	sort.Strings(sorted)
	data := fmt.Sprintf("%d|%s", blockNumber, strings.Join(sorted, ","))
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16])
}

// BroadcastProposal sends a pre-selection proposal to all eligible candidates
func (psm *PreSelectionManager) BroadcastProposal(blockNumber uint64, epochSeed string, candidates []string) (*PreSelectionProposal, error) {
	proposalID := computeProposalID(blockNumber, candidates)
	proposal := &PreSelectionProposal{
		ProposalID:  proposalID,
		BlockNumber: blockNumber,
		EpochSeed:   epochSeed,
		Candidates:  candidates,
		Coordinator: strings.ToLower(psm.host.ethAddress),
		Timestamp:   time.Now().UnixMilli(),
	}

	payload := fmt.Sprintf("PROPOSAL|%s|%d|%s|%s",
		proposalID, blockNumber, epochSeed, strings.Join(candidates, ","))
	sig, err := psm.host.signPayload(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to sign proposal: %w", err)
	}
	proposal.Signature = sig

	state := &PreSelectionState{
		ProposalID: proposalID,
		Proposal:   proposal,
		Votes:      make(map[string]*PreSelectionVote),
		StartTime:  time.Now(),
	}

	psm.mu.Lock()
	psm.states[proposalID] = state
	psm.mu.Unlock()

	selfVote := &PreSelectionVote{
		ProposalID: proposalID,
		Voter:      strings.ToLower(psm.host.ethAddress),
		Approved:   true,
		Timestamp:  time.Now().UnixMilli(),
	}
	votePayload := fmt.Sprintf("VOTE|%s|%s|%t", proposalID, selfVote.Voter, selfVote.Approved)
	selfVote.Signature, _ = psm.host.signPayload(votePayload)
	state.mu.Lock()
	state.Votes[selfVote.Voter] = selfVote
	state.mu.Unlock()

	msgData, _ := json.Marshal(proposal)
	msg := fmt.Sprintf("PROPOSAL|%s\n", string(msgData))
	psm.broadcastToAddresses(candidates, msg)

	log.Printf("[PreSelection] Broadcast proposal %s with %d candidates at block %d",
		safeTruncate(proposalID, 8), len(candidates), blockNumber)
	return proposal, nil
}

// VoteOnProposal votes on a received proposal
func (psm *PreSelectionManager) VoteOnProposal(proposalID string, approved bool, localCandidates []string) error {
	psm.mu.RLock()
	state := psm.states[proposalID]
	proposal := psm.pending[proposalID]
	psm.mu.RUnlock()

	if state == nil && proposal == nil {
		return fmt.Errorf("proposal %s not found", proposalID)
	}

	if state == nil {
		state = &PreSelectionState{
			ProposalID: proposalID,
			Proposal:   proposal,
			Votes:      make(map[string]*PreSelectionVote),
			StartTime:  time.Now(),
		}
		psm.mu.Lock()
		psm.states[proposalID] = state
		delete(psm.pending, proposalID)
		psm.mu.Unlock()
	}

	vote := &PreSelectionVote{
		ProposalID: proposalID,
		Voter:      strings.ToLower(psm.host.ethAddress),
		Approved:   approved,
		Timestamp:  time.Now().UnixMilli(),
	}
	payload := fmt.Sprintf("VOTE|%s|%s|%t", proposalID, vote.Voter, vote.Approved)
	sig, err := psm.host.signPayload(payload)
	if err != nil {
		return fmt.Errorf("failed to sign vote: %w", err)
	}
	vote.Signature = sig

	state.mu.Lock()
	state.Votes[vote.Voter] = vote
	state.mu.Unlock()

	msgData, _ := json.Marshal(vote)
	msg := fmt.Sprintf("VOTE|%s\n", string(msgData))

	if state.Proposal != nil {
		psm.broadcastToAddresses(state.Proposal.Candidates, msg)
	}

	log.Printf("[PreSelection] Voted %t on proposal %s", approved, safeTruncate(proposalID, 8))
	return nil
}

// WaitForConsensus waits for all candidates to vote on a proposal
func (psm *PreSelectionManager) WaitForConsensus(proposalID string, expectedVoters []string, timeoutMs int) (*PreSelectionResult, error) {
	psm.mu.RLock()
	state := psm.states[proposalID]
	psm.mu.RUnlock()

	if state == nil {
		return nil, fmt.Errorf("proposal %s not found", proposalID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	expectedSet := make(map[string]bool)
	for _, addr := range expectedVoters {
		expectedSet[strings.ToLower(addr)] = true
	}
	requiredCount := len(expectedSet)

	for {
		select {
		case <-ticker.C:
			state.mu.RLock()
			approvedCount := 0
			var missing []string
			for addr := range expectedSet {
				vote, ok := state.Votes[addr]
				if ok && vote.Approved {
					approvedCount++
				} else if !ok {
					missing = append(missing, addr)
				}
			}
			completed := state.Completed
			state.mu.RUnlock()

			if completed {
				return &PreSelectionResult{
					ProposalID: proposalID,
					Success:    state.Approved,
					Approved:   approvedCount,
					Total:      requiredCount,
				}, nil
			}

			if approvedCount == requiredCount {
				state.mu.Lock()
				state.Completed = true
				state.Approved = true
				state.mu.Unlock()
				log.Printf("[PreSelection] Consensus reached for %s (%d/%d approved)",
					safeTruncate(proposalID, 8), approvedCount, requiredCount)
				return &PreSelectionResult{
					ProposalID: proposalID,
					Success:    true,
					Approved:   approvedCount,
					Total:      requiredCount,
				}, nil
			}

			if time.Now().After(deadline) {
				state.mu.Lock()
				state.Completed = true
				state.Approved = false
				state.mu.Unlock()
				log.Printf("[PreSelection] Timeout for %s (%d/%d approved, missing: %v)",
					safeTruncate(proposalID, 8), approvedCount, requiredCount, missing)
				return &PreSelectionResult{
					ProposalID: proposalID,
					Success:    false,
					Approved:   approvedCount,
					Total:      requiredCount,
					Missing:    missing,
				}, fmt.Errorf("timeout waiting for consensus")
			}
		}
	}
}

// PreSelectionResult contains the outcome of a pre-selection consensus
type PreSelectionResult struct {
	ProposalID string   `json:"proposalId"`
	Success    bool     `json:"success"`
	Approved   int      `json:"approved"`
	Total      int      `json:"total"`
	Missing    []string `json:"missing,omitempty"`
}

// GetPendingProposal returns a pending proposal by ID
func (psm *PreSelectionManager) GetPendingProposal(proposalID string) *PreSelectionProposal {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	if p, ok := psm.pending[proposalID]; ok {
		return p
	}
	if s, ok := psm.states[proposalID]; ok && s.Proposal != nil {
		return s.Proposal
	}
	return nil
}

// GetLatestProposal returns the most recent pending proposal
func (psm *PreSelectionManager) GetLatestProposal() *PreSelectionProposal {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	var latest *PreSelectionProposal
	for _, p := range psm.pending {
		if latest == nil || p.Timestamp > latest.Timestamp {
			latest = p
		}
	}
	for _, s := range psm.states {
		if s.Proposal != nil && (latest == nil || s.Proposal.Timestamp > latest.Timestamp) {
			latest = s.Proposal
		}
	}
	return latest
}

// Cleanup removes old proposal states
func (psm *PreSelectionManager) Cleanup(maxAge time.Duration) {
	psm.mu.Lock()
	defer psm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for id, state := range psm.states {
		if state.StartTime.Before(cutoff) {
			delete(psm.states, id)
		}
	}
	for id, proposal := range psm.pending {
		if time.UnixMilli(proposal.Timestamp).Before(cutoff) {
			delete(psm.pending, id)
		}
	}
}

func (psm *PreSelectionManager) handleStream(s network.Stream) {
	defer s.Close()
	reader := bufio.NewReader(s)
	data, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	data = strings.TrimSpace(data)
	parts := strings.SplitN(data, "|", 2)
	if len(parts) < 2 {
		return
	}

	msgType := parts[0]
	payload := parts[1]

	switch msgType {
	case "PROPOSAL":
		var proposal PreSelectionProposal
		if err := json.Unmarshal([]byte(payload), &proposal); err != nil {
			log.Printf("[PreSelection] Invalid proposal: %v", err)
			return
		}
		psm.handleProposal(&proposal)
	case "VOTE":
		var vote PreSelectionVote
		if err := json.Unmarshal([]byte(payload), &vote); err != nil {
			log.Printf("[PreSelection] Invalid vote: %v", err)
			return
		}
		psm.handleVote(&vote)
	}
	s.Write([]byte("ACK"))
}

func (psm *PreSelectionManager) handleProposal(proposal *PreSelectionProposal) {
	payload := fmt.Sprintf("PROPOSAL|%s|%d|%s|%s",
		proposal.ProposalID, proposal.BlockNumber, proposal.EpochSeed,
		strings.Join(proposal.Candidates, ","))
	if !verifyPayloadSignature(payload, proposal.Signature, proposal.Coordinator) {
		log.Printf("[PreSelection] Invalid signature on proposal from %s", safeTruncate(proposal.Coordinator, 8))
		return
	}

	psm.mu.Lock()
	if _, exists := psm.states[proposal.ProposalID]; !exists {
		psm.pending[proposal.ProposalID] = proposal
	}
	psm.mu.Unlock()

	log.Printf("[PreSelection] Received proposal %s from %s (block %d, %d candidates)",
		safeTruncate(proposal.ProposalID, 8), safeTruncate(proposal.Coordinator, 8), proposal.BlockNumber, len(proposal.Candidates))
}

func (psm *PreSelectionManager) handleVote(vote *PreSelectionVote) {
	payload := fmt.Sprintf("VOTE|%s|%s|%t", vote.ProposalID, vote.Voter, vote.Approved)
	if !verifyPayloadSignature(payload, vote.Signature, vote.Voter) {
		log.Printf("[PreSelection] Invalid signature on vote from %s", safeTruncate(vote.Voter, 8))
		return
	}

	psm.mu.RLock()
	state := psm.states[vote.ProposalID]
	psm.mu.RUnlock()

	if state == nil {
		return
	}

	state.mu.Lock()
	state.Votes[vote.Voter] = vote
	state.mu.Unlock()

	log.Printf("[PreSelection] Received vote from %s on %s: %t",
		safeTruncate(vote.Voter, 8), safeTruncate(vote.ProposalID, 8), vote.Approved)
}

func (psm *PreSelectionManager) broadcastToAddresses(addresses []string, msg string) {
	selfAddr := strings.ToLower(psm.host.ethAddress)
	for _, addr := range addresses {
		addrLower := strings.ToLower(addr)
		if addrLower == selfAddr {
			continue
		}

		peerID := psm.host.getPeerIDForAddress(addrLower)
		if peerID == "" {
			continue
		}

		pid, err := peer.Decode(peerID)
		if err != nil {
			continue
		}

		go func(pid peer.ID, m string) {
			ctx, cancel := context.WithTimeout(psm.host.ctx, 10*time.Second)
			defer cancel()

			s, err := psm.host.host.NewStream(ctx, pid, ProtocolPreSelection)
			if err != nil {
				return
			}
			defer s.Close()
			s.Write([]byte(m))
			bufio.NewReader(s).ReadString('\n')
		}(pid, msg)
	}
}

// StartCleanup starts a background goroutine to periodically clean up old states
func (psm *PreSelectionManager) StartCleanup(ctx context.Context, interval time.Duration, maxAge time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				psm.Cleanup(maxAge)
			}
		}
	}()
	log.Printf("[PreSelection] Cleanup routine started (interval=%v, maxAge=%v)", interval, maxAge)
}
