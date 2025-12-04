package p2p

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Protocol ID for PBFT consensus
const ProtocolPBFT = "/znode/pbft/1.0.0"

// PBFT Message Types
const (
	MsgPrePrepare = "PRE-PREPARE"
	MsgPrepare    = "PREPARE"
	MsgCommit     = "COMMIT"
	MsgViewChange = "VIEW-CHANGE"
)

// Timeouts (can be overridden via env)
var (
	PrePrepareTimeout = getEnvDuration("PBFT_PREPREPARE_TIMEOUT_MS", 30000)
	PrepareTimeout    = getEnvDuration("PBFT_PREPARE_TIMEOUT_MS", 60000)
	CommitTimeout     = getEnvDuration("PBFT_COMMIT_TIMEOUT_MS", 60000)
)

func getEnvDuration(key string, defaultMs int) time.Duration {
	if val := os.Getenv(key); val != "" {
		if ms, err := strconv.Atoi(val); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return time.Duration(defaultMs) * time.Millisecond
}

// PBFTMessage represents a PBFT protocol message
type PBFTMessage struct {
	Type       string `json:"type"`
	ClusterID  string `json:"clusterId"`
	Phase      string `json:"phase"`
	ViewNumber int    `json:"viewNumber"`
	Digest     string `json:"digest"`
	Address    string `json:"address"`
	Signature  string `json:"signature"`
	Timestamp  int64  `json:"timestamp"`
}

// PBFTConsensusState tracks the state of a single consensus round
type PBFTConsensusState struct {
	ClusterID          string
	Phase              string
	ViewNumber         int
	Digest             string
	PrePrepareReceived bool
	Prepares           map[string]*PBFTMessage // address -> PREPARE message
	Commits            map[string]*PBFTMessage // address -> COMMIT message
	ViewChanges        map[string]int          // address -> requested view number
	Members            []string                // canonical ordered members (lowercase)
	Coordinator        string                  // current coordinator address
	StartTime          time.Time
	Completed          bool
	Aborted            bool
	AbortReason        string
	mu                 sync.RWMutex
}

// ConsensusResult is returned when consensus completes or fails
type ConsensusResult struct {
	Success      bool     `json:"success"`
	Phase        string   `json:"phase"`
	ViewNumber   int      `json:"viewNumber"`
	Digest       string   `json:"digest"`
	Participants []string `json:"participants"`
	Missing      []string `json:"missing"`
	Aborted      bool     `json:"aborted"`
	AbortReason  string   `json:"abortReason,omitempty"`
}

// PBFTManager handles PBFT consensus for a host
type PBFTManager struct {
	host        *Host
	consensus   map[string]*PBFTConsensusState // clusterID:phase -> state
	pendingMsgs map[string][]*PBFTMessage      // clusterID:phase -> buffered messages
	mu          sync.RWMutex
}

// NewPBFTManager creates a new PBFT manager
func NewPBFTManager(h *Host) *PBFTManager {
	return &PBFTManager{
		host:        h,
		consensus:   make(map[string]*PBFTConsensusState),
		pendingMsgs: make(map[string][]*PBFTMessage),
	}
}

// pbftConsensusKey generates a unique key for a consensus instance
func pbftConsensusKey(clusterID, phase string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(clusterID), phase)
}

// getPBFTCoordinator determines the coordinator for a given view number
// For view 0: uses keccak256(clusterId) % len(members) to match JS coordinator selection
// For view > 0: rotates from initial coordinator by viewNumber
func getPBFTCoordinator(members []string, viewNumber int, clusterID string) string {
	if len(members) == 0 {
		return ""
	}

	// Normalize clusterID and decode as bytes32, matching JS:
	// JS: ethers.keccak256(ethers.solidityPacked(['bytes32'], [clusterId]))
	clusterID = strings.ToLower(clusterID)
	// common.FromHex handles optional 0x prefix
	clusterBytes := common.FromHex(clusterID)
	if len(clusterBytes) == 0 {
		// Fallback: hash the UTF-8 string to avoid panics, but this should not happen
		clusterBytes = []byte(clusterID)
	}

	hash := crypto.Keccak256Hash(clusterBytes)

	// IMPORTANT: use the full 256-bit hash when computing the index.
	// Using only Uint64() would drop high bits and change (hash % N).
	mod := new(big.Int).Mod(hash.Big(), big.NewInt(int64(len(members))))
	initialIndex := int(mod.Int64())

	// For view changes, rotate from initial position
	finalIndex := (initialIndex + viewNumber) % len(members)
	return members[finalIndex]
}

// pbftCanonicalizeMembers returns lowercase sorted member addresses
func pbftCanonicalizeMembers(members []string) []string {
	result := make([]string, len(members))
	for i, m := range members {
		result[i] = strings.ToLower(m)
	}
	sort.Strings(result)
	return result
}

func pbftFaultTolerance(n int) int {
	if n <= 1 {
		return 0
	}
	return (n - 1) / 3
}

func pbftQuorum(n int) int {
	if n <= 0 {
		return 0
	}
	f := pbftFaultTolerance(n)
	q := 2*f + 1
	if q < 1 {
		q = 1
	}
	if q > n {
		q = n
	}
	return q
}

// computePBFTDigest computes Keccak256 hash of phase data
func computePBFTDigest(clusterID, phase string, data string) string {
	input := fmt.Sprintf("%s|%s|%s", strings.ToLower(clusterID), phase, data)
	hash := crypto.Keccak256([]byte(input))
	return hex.EncodeToString(hash)
}

// SetupHandler sets up the PBFT stream handler on the host
func (pm *PBFTManager) SetupHandler() {
	pm.host.host.SetStreamHandler(ProtocolPBFT, pm.handlePBFTStream)
	log.Printf("[PBFT] Stream handler registered for %s", ProtocolPBFT)
}

// handlePBFTStream handles incoming PBFT protocol messages
func (pm *PBFTManager) handlePBFTStream(s network.Stream) {
	defer s.Close()

	reader := bufio.NewReader(s)
	data, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[PBFT] Error reading stream: %v", err)
		return
	}
	data = strings.TrimSpace(data)

	// Parse message: TYPE|CLUSTER_ID|PHASE|VIEW_NUMBER|DIGEST|ADDRESS|SIGNATURE|TIMESTAMP
	parts := strings.Split(data, "|")
	if len(parts) < 8 {
		log.Printf("[PBFT] Invalid message format: %s", data)
		return
	}

	msgType := parts[0]
	clusterID := strings.ToLower(parts[1])
	phase := parts[2]
	viewNumber, _ := strconv.Atoi(parts[3])
	digest := parts[4]
	address := strings.ToLower(parts[5])
	signature := parts[6]
	timestamp, _ := strconv.ParseInt(parts[7], 10, 64)

	msg := &PBFTMessage{
		Type:       msgType,
		ClusterID:  clusterID,
		Phase:      phase,
		ViewNumber: viewNumber,
		Digest:     digest,
		Address:    address,
		Signature:  signature,
		Timestamp:  timestamp,
	}

	// Verify signature
	payload := fmt.Sprintf("%s|%s|%s|%d|%s", msgType, clusterID, phase, viewNumber, digest)
	if !verifyPayloadSignature(payload, signature, address) {
		log.Printf("[PBFT] Invalid signature from %s for %s", address, msgType)
		return
	}

	// Route to appropriate handler
	switch msgType {
	case MsgPrePrepare:
		pm.handlePrePrepare(msg)
	case MsgPrepare:
		pm.handlePrepare(msg)
	case MsgCommit:
		pm.handleCommit(msg)
	case MsgViewChange:
		pm.handleViewChange(msg)
	default:
		log.Printf("[PBFT] Unknown message type: %s", msgType)
	}

	// Send ACK
	s.Write([]byte("ACK"))
}

// getOrCreateConsensus gets or creates a consensus state
func (pm *PBFTManager) getOrCreateConsensus(clusterID, phase string, members []string) *PBFTConsensusState {
	key := pbftConsensusKey(clusterID, phase)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if cs, exists := pm.consensus[key]; exists {
		return cs
	}

	canonical := pbftCanonicalizeMembers(members)
	cs := &PBFTConsensusState{
		ClusterID:   strings.ToLower(clusterID),
		Phase:       phase,
		ViewNumber:  0,
		Prepares:    make(map[string]*PBFTMessage),
		Commits:     make(map[string]*PBFTMessage),
		ViewChanges: make(map[string]int),
		Members:     canonical,
		Coordinator: getPBFTCoordinator(canonical, 0, clusterID),
		StartTime:   time.Now(),
	}
	pm.consensus[key] = cs
	return cs
}

// getConsensus retrieves existing consensus state
func (pm *PBFTManager) getConsensus(clusterID, phase string) *PBFTConsensusState {
	key := pbftConsensusKey(clusterID, phase)
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.consensus[key]
}

// bufferMessage stores a message for later replay when consensus starts
func (pm *PBFTManager) bufferMessage(msg *PBFTMessage) {
	key := pbftConsensusKey(msg.ClusterID, msg.Phase)
	pm.mu.Lock()
	defer pm.mu.Unlock()
	// Limit buffer size per phase to prevent memory issues
	if len(pm.pendingMsgs[key]) < 100 {
		pm.pendingMsgs[key] = append(pm.pendingMsgs[key], msg)
	}
}

// replayBufferedMessages processes any messages received before consensus started
func (pm *PBFTManager) replayBufferedMessages(clusterID, phase string) {
	key := pbftConsensusKey(clusterID, phase)
	pm.mu.Lock()
	msgs := pm.pendingMsgs[key]
	delete(pm.pendingMsgs, key)
	pm.mu.Unlock()

	if len(msgs) == 0 {
		return
	}

	log.Printf("[PBFT] Replaying %d buffered messages for %s:%s", len(msgs), clusterID[:8], phase)
	for _, msg := range msgs {
		switch msg.Type {
		case MsgPrePrepare:
			pm.handlePrePrepare(msg)
		case MsgPrepare:
			pm.handlePrepare(msg)
		case MsgCommit:
			pm.handleCommit(msg)
		case MsgViewChange:
			pm.handleViewChange(msg)
		}
	}
}

// handlePrePrepare processes PRE-PREPARE from coordinator
func (pm *PBFTManager) handlePrePrepare(msg *PBFTMessage) {
	cs := pm.getConsensus(msg.ClusterID, msg.Phase)
	if cs == nil {
		// Buffer the message for when consensus starts
		pm.bufferMessage(msg)
		log.Printf("[PBFT] Buffered PRE-PREPARE for %s:%s (consensus not started yet)", msg.ClusterID[:8], msg.Phase)
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Verify sender is coordinator for this view
	expectedCoord := getPBFTCoordinator(cs.Members, msg.ViewNumber, cs.ClusterID)
	if msg.Address != expectedCoord {
		log.Printf("[PBFT] PRE-PREPARE from non-coordinator %s (expected %s)", msg.Address, expectedCoord)
		return
	}

	// Verify view number matches
	if msg.ViewNumber != cs.ViewNumber {
		log.Printf("[PBFT] PRE-PREPARE view mismatch: got %d, expected %d", msg.ViewNumber, cs.ViewNumber)
		return
	}

	// Verify digest matches local expectation (if any)
	if cs.Digest != "" && msg.Digest != cs.Digest {
		log.Printf("[PBFT] PRE-PREPARE digest mismatch for %s phase=%s: expected %s got %s",
			msg.ClusterID[:8], msg.Phase, cs.Digest[:16], msg.Digest[:16])
		return
	}
	if cs.Digest == "" {
		cs.Digest = msg.Digest
	}
	cs.PrePrepareReceived = true

	log.Printf("[PBFT] Accepted PRE-PREPARE for %s phase=%s view=%d digest=%s",
		msg.ClusterID[:8], msg.Phase, msg.ViewNumber, msg.Digest[:16])

	// Broadcast our PREPARE
	go pm.broadcastPrepare(cs)
}

// handlePrepare processes PREPARE messages
func (pm *PBFTManager) handlePrepare(msg *PBFTMessage) {
	cs := pm.getConsensus(msg.ClusterID, msg.Phase)
	if cs == nil {
		// Buffer the message for when consensus starts
		pm.bufferMessage(msg)
		log.Printf("[PBFT] Buffered PREPARE from %s for %s:%s", msg.Address[:8], msg.ClusterID[:8], msg.Phase)
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Verify sender is a member
	isMember := false
	for _, m := range cs.Members {
		if m == msg.Address {
			isMember = true
			break
		}
	}
	if !isMember {
		log.Printf("[PBFT] PREPARE from non-member %s", msg.Address)
		return
	}

	// Verify view number and digest match
	if msg.ViewNumber != cs.ViewNumber {
		log.Printf("[PBFT] PREPARE view mismatch from %s", msg.Address)
		return
	}
	if cs.Digest != "" && msg.Digest != cs.Digest {
		log.Printf("[PBFT] PREPARE digest mismatch from %s", msg.Address)
		return
	}

	// Store PREPARE
	cs.Prepares[msg.Address] = msg
	log.Printf("[PBFT] Received PREPARE from %s (%d/%d) phase=%s",
		msg.Address[:8], len(cs.Prepares), len(cs.Members), msg.Phase)

	// Check if we have reached PREPARE quorum
	quorum := pbftQuorum(len(cs.Members))
	if len(cs.Prepares) >= quorum {
		log.Printf("[PBFT] PREPARE quorum reached (%d/%d) for %s, broadcasting COMMIT", len(cs.Prepares), quorum, msg.Phase)
		go pm.broadcastCommit(cs)
	}
}

// handleCommit processes COMMIT messages
func (pm *PBFTManager) handleCommit(msg *PBFTMessage) {
	cs := pm.getConsensus(msg.ClusterID, msg.Phase)
	if cs == nil {
		// Buffer the message for when consensus starts
		pm.bufferMessage(msg)
		log.Printf("[PBFT] Buffered COMMIT from %s for %s:%s", msg.Address[:8], msg.ClusterID[:8], msg.Phase)
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Verify sender is a member
	isMember := false
	for _, m := range cs.Members {
		if m == msg.Address {
			isMember = true
			break
		}
	}
	if !isMember {
		log.Printf("[PBFT] COMMIT from non-member %s", msg.Address)
		return
	}

	// Verify view number and digest match
	if msg.ViewNumber != cs.ViewNumber {
		log.Printf("[PBFT] COMMIT view mismatch from %s", msg.Address)
		return
	}
	if cs.Digest != "" && msg.Digest != cs.Digest {
		log.Printf("[PBFT] COMMIT digest mismatch from %s", msg.Address)
		return
	}

	// Store COMMIT
	cs.Commits[msg.Address] = msg
	log.Printf("[PBFT] Received COMMIT from %s (%d/%d) phase=%s",
		msg.Address[:8], len(cs.Commits), len(cs.Members), msg.Phase)

	// Check if we have COMMIT quorum
	quorum := pbftQuorum(len(cs.Members))
	if len(cs.Commits) >= quorum && !cs.Completed {
		cs.Completed = true
		log.Printf("[PBFT] CONSENSUS REACHED for %s phase=%s with %d/%d nodes",
			msg.ClusterID[:8], msg.Phase, len(cs.Commits), len(cs.Members))
	}
}

// handleViewChange processes VIEW-CHANGE messages
func (pm *PBFTManager) handleViewChange(msg *PBFTMessage) {
	cs := pm.getConsensus(msg.ClusterID, msg.Phase)
	if cs == nil {
		log.Printf("[PBFT] No consensus state for VIEW-CHANGE %s:%s", msg.ClusterID, msg.Phase)
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Parse requested new view from digest field
	newView, err := strconv.Atoi(msg.Digest)
	if err != nil {
		log.Printf("[PBFT] Invalid VIEW-CHANGE view number: %s", msg.Digest)
		return
	}

	// Only accept view changes for higher view numbers
	if newView <= cs.ViewNumber {
		return
	}

	cs.ViewChanges[msg.Address] = newView
	log.Printf("[PBFT] VIEW-CHANGE from %s requesting view %d (%d/%d)",
		msg.Address[:8], newView, len(cs.ViewChanges), len(cs.Members))

	// Count votes for each view number
	viewVotes := make(map[int]int)
	for _, v := range cs.ViewChanges {
		viewVotes[v]++
	}

	// Need quorum for view change (2f+1)
	quorum := pbftQuorum(len(cs.Members))
	for view, count := range viewVotes {
		if count >= quorum {
			log.Printf("[PBFT] VIEW CHANGE to view %d approved by %d/%d members", view, count, len(cs.Members))
			cs.ViewNumber = view
			cs.Coordinator = getPBFTCoordinator(cs.Members, view, cs.ClusterID)
			cs.Prepares = make(map[string]*PBFTMessage)
			cs.Commits = make(map[string]*PBFTMessage)
			cs.ViewChanges = make(map[string]int)
			cs.PrePrepareReceived = false

			// New coordinator broadcasts PRE-PREPARE
			if strings.ToLower(pm.host.ethAddress) == cs.Coordinator {
				go pm.broadcastPrePrepare(cs)
			}
			break
		}
	}
}

// broadcastPrePrepare sends PRE-PREPARE to all cluster members
func (pm *PBFTManager) broadcastPrePrepare(cs *PBFTConsensusState) {
	cs.mu.RLock()
	clusterID := cs.ClusterID
	phase := cs.Phase
	viewNumber := cs.ViewNumber
	digest := cs.Digest
	members := cs.Members
	cs.mu.RUnlock()

	myAddr := strings.ToLower(pm.host.ethAddress)
	sig, err := pm.host.signPayload(fmt.Sprintf("%s|%s|%s|%d|%s", MsgPrePrepare, clusterID, phase, viewNumber, digest))
	if err != nil {
		log.Printf("[PBFT] Failed to sign PRE-PREPARE: %v", err)
		return
	}
	timestamp := time.Now().UnixMilli()

	// Locally handle our own PRE-PREPARE so the coordinator also broadcasts PREPARE.
	// This mirrors what happens on other nodes when they receive the PRE-PREPARE
	// over the network.
	localMsg := &PBFTMessage{
		Type:       MsgPrePrepare,
		ClusterID:  clusterID,
		Phase:      phase,
		ViewNumber: viewNumber,
		Digest:     digest,
		Address:    myAddr,
		Signature:  sig,
		Timestamp:  timestamp,
	}
	pm.handlePrePrepare(localMsg)

	msg := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%d\n",
		MsgPrePrepare, clusterID, phase, viewNumber, digest, myAddr, sig, timestamp)

	log.Printf("[PBFT] Broadcasting PRE-PREPARE for %s phase=%s view=%d", clusterID[:8], phase, viewNumber)
	pm.broadcastToMembers(clusterID, members, msg)
}

// broadcastPrepare sends PREPARE to all cluster members
func (pm *PBFTManager) broadcastPrepare(cs *PBFTConsensusState) {
	cs.mu.RLock()
	clusterID := cs.ClusterID
	phase := cs.Phase
	viewNumber := cs.ViewNumber
	digest := cs.Digest
	members := cs.Members
	cs.mu.RUnlock()

	myAddr := strings.ToLower(pm.host.ethAddress)
	sig, err := pm.host.signPayload(fmt.Sprintf("%s|%s|%s|%d|%s", MsgPrepare, clusterID, phase, viewNumber, digest))
	if err != nil {
		log.Printf("[PBFT] Failed to sign PREPARE: %v", err)
		return
	}
	timestamp := time.Now().UnixMilli()

	msg := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%d\n",
		MsgPrepare, clusterID, phase, viewNumber, digest, myAddr, sig, timestamp)

	// Also store our own PREPARE
	cs.mu.Lock()
	cs.Prepares[myAddr] = &PBFTMessage{
		Type:       MsgPrepare,
		ClusterID:  clusterID,
		Phase:      phase,
		ViewNumber: viewNumber,
		Digest:     digest,
		Address:    myAddr,
		Signature:  sig,
		Timestamp:  timestamp,
	}
	prepareCount := len(cs.Prepares)
	memberCount := len(cs.Members)
	cs.mu.Unlock()

	log.Printf("[PBFT] Broadcasting PREPARE for %s phase=%s (%d/%d)", clusterID[:8], phase, prepareCount, memberCount)
	pm.broadcastToMembers(clusterID, members, msg)

	// Check if we now have PREPARE quorum (after adding our own)
	cs.mu.Lock()
	quorum := pbftQuorum(len(cs.Members))
	if len(cs.Prepares) >= quorum {
		cs.mu.Unlock()
		log.Printf("[PBFT] PREPARE quorum reached (%d/%d), broadcasting COMMIT", len(cs.Prepares), quorum)
		pm.broadcastCommit(cs)
	} else {
		cs.mu.Unlock()
	}
}

// broadcastCommit sends COMMIT to all cluster members
func (pm *PBFTManager) broadcastCommit(cs *PBFTConsensusState) {
	cs.mu.RLock()
	clusterID := cs.ClusterID
	phase := cs.Phase
	viewNumber := cs.ViewNumber
	digest := cs.Digest
	members := cs.Members
	cs.mu.RUnlock()

	myAddr := strings.ToLower(pm.host.ethAddress)
	sig, err := pm.host.signPayload(fmt.Sprintf("%s|%s|%s|%d|%s", MsgCommit, clusterID, phase, viewNumber, digest))
	if err != nil {
		log.Printf("[PBFT] Failed to sign COMMIT: %v", err)
		return
	}
	timestamp := time.Now().UnixMilli()

	msg := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%d\n",
		MsgCommit, clusterID, phase, viewNumber, digest, myAddr, sig, timestamp)

	// Also store our own COMMIT
	cs.mu.Lock()
	cs.Commits[myAddr] = &PBFTMessage{
		Type:       MsgCommit,
		ClusterID:  clusterID,
		Phase:      phase,
		ViewNumber: viewNumber,
		Digest:     digest,
		Address:    myAddr,
		Signature:  sig,
		Timestamp:  timestamp,
	}
	commitCount := len(cs.Commits)
	memberCount := len(cs.Members)
	cs.mu.Unlock()

	log.Printf("[PBFT] Broadcasting COMMIT for %s phase=%s (%d/%d)", clusterID[:8], phase, commitCount, memberCount)
	pm.broadcastToMembers(clusterID, members, msg)

	// Check if consensus reached after adding our own
	cs.mu.Lock()
	quorum := pbftQuorum(len(cs.Members))
	if len(cs.Commits) >= quorum && !cs.Completed {
		cs.Completed = true
		log.Printf("[PBFT] CONSENSUS REACHED for %s phase=%s with %d/%d nodes",
			clusterID[:8], phase, len(cs.Commits), len(cs.Members))
	}
	cs.mu.Unlock()
}

// broadcastViewChange sends VIEW-CHANGE to all cluster members
func (pm *PBFTManager) broadcastViewChange(cs *PBFTConsensusState, newView int) {
	cs.mu.RLock()
	clusterID := cs.ClusterID
	phase := cs.Phase
	members := cs.Members
	cs.mu.RUnlock()

	myAddr := strings.ToLower(pm.host.ethAddress)
	viewStr := strconv.Itoa(newView)
	sig, err := pm.host.signPayload(fmt.Sprintf("%s|%s|%s|%d|%s", MsgViewChange, clusterID, phase, newView, viewStr))
	if err != nil {
		log.Printf("[PBFT] Failed to sign VIEW-CHANGE: %v", err)
		return
	}
	timestamp := time.Now().UnixMilli()

	msg := fmt.Sprintf("%s|%s|%s|%d|%s|%s|%s|%d\n",
		MsgViewChange, clusterID, phase, newView, viewStr, myAddr, sig, timestamp)

	// Also record our own view change vote
	cs.mu.Lock()
	cs.ViewChanges[myAddr] = newView
	cs.mu.Unlock()

	log.Printf("[PBFT] Broadcasting VIEW-CHANGE to view %d for %s phase=%s", newView, clusterID[:8], phase)
	pm.broadcastToMembers(clusterID, members, msg)
}

// broadcastToMembers sends a message to all cluster members via direct streams
func (pm *PBFTManager) broadcastToMembers(clusterID string, members []string, msg string) {
	pm.host.mu.RLock()
	cs := pm.host.clusters[strings.ToLower(clusterID)]
	pm.host.mu.RUnlock()

	if cs == nil {
		log.Printf("[PBFT] No cluster state for %s", clusterID)
		return
	}

	myAddr := strings.ToLower(pm.host.ethAddress)

	var wg sync.WaitGroup
	for _, member := range members {
		if member == myAddr {
			continue
		}

		cs.mu.RLock()
		peerIDStr, ok := cs.PeerBindings[member]
		cs.mu.RUnlock()

		if !ok || peerIDStr == "" {
			log.Printf("[PBFT] No peer binding for %s", member[:8])
			continue
		}

		peerID, err := peer.Decode(peerIDStr)
		if err != nil {
			log.Printf("[PBFT] Invalid peer ID for %s: %v", member[:8], err)
			continue
		}

		wg.Add(1)
		go func(pid peer.ID, addr string) {
			defer wg.Done()
			pm.sendToPeer(pid, msg)
		}(peerID, member)
	}
	wg.Wait()
}

// sendToPeer sends a PBFT message to a specific peer
func (pm *PBFTManager) sendToPeer(peerID peer.ID, msg string) error {
	ctx := pm.host.ctx

	s, err := pm.host.host.NewStream(ctx, peerID, ProtocolPBFT)
	if err != nil {
		log.Printf("[PBFT] Failed to open stream to %s: %v", peerID.String()[:8], err)
		return err
	}
	defer s.Close()

	s.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = s.Write([]byte(msg))
	if err != nil {
		log.Printf("[PBFT] Failed to write to %s: %v", peerID.String()[:8], err)
		return err
	}

	// Wait for ACK
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	ack := make([]byte, 3)
	_, err = s.Read(ack)
	if err != nil {
		log.Printf("[PBFT] No ACK from %s: %v", peerID.String()[:8], err)
		return err
	}

	return nil
}

// RunConsensus initiates and waits for PBFT consensus on a phase
// This is the main entry point called from RPC
func (pm *PBFTManager) RunConsensus(clusterID, phase, data string, members []string) (*ConsensusResult, error) {
	clusterID = strings.ToLower(clusterID)
	canonical := pbftCanonicalizeMembers(members)
	myAddr := strings.ToLower(pm.host.ethAddress)

	// Compute digest of the phase data
	digest := computePBFTDigest(clusterID, phase, data)

	// Create consensus state
	cs := pm.getOrCreateConsensus(clusterID, phase, canonical)
	cs.mu.Lock()
	cs.Digest = digest
	isCoordinator := cs.Coordinator == myAddr
	cs.mu.Unlock()

	log.Printf("[PBFT] Starting consensus for %s phase=%s coordinator=%v members=%d digest=%s",
		clusterID[:8], phase, isCoordinator, len(canonical), digest[:16])

	// Replay any messages received before we started consensus
	pm.replayBufferedMessages(clusterID, phase)

	// If we're coordinator, send PRE-PREPARE
	if isCoordinator {
		pm.broadcastPrePrepare(cs)
	}

	// Start timeout monitor for view change
	viewChangeTimer := time.NewTimer(PrePrepareTimeout)
	defer viewChangeTimer.Stop()

	// Wait for consensus or timeout
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	prepareDeadline := time.Now().Add(PrepareTimeout)
	commitDeadline := time.Now().Add(PrepareTimeout + CommitTimeout)

	for {
		select {
		case <-viewChangeTimer.C:
			// Check if we have PRE-PREPARE yet for the current view
			cs.mu.RLock()
			prePrepareReceived := cs.PrePrepareReceived
			hasPrepares := len(cs.Prepares) > 0
			currentView := cs.ViewNumber
			cs.mu.RUnlock()

			if !prePrepareReceived && !hasPrepares {
				// No PRE-PREPARE received, initiate view change
				log.Printf("[PBFT] PRE-PREPARE timeout, initiating view change to %d", currentView+1)
				pm.broadcastViewChange(cs, currentView+1)
				viewChangeTimer.Reset(PrePrepareTimeout)
			}

		case <-ticker.C:
			cs.mu.RLock()
			completed := cs.Completed
			aborted := cs.Aborted
			abortReason := cs.AbortReason
			prepareCount := len(cs.Prepares)
			commitCount := len(cs.Commits)
			memberCount := len(cs.Members)
			cs.mu.RUnlock()
			quorum := pbftQuorum(memberCount)

			if completed {
				// Collect participants
				cs.mu.RLock()
				participants := make([]string, 0, len(cs.Commits))
				for addr := range cs.Commits {
					participants = append(participants, addr)
				}
				cs.mu.RUnlock()
				sort.Strings(participants)

				log.Printf("[PBFT] Consensus complete for %s phase=%s", clusterID[:8], phase)
				pm.cleanupConsensus(clusterID, phase)

				return &ConsensusResult{
					Success:      true,
					Phase:        phase,
					ViewNumber:   cs.ViewNumber,
					Digest:       digest,
					Participants: participants,
				}, nil
			}

			if aborted {
				pm.cleanupConsensus(clusterID, phase)
				return &ConsensusResult{
					Success:     false,
					Phase:       phase,
					Aborted:     true,
					AbortReason: abortReason,
				}, fmt.Errorf("consensus aborted: %s", abortReason)
			}

			// Check timeouts
			now := time.Now()
			if now.After(prepareDeadline) && prepareCount < quorum {
				// PREPARE timeout - quorum not reached
				cs.mu.RLock()
				missing := pm.getMissingMembers(cs, cs.Prepares)
				cs.mu.RUnlock()

				log.Printf("[PBFT] PREPARE timeout for %s phase=%s (%d/%d) missing=%v",
					clusterID[:8], phase, prepareCount, memberCount, missing)
				pm.cleanupConsensus(clusterID, phase)

				return &ConsensusResult{
					Success:     false,
					Phase:       phase,
					Aborted:     true,
					AbortReason: "prepare timeout - quorum not reached",
					Missing:     missing,
				}, fmt.Errorf("prepare timeout: missing %d nodes", len(missing))
			}

			if now.After(commitDeadline) && commitCount < quorum {
				// COMMIT timeout - quorum not reached
				cs.mu.RLock()
				missing := pm.getMissingMembers(cs, cs.Commits)
				cs.mu.RUnlock()

				log.Printf("[PBFT] COMMIT timeout for %s phase=%s (%d/%d) missing=%v",
					clusterID[:8], phase, commitCount, memberCount, missing)
				pm.cleanupConsensus(clusterID, phase)

				return &ConsensusResult{
					Success:     false,
					Phase:       phase,
					Aborted:     true,
					AbortReason: "commit timeout - quorum not reached",
					Missing:     missing,
				}, fmt.Errorf("commit timeout: missing %d nodes", len(missing))
			}
		}
	}
}

// getMissingMembers returns members who haven't sent messages
func (pm *PBFTManager) getMissingMembers(cs *PBFTConsensusState, received map[string]*PBFTMessage) []string {
	missing := make([]string, 0)
	for _, member := range cs.Members {
		if _, ok := received[member]; !ok {
			missing = append(missing, member)
		}
	}
	return missing
}

// cleanupConsensus removes consensus state for a completed phase
func (pm *PBFTManager) cleanupConsensus(clusterID, phase string) {
	key := pbftConsensusKey(clusterID, phase)
	pm.mu.Lock()
	delete(pm.consensus, key)
	pm.mu.Unlock()
}

// AbortConsensus allows external abort of consensus
func (pm *PBFTManager) AbortConsensus(clusterID, phase, reason string) {
	cs := pm.getConsensus(clusterID, phase)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	cs.Aborted = true
	cs.AbortReason = reason
	cs.mu.Unlock()

	log.Printf("[PBFT] Consensus aborted for %s phase=%s: %s", clusterID[:8], phase, reason)
}

// PBFTConsensusDebugState is a snapshot of a consensus state for debugging/testing.
type PBFTConsensusDebugState struct {
	ClusterID          string         `json:"clusterId"`
	Phase              string         `json:"phase"`
	ViewNumber         int            `json:"viewNumber"`
	Digest             string         `json:"digest"`
	PrePrepareReceived bool           `json:"prePrepareReceived"`
	Members            []string       `json:"members"`
	Coordinator        string         `json:"coordinator"`
	StartTime          time.Time      `json:"startTime"`
	Completed          bool           `json:"completed"`
	Aborted            bool           `json:"aborted"`
	AbortReason        string         `json:"abortReason"`
	PrepareSenders     []string       `json:"prepareSenders"`
	CommitSenders      []string       `json:"commitSenders"`
	ViewChanges        map[string]int `json:"viewChanges"`
}

// GetConsensusDebugState returns a snapshot of the consensus state for a cluster/phase.
// Returns nil if there is no active consensus for the given identifiers.
func (pm *PBFTManager) GetConsensusDebugState(clusterID, phase string) *PBFTConsensusDebugState {
	key := pbftConsensusKey(clusterID, phase)

	pm.mu.RLock()
	cs, ok := pm.consensus[key]
	pm.mu.RUnlock()
	if !ok || cs == nil {
		return nil
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	debugState := &PBFTConsensusDebugState{
		ClusterID:          cs.ClusterID,
		Phase:              cs.Phase,
		ViewNumber:         cs.ViewNumber,
		Digest:             cs.Digest,
		PrePrepareReceived: cs.PrePrepareReceived,
		Members:            append([]string(nil), cs.Members...),
		Coordinator:        cs.Coordinator,
		StartTime:          cs.StartTime,
		Completed:          cs.Completed,
		Aborted:            cs.Aborted,
		AbortReason:        cs.AbortReason,
		ViewChanges:        make(map[string]int, len(cs.ViewChanges)),
	}

	for addr, view := range cs.ViewChanges {
		debugState.ViewChanges[addr] = view
	}

	for addr := range cs.Prepares {
		debugState.PrepareSenders = append(debugState.PrepareSenders, addr)
	}
	sort.Strings(debugState.PrepareSenders)

	for addr := range cs.Commits {
		debugState.CommitSenders = append(debugState.CommitSenders, addr)
	}
	sort.Strings(debugState.CommitSenders)

	return debugState
}
