package p2p

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

// Protocol IDs for direct streams
const (
	ProtocolRoundData = "/znode/round/1.0.0"
	ProtocolSignature = "/znode/sig/1.0.0"
	ProtocolIdentity  = "/znode/identity/1.0.0"
	ProtocolNudge     = "/znode/nudge/1.0.0"
)

// HostConfig holds configuration for creating a libp2p host
type HostConfig struct {
	PrivateKey     string   // Ethereum private key (hex)
	EthAddress     string   // Ethereum address
	ListenAddr     string   // libp2p listen address
	BootstrapAddrs []string // Bootstrap peer multiaddrs
}

// PeerInfo represents a cluster peer
type PeerInfo struct {
	Address string // Ethereum address
	PeerID  string // libp2p peer ID (may be empty initially)
}

// ClusterState holds state for a cluster we're participating in
type ClusterState struct {
	ID            string
	Members       []PeerInfo
	IsCoordinator bool
	RoundData     map[int]map[string]string  // round -> address -> payload
	Signatures    map[int]map[string]SigData // round -> address -> signature
	Identities    map[string]IdentityData    // address -> identity
	PeerBindings  map[string]string          // address -> peerID
	PeerScores    map[string]float64         // address -> score
	mu            sync.RWMutex
}

// SigData holds signature information
type SigData struct {
	DataHash  string
	Signature string
	Timestamp int64
}

// IdentityData holds peer identity information
type IdentityData struct {
	PeerID    string
	PublicKey string
	Timestamp int64
}

// Host wraps a libp2p host with cluster coordination capabilities
type Host struct {
	presel     *PreSelectionManager
	host       host.Host
	ctx        context.Context
	ethAddress string
	pbft       *PBFTManager
	ethPrivKey *ecdsa.PrivateKey
	clusters   map[string]*ClusterState
	heartbeat  *HeartbeatManager
	discovery  *DiscoveryManager
	peerStore  *PersistentPeerStore
	mu         sync.RWMutex
}

// NewHost creates a new libp2p host
func NewHost(ctx context.Context, config *HostConfig) (*Host, error) {
	// Parse Ethereum private key
	keyBytes, err := hex.DecodeString(strings.TrimPrefix(config.PrivateKey, "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	ethPrivKey, err := ethcrypto.ToECDSA(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	// Convert Ethereum key to libp2p key
	libp2pPrivKey, err := crypto.UnmarshalSecp256k1PrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to convert key: %w", err)
	}

	// Parse listen address
	listenAddr, err := ma.NewMultiaddr(config.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen address: %w", err)
	}

	// Create libp2p host
	h, err := libp2p.New(
		libp2p.Identity(libp2pPrivKey),
		libp2p.ListenAddrs(listenAddr),
		libp2p.EnableRelay(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	host := &Host{
		host:       h,
		ctx:        ctx,
		ethAddress: strings.ToLower(config.EthAddress),
		ethPrivKey: ethPrivKey,
		clusters:   make(map[string]*ClusterState),
	}

	// Initialize persistent peer store (optional) and merge any stored peers into bootstrap list
	host.peerStore = NewPersistentPeerStoreFromEnv()
	if host.peerStore != nil {
		if stored := host.peerStore.LoadBootstrapMultiaddrs(); len(stored) > 0 {
			// Merge stored addresses into the configured bootstrap list, avoiding duplicates.
			existing := make(map[string]struct{}, len(config.BootstrapAddrs)+len(stored))
			for _, a := range config.BootstrapAddrs {
				if a == "" {
					continue
				}
				existing[a] = struct{}{}
			}
			for _, a := range stored {
				if a == "" {
					continue
				}
				if _, ok := existing[a]; ok {
					continue
				}
				config.BootstrapAddrs = append(config.BootstrapAddrs, a)
				existing[a] = struct{}{}
			}
			log.Printf("[Peers] Loaded %d persisted peers for bootstrap", len(stored))
		}
	}

	// Set up stream handlers

	// Initialize heartbeat manager
	host.heartbeat = NewHeartbeatManager(host)
	h.SetStreamHandler(protocol.ID(ProtocolHeartbeat), host.heartbeat.HandleHeartbeatStream)
	h.SetStreamHandler(protocol.ID(ProtocolRoundData), host.handleRoundDataStream)
	h.SetStreamHandler(protocol.ID(ProtocolSignature), host.handleSignatureStream)
	h.SetStreamHandler(protocol.ID(ProtocolIdentity), host.handleIdentityStream)
	h.SetStreamHandler(protocol.ID(ProtocolNudge), host.handleNudgeStream)
	// Initialize PBFT manager
	host.pbft = NewPBFTManager(host)
	host.pbft.SetupHandler()

	host.presel = NewPreSelectionManager(host)
	host.presel.SetupHandler()
	host.presel.StartCleanup(ctx, 5*time.Minute, 10*time.Minute)

	// Connect to bootstrap peers
	var bootstrapPeers []peer.AddrInfo
	for _, addrStr := range config.BootstrapAddrs {
		if addrStr == "" {
			continue
		}
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			log.Printf("Invalid bootstrap addr %s: %v", addrStr, err)
			continue
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Printf("Invalid bootstrap peer info %s: %v", addrStr, err)
			continue
		}
		go func(pi peer.AddrInfo) {
			if err := h.Connect(ctx, pi); err != nil {
				log.Printf("Failed to connect to bootstrap peer %s: %v", pi.ID, err)
			} else {
				log.Printf("Connected to bootstrap peer %s", pi.ID)
			}
		}(*peerInfo)
		bootstrapPeers = append(bootstrapPeers, *peerInfo)
	}

	// Initialize DHT-based peer discovery
	discovery, err := NewDiscoveryManager(ctx, h, bootstrapPeers)
	if err != nil {
		log.Printf("Warning: Failed to initialize DHT discovery: %v", err)
	} else {
		host.discovery = discovery
		// When we successfully discover and connect to a peer, remember it on disk for future bootstraps.
		if host.peerStore != nil {
			discovery.SetPeerFoundCallback(func(info peer.AddrInfo) {
				host.peerStore.RememberPeer(info)
			})
		}
		discovery.Start()
		log.Printf("DHT peer discovery started")
	}

	return host, nil
}

// PeerID returns the host's peer ID as a string
func (h *Host) PeerID() string {
	return h.host.ID().String()
}

// Addrs returns the host's listen addresses
func (h *Host) Addrs() []ma.Multiaddr {
	return h.host.Addrs()
}

// ConnectedPeerCount returns the number of connected peers
func (h *Host) ConnectedPeerCount() int {
	return len(h.host.Network().Peers())
}

// ActiveClusters returns the list of active cluster IDs
func (h *Host) ActiveClusters() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	clusters := make([]string, 0, len(h.clusters))
	for id := range h.clusters {
		clusters = append(clusters, id)
	}
	return clusters
}

// Close closes the host
func (h *Host) Close() error {
	return h.host.Close()
}

// JoinCluster joins a cluster (preserves any pre-received identities)
func (h *Host) JoinCluster(clusterID string, members []PeerInfo, isCoordinator bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Normalize cluster ID to lowercase for internal storage
	clusterID = strings.ToLower(clusterID)

	// Check if we already have a cluster state (from early identity messages)
	existingCS := h.clusters[clusterID]

	// Create new cluster state
	cs := &ClusterState{
		ID:            clusterID,
		Members:       members,
		IsCoordinator: isCoordinator,
		RoundData:     make(map[int]map[string]string),
		Signatures:    make(map[int]map[string]SigData),
		Identities:    make(map[string]IdentityData),
		PeerBindings:  make(map[string]string),
		PeerScores:    make(map[string]float64),
	}

	// Initialize peer scores
	for _, m := range members {
		cs.PeerScores[strings.ToLower(m.Address)] = 100.0
		if m.PeerID != "" {
			cs.PeerBindings[strings.ToLower(m.Address)] = m.PeerID
		}
	}

	// Merge pre-received identities from existing cluster state
	if existingCS != nil {
		existingCS.mu.RLock()
		preReceivedCount := 0
		for addr, identity := range existingCS.Identities {
			cs.Identities[addr] = identity
			if identity.PeerID != "" {
				cs.PeerBindings[addr] = identity.PeerID
			}
			preReceivedCount++
		}
		existingCS.mu.RUnlock()
		if preReceivedCount > 0 {
			log.Printf("[P2P] Merged %d pre-received identities into cluster %s", preReceivedCount, truncateStr(clusterID, 10))
		}
	}

	h.clusters[clusterID] = cs
	log.Printf("[P2P] Joined cluster %s with %d members (coordinator: %v)", truncateStr(clusterID, 10), len(members), isCoordinator)

	return nil
}

// LeaveCluster leaves a cluster
func (h *Host) LeaveCluster(clusterID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	clusterID = strings.ToLower(clusterID)
	delete(h.clusters, clusterID)
	log.Printf("[P2P] Left cluster %s", truncateStr(clusterID, 10))

	return nil
}

// getCluster gets a cluster by ID (must hold read lock)
func (h *Host) getCluster(clusterID string) *ClusterState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.clusters[strings.ToLower(clusterID)]
}

// BroadcastRoundData broadcasts round data to all cluster peers
func (h *Host) BroadcastRoundData(clusterID string, round int, payload string) (int, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	// Store our own data
	cs.mu.Lock()
	if cs.RoundData[round] == nil {
		cs.RoundData[round] = make(map[string]string)
	}
	cs.RoundData[round][h.ethAddress] = payload
	cs.mu.Unlock()

	// Send to all connected peers
	connectedPeers := h.host.Network().Peers()
	log.Printf("[P2P] Broadcasting round %d to %d connected peers", round, len(connectedPeers))

	var wg sync.WaitGroup
	var delivered int
	var deliveredMu sync.Mutex

	for _, peerID := range connectedPeers {
		if peerID == h.host.ID() {
			continue
		}

		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendRoundData(pid.String(), clusterID, round, payload); err != nil {
				log.Printf("[P2P] Failed to send round %d to %s: %v", round, truncateStr(pid.String(), 12), err)
			} else {
				deliveredMu.Lock()
				delivered++
				deliveredMu.Unlock()
			}
		}(peerID)
	}

	wg.Wait()
	log.Printf("[P2P] Round %d broadcast complete: delivered to %d peers", round, delivered)
	return delivered, nil
}

// sendRoundData sends round data to a specific peer
func (h *Host) sendRoundData(peerIDStr string, clusterID string, round int, payload string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolRoundData))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	// Sign the payload to prove ownership of the address
	signature, err := h.signPayload(payload)
	if err != nil {
		return fmt.Errorf("failed to sign payload: %w", err)
	}

	// Send message with signature
	msg := fmt.Sprintf("%s|%d|%s|%s|%s\n", clusterID, round, h.ethAddress, payload, signature)
	if _, err := stream.Write([]byte(msg)); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	// Wait for ACK
	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Read(ack); err != nil {
		return fmt.Errorf("no ACK received: %w", err)
	}

	return nil
}

// handleRoundDataStream handles incoming round data
func (h *Host) handleRoundDataStream(stream network.Stream) {
	defer stream.Close()

	// Read a full line of round data terminated by a newline.
	reader := bufio.NewReader(stream)
	msg, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[P2P] Failed to read round data: %v", err)
		return
	}

	msg = strings.TrimSpace(msg)
	parts := strings.SplitN(msg, "|", 5)
	if len(parts) < 5 {
		log.Printf("[P2P] Invalid round data format (need 5 parts, got %d)", len(parts))
		return
	}

	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)
	address := strings.ToLower(parts[2])
	payload := parts[3]
	signature := parts[4]

	// Verify signature before accepting
	if !verifyPayloadSignature(payload, signature, address) {
		log.Printf("[P2P] Rejected round %d data from %s: invalid signature", round, truncateStr(address, 8))
		return
	}

	// Store the data
	cs := h.getCluster(clusterID)
	if cs != nil {
		cs.mu.Lock()
		if cs.RoundData[round] == nil {
			cs.RoundData[round] = make(map[string]string)
		}
		cs.RoundData[round][address] = payload
		cs.mu.Unlock()
		log.Printf("[P2P] Received verified round %d data from %s", round, truncateStr(address, 8))

		// Update peer binding
		h.updatePeerBinding(cs, address, stream.Conn().RemotePeer().String())
	}

	// Send ACK
	if _, err := stream.Write([]byte("ACK")); err != nil {
		log.Printf("[P2P] Failed to send ACK for round %d data: %v", round, err)
	}
}

// WaitForRoundCompletion waits for all members to submit round data, periodically re-broadcasting our own
func (h *Host) WaitForRoundCompletion(clusterID string, round int, members []string, timeoutMs int) (*RoundCompletionResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	// Get our payload for re-broadcasting
	cs.mu.RLock()
	ourPayload := ""
	if cs.RoundData[round] != nil {
		ourPayload = cs.RoundData[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	quorum := pbftQuorum(len(membersLower))

	log.Printf("[P2P] Waiting for round %d completion from %d members (quorum=%d)", round, len(members), quorum)

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		roundData := cs.RoundData[round]
		var received, missing []string
		for _, addr := range membersLower {
			if roundData != nil && roundData[addr] != "" {
				received = append(received, addr)
			} else {
				missing = append(missing, addr)
			}
		}
		cs.mu.RUnlock()

		if len(received) >= quorum {
			log.Printf("[P2P] Round %d complete: %d/%d received (quorum=%d)", round, len(received), len(membersLower), quorum)
			return &RoundCompletionResult{
				Complete: true,
				Received: received,
				Missing:  missing,
			}, nil
		}

		// Re-broadcast and nudge periodically
		if time.Since(lastRebroadcast) >= rebroadcastInterval && len(missing) > 0 {
			log.Printf("[P2P] Round %d progress: %d/%d (missing: %d). Re-broadcasting... Missing addrs: %v",
				round, len(received), len(membersLower), len(missing), missing)

			// Re-broadcast our data if we have it
			if ourPayload != "" {
				delivered, _ := h.BroadcastRoundData(clusterID, round, ourPayload)
				log.Printf("[P2P] Re-broadcast round %d data to %d peers", round, delivered)
			}

			// Also nudge peers who haven't sent data
			h.nudgeMissingPeers(cs, clusterID, round, missing)
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Timeout - return what we have
	cs.mu.RLock()
	roundData := cs.RoundData[round]
	var received, missing []string
	for _, addr := range membersLower {
		if roundData != nil && roundData[addr] != "" {
			received = append(received, addr)
		} else {
			missing = append(missing, addr)
		}
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Round %d timeout: %d/%d received. Missing: %v", round, len(received), len(membersLower), missing)

	return &RoundCompletionResult{
		Complete: false,
		Received: received,
		Missing:  missing,
	}, fmt.Errorf("timeout waiting for round %d completion", round)
}

// RoundCompletionResult holds the result of waiting for round completion
type RoundCompletionResult struct {
	Complete bool     `json:"complete"`
	Received []string `json:"received"`
	Missing  []string `json:"missing"`
}

// nudgeMissingPeers sends nudge requests to peers who haven't submitted data
func (h *Host) nudgeMissingPeers(cs *ClusterState, clusterID string, round int, missing []string) {
	for _, addr := range missing {
		peerID := h.getPeerID(cs, addr)
		if peerID == "" {
			continue
		}

		go func(pid, address string) {
			if err := h.sendNudge(pid, clusterID, round); err != nil {
				log.Printf("[P2P] Nudge failed for %s: %v", truncateStr(address, 8), err)
			}
		}(peerID, addr)
	}
}

// sendNudge sends a nudge request to a peer
func (h *Host) sendNudge(peerIDStr string, clusterID string, round int) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolNudge))
	if err != nil {
		return err
	}
	defer stream.Close()

	msg := fmt.Sprintf("%s|%d|%s\n", clusterID, round, h.ethAddress)
	stream.Write([]byte(msg))

	return nil
}

// handleNudgeStream handles incoming nudge requests
func (h *Host) handleNudgeStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	msg := string(buf[:n])
	parts := strings.SplitN(strings.TrimSpace(msg), "|", 3)
	if len(parts) < 2 {
		return
	}

	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)

	// If we have the data, send it back to the requester
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.RLock()
	payload := ""
	if cs.RoundData[round] != nil {
		payload = cs.RoundData[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	if payload != "" {
		// Reply directly to the peer who sent the nudge
		remotePeer := stream.Conn().RemotePeer()
		h.sendRoundData(remotePeer.String(), clusterID, round, payload)
	}
}

// GetPeerPayloads returns payloads from peers in canonical order
func (h *Host) GetPeerPayloads(clusterID string, round int, members []string) map[string]string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	roundData := cs.RoundData[round]
	if roundData == nil {
		return make(map[string]string)
	}

	result := make(map[string]string)
	for _, m := range members {
		addr := strings.ToLower(m)
		if addr == h.ethAddress {
			continue // Exclude self
		}
		if payload, ok := roundData[addr]; ok {
			result[addr] = payload
		}
	}

	return result
}

// BroadcastSignature broadcasts a signature to all cluster peers
func (h *Host) BroadcastSignature(clusterID string, round int, dataHash, signature string) (int, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	// Store our own signature
	cs.mu.Lock()
	if cs.Signatures[round] == nil {
		cs.Signatures[round] = make(map[string]SigData)
	}
	cs.Signatures[round][h.ethAddress] = SigData{
		DataHash:  dataHash,
		Signature: signature,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.mu.Unlock()

	// Send to all connected peers
	connectedPeers := h.host.Network().Peers()

	var wg sync.WaitGroup
	var delivered int
	var mu sync.Mutex

	for _, peerID := range connectedPeers {
		if peerID == h.host.ID() {
			continue
		}

		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendSignature(pid.String(), clusterID, round, dataHash, signature); err != nil {
				log.Printf("[P2P] Failed to send signature to %s: %v", truncateStr(pid.String(), 12), err)
			} else {
				mu.Lock()
				delivered++
				mu.Unlock()
			}
		}(peerID)
	}

	wg.Wait()
	return delivered, nil
}

// sendSignature sends a signature to a specific peer
func (h *Host) sendSignature(peerIDStr string, clusterID string, round int, dataHash, signature string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolSignature))
	if err != nil {
		return err
	}
	defer stream.Close()

	msg := fmt.Sprintf("%s|%d|%s|%s|%s|%d\n", clusterID, round, h.ethAddress, dataHash, signature, time.Now().UnixMilli())
	if _, err := stream.Write([]byte(msg)); err != nil {
		return err
	}

	// Wait for ACK
	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	stream.Read(ack)

	return nil
}

// handleSignatureStream handles incoming signatures
func (h *Host) handleSignatureStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 4096)
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	msg := string(buf[:n])
	parts := strings.SplitN(strings.TrimSpace(msg), "|", 6)
	if len(parts) < 5 {
		return
	}

	clusterID := parts[0]
	round := 0
	fmt.Sscanf(parts[1], "%d", &round)
	address := strings.ToLower(parts[2])
	dataHash := parts[3]
	signature := parts[4]

	cs := h.getCluster(clusterID)
	if cs != nil {
		cs.mu.Lock()
		if cs.Signatures[round] == nil {
			cs.Signatures[round] = make(map[string]SigData)
		}
		cs.Signatures[round][address] = SigData{
			DataHash:  dataHash,
			Signature: signature,
			Timestamp: time.Now().UnixMilli(),
		}
		cs.mu.Unlock()
		log.Printf("[P2P] Received signature for round %d from %s", round, truncateStr(address, 8))
	}

	stream.Write([]byte("ACK"))
}

// WaitForSignatureBarrier waits for all members to submit signatures with matching hash
func (h *Host) WaitForSignatureBarrier(clusterID string, round int, members []string, timeoutMs int) (*SignatureBarrierResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	quorum := pbftQuorum(len(membersLower))

	// Try to compute consensus hash from round data (may be empty for ready barriers)
	expectedHash := h.ComputePayloadConsensusHash(clusterID, round, members)

	// Get our signature for re-broadcasting and determining expected hash
	cs.mu.RLock()
	var ourSig SigData
	if cs.Signatures[round] != nil {
		ourSig = cs.Signatures[round][h.ethAddress]
	}
	cs.mu.RUnlock()

	// If no round data hash, use our own signature's hash as the expected hash
	// This handles "ready" barriers where all nodes just sign a simple value
	if expectedHash == "" {
		if ourSig.DataHash != "" {
			expectedHash = ourSig.DataHash
			log.Printf("[P2P] Round %d signature barrier: using broadcast hash %s (no round data)", round, truncateStr(expectedHash, 16))
		} else {
			return nil, fmt.Errorf("cannot determine expected hash - no round data and no signature broadcast")
		}
	} else {
		log.Printf("[P2P] Round %d signature barrier: consensus hash %s", round, truncateStr(expectedHash, 16))
	}

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		sigData := cs.Signatures[round]
		var sigs []SignatureInfo
		var missing []string
		var hashMismatches []string

		for _, addr := range membersLower {
			if sigData != nil {
				if sd, ok := sigData[addr]; ok {
					// Verify the peer's hash matches expected
					if sd.DataHash != expectedHash {
						hashMismatches = append(hashMismatches, addr)
						continue
					}
					sigs = append(sigs, SignatureInfo{
						Address:   addr,
						DataHash:  sd.DataHash,
						Signature: sd.Signature,
					})
					continue
				}
			}
			missing = append(missing, addr)
		}
		cs.mu.RUnlock()

		// Success: quorum signatures collected with matching hashes
		if len(sigs) >= quorum && len(hashMismatches) == 0 {
			log.Printf("[P2P] Round %d signature barrier complete (%d/%d, quorum=%d)", round, len(sigs), len(membersLower), quorum)
			return &SignatureBarrierResult{
				Complete:      true,
				Signatures:    sigs,
				Missing:       nil,
				ConsensusHash: expectedHash,
			}, nil
		}

		// If we have hash mismatches, that's a critical error - fail immediately
		if len(hashMismatches) > 0 {
			log.Printf("[P2P] Round %d hash MISMATCH from %d peers: %v", round, len(hashMismatches), hashMismatches)
			return &SignatureBarrierResult{
				Complete:      false,
				Signatures:    sigs,
				Missing:       missing,
				ConsensusHash: expectedHash,
				Mismatched:    hashMismatches,
			}, fmt.Errorf("hash mismatch from %d peers", len(hashMismatches))
		}

		// Re-broadcast periodically
		if time.Since(lastRebroadcast) >= rebroadcastInterval && len(missing) > 0 {
			log.Printf("[P2P] Round %d signature progress: %d/%d (missing: %d). Re-broadcasting...",
				round, len(sigs), len(membersLower), len(missing))

			// Re-broadcast our signature if we have it
			if ourSig.DataHash != "" {
				delivered, _ := h.BroadcastSignature(clusterID, round, ourSig.DataHash, ourSig.Signature)
				log.Printf("[P2P] Re-broadcast round %d signature to %d peers", round, delivered)
			}
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Timeout - collect final state
	cs.mu.RLock()
	sigData := cs.Signatures[round]
	var sigs []SignatureInfo
	var missing []string
	for _, addr := range membersLower {
		if sigData != nil {
			if sd, ok := sigData[addr]; ok {
				sigs = append(sigs, SignatureInfo{
					Address:   addr,
					DataHash:  sd.DataHash,
					Signature: sd.Signature,
				})
				continue
			}
		}
		missing = append(missing, addr)
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Round %d signature barrier timeout: %d/%d. Missing: %v", round, len(sigs), len(membersLower), missing)

	return &SignatureBarrierResult{
		Complete:      false,
		Signatures:    sigs,
		Missing:       missing,
		ConsensusHash: expectedHash,
	}, fmt.Errorf("timeout waiting for signature barrier")
}

// SignatureBarrierResult holds the result of waiting for signature barrier
type SignatureBarrierResult struct {
	Complete      bool            `json:"complete"`
	Signatures    []SignatureInfo `json:"signatures"`
	Missing       []string        `json:"missing"`
	ConsensusHash string          `json:"consensusHash"`
	Mismatched    []string        `json:"mismatched,omitempty"`
}

// SignatureInfo holds signature information
type SignatureInfo struct {
	Address   string `json:"address"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

// BroadcastIdentity broadcasts our identity to cluster peers
func (h *Host) BroadcastIdentity(clusterID string, publicKey string) (int, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return 0, fmt.Errorf("cluster not found: %s", clusterID)
	}

	// Store our own identity
	cs.mu.Lock()
	cs.Identities[h.ethAddress] = IdentityData{
		PeerID:    h.PeerID(),
		PublicKey: publicKey,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.PeerBindings[h.ethAddress] = h.PeerID()
	cs.mu.Unlock()

	// Send to all connected peers (not just known members, since we may not know their peer IDs yet)
	connectedPeers := h.host.Network().Peers()
	log.Printf("[P2P] Broadcasting identity to %d connected peers for cluster %s", len(connectedPeers), truncateStr(clusterID, 10))

	var wg sync.WaitGroup
	var delivered int
	var mu sync.Mutex

	for _, peerID := range connectedPeers {
		if peerID == h.host.ID() {
			continue
		}

		wg.Add(1)
		go func(pid peer.ID) {
			defer wg.Done()
			if err := h.sendIdentity(pid.String(), clusterID, publicKey); err != nil {
				log.Printf("[P2P] Failed to send identity to %s: %v", truncateStr(pid.String(), 12), err)
			} else {
				mu.Lock()
				delivered++
				mu.Unlock()
			}
		}(peerID)
	}

	wg.Wait()
	log.Printf("[P2P] Identity broadcast complete: delivered to %d peers", delivered)
	return delivered, nil
}

// sendIdentity sends identity to a specific peer
func (h *Host) sendIdentity(peerIDStr string, clusterID string, publicKey string) error {
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()

	stream, err := h.host.NewStream(ctx, peerID, protocol.ID(ProtocolIdentity))
	if err != nil {
		return err
	}
	defer stream.Close()

	// Get member list from cluster state
	cs := h.getCluster(clusterID)
	var membersList string
	if cs != nil {
		cs.mu.RLock()
		addrs := make([]string, 0, len(cs.Members))
		for _, m := range cs.Members {
			addrs = append(addrs, strings.ToLower(m.Address))
		}
		cs.mu.RUnlock()
		sort.Strings(addrs)
		membersList = strings.Join(addrs, ",")
	}

	// Sign identity data to prove ownership (includes member list for verification)
	identityData := fmt.Sprintf("%s|%s|%s|%s", clusterID, h.PeerID(), publicKey, membersList)
	signature, err := h.signPayload(identityData)
	if err != nil {
		return fmt.Errorf("failed to sign identity: %w", err)
	}

	msg := fmt.Sprintf("%s|%s|%s|%s|%s|%s\n", clusterID, h.ethAddress, h.PeerID(), publicKey, membersList, signature)
	if _, err := stream.Write([]byte(msg)); err != nil {
		return err
	}

	// Wait for ACK
	ack := make([]byte, 3)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	stream.Read(ack)

	return nil
}

// handleIdentityStream handles incoming identity messages
func (h *Host) handleIdentityStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 8192) // Larger buffer for member list
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	msg := string(buf[:n])
	parts := strings.SplitN(strings.TrimSpace(msg), "|", 6)
	if len(parts) < 6 {
		// Try old format for backwards compatibility (5 parts)
		if len(parts) == 5 {
			log.Printf("[P2P] Received old identity format without member list from %s - ignoring", truncateStr(parts[1], 8))
		} else {
			log.Printf("[P2P] Invalid identity format (need 6 parts, got %d)", len(parts))
		}
		return
	}

	clusterID := parts[0]
	address := strings.ToLower(parts[1])
	peerIDStr := parts[2]
	publicKey := parts[3]
	membersList := parts[4]
	signature := parts[5]

	// Parse member list
	var members []string
	if membersList != "" {
		members = strings.Split(membersList, ",")
	}

	// Check if we're a member of this cluster
	isMember := false
	for _, m := range members {
		if strings.ToLower(m) == h.ethAddress {
			isMember = true
			break
		}
	}
	if !isMember {
		// Silently ignore - we're not part of this cluster
		return
	}

	// Verify the sender is also a member
	senderIsMember := false
	for _, m := range members {
		if strings.ToLower(m) == address {
			senderIsMember = true
			break
		}
	}
	if !senderIsMember {
		log.Printf("[P2P] Rejected identity from %s: sender not in member list", truncateStr(address, 8))
		return
	}

	// Verify signature (includes member list)
	identityData := fmt.Sprintf("%s|%s|%s|%s", clusterID, peerIDStr, publicKey, membersList)
	if !verifyPayloadSignature(identityData, signature, address) {
		log.Printf("[P2P] Rejected identity from %s: invalid signature", truncateStr(address, 8))
		return
	}

	// Get or create cluster state
	cs := h.getCluster(clusterID)
	if cs == nil {
		// Create cluster state with member list from the identity message
		log.Printf("[P2P] Auto-creating cluster %s from identity (we are a member)", truncateStr(clusterID, 10))
		h.mu.Lock()
		memberInfos := make([]PeerInfo, len(members))
		for i, m := range members {
			memberInfos[i] = PeerInfo{Address: strings.ToLower(m)}
		}
		cs = &ClusterState{
			ID:           clusterID,
			Members:      memberInfos,
			Identities:   make(map[string]IdentityData),
			PeerBindings: make(map[string]string),
			RoundData:    make(map[int]map[string]string),
			Signatures:   make(map[int]map[string]SigData),
			PeerScores:   make(map[string]float64),
		}
		for _, m := range members {
			cs.PeerScores[strings.ToLower(m)] = 100.0
		}
		h.clusters[clusterID] = cs
		h.mu.Unlock()
	}

	cs.mu.Lock()
	cs.Identities[address] = IdentityData{
		PeerID:    peerIDStr,
		PublicKey: publicKey,
		Timestamp: time.Now().UnixMilli(),
	}
	cs.PeerBindings[address] = peerIDStr
	cs.mu.Unlock()
	log.Printf("[P2P] Received verified identity from %s -> %s", truncateStr(address, 8), truncateStr(peerIDStr, 12))

	stream.Write([]byte("ACK"))
}

// WaitForIdentities waits for all member identities, periodically re-broadcasting our own
func (h *Host) WaitForIdentities(clusterID string, members []string, timeoutMs int) (*IdentityResult, error) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	deadline := time.Now().Add(timeout)
	rebroadcastInterval := 10 * time.Second
	lastRebroadcast := time.Now()

	membersLower := make([]string, len(members))
	for i, m := range members {
		membersLower[i] = strings.ToLower(m)
	}

	// Initial broadcast
	h.BroadcastIdentity(clusterID, "")
	log.Printf("[P2P] Identity exchange started for cluster %s with %d members", truncateStr(clusterID, 10), len(members))

	for time.Now().Before(deadline) {
		cs.mu.RLock()
		var identities []IdentityInfo
		var missing []string
		for _, addr := range membersLower {
			if id, ok := cs.Identities[addr]; ok {
				identities = append(identities, IdentityInfo{
					Address:   addr,
					PeerID:    id.PeerID,
					PublicKey: id.PublicKey,
				})
			} else {
				missing = append(missing, addr)
			}
		}
		cs.mu.RUnlock()

		if len(identities) >= len(membersLower) {
			log.Printf("[P2P] Identity exchange complete: %d/%d identities received", len(identities), len(membersLower))
			return &IdentityResult{
				Complete:   true,
				Identities: identities,
				Missing:    nil,
			}, nil
		}

		// Log progress periodically
		if time.Since(lastRebroadcast) >= rebroadcastInterval {
			log.Printf("[P2P] Identity progress: %d/%d (missing: %d). Re-broadcasting...",
				len(identities), len(membersLower), len(missing))
			// Re-broadcast our identity to pick up newly connected peers
			delivered, _ := h.BroadcastIdentity(clusterID, "")
			log.Printf("[P2P] Re-broadcast identity to %d peers", delivered)
			lastRebroadcast = time.Now()
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Timeout - collect final state
	cs.mu.RLock()
	var identities []IdentityInfo
	var missing []string
	for _, addr := range membersLower {
		if id, ok := cs.Identities[addr]; ok {
			identities = append(identities, IdentityInfo{
				Address:   addr,
				PeerID:    id.PeerID,
				PublicKey: id.PublicKey,
			})
		} else {
			missing = append(missing, addr)
		}
	}
	cs.mu.RUnlock()

	log.Printf("[P2P] Identity exchange timeout: %d/%d identities. Missing: %v",
		len(identities), len(membersLower), missing)

	return &IdentityResult{
		Complete:   false,
		Identities: identities,
		Missing:    missing,
	}, fmt.Errorf("timeout waiting for identities")
}

// IdentityResult holds the result of waiting for identities
type IdentityResult struct {
	Complete   bool           `json:"complete"`
	Identities []IdentityInfo `json:"identities"`
	Missing    []string       `json:"missing"`
}

// IdentityInfo holds identity information
type IdentityInfo struct {
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	PublicKey string `json:"publicKey"`
}

// GetPeerBindings returns address -> peerID bindings
func (h *Host) GetPeerBindings(clusterID string) map[string]string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return make(map[string]string)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]string)
	for addr, peerID := range cs.PeerBindings {
		result[addr] = peerID
	}
	return result
}

// ClearPeerBindings clears peer bindings for a cluster
func (h *Host) ClearPeerBindings(clusterID string) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	cs.PeerBindings = make(map[string]string)
	cs.Identities = make(map[string]IdentityData)
	cs.mu.Unlock()

	log.Printf("[P2P] Cleared peer bindings for cluster %s", truncateStr(clusterID, 10))
}

// GetPeerScores returns peer health scores
func (h *Host) GetPeerScores(clusterID string) map[string]float64 {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return make(map[string]float64)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make(map[string]float64)
	for addr, score := range cs.PeerScores {
		result[addr] = score
	}
	return result
}

// ReportPeerFailure reports a peer failure
func (h *Host) ReportPeerFailure(clusterID, address, reason string) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	h.penalizePeer(cs, strings.ToLower(address), 10.0)
	log.Printf("[P2P] Peer %s reported failed: %s", truncateStr(address, 8), reason)
}

// Helper functions

func (h *Host) getPeerID(cs *ClusterState, address string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.PeerBindings[address]
}

func (h *Host) updatePeerBinding(cs *ClusterState, address, peerID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.PeerBindings[address] = peerID
}

func (h *Host) penalizePeer(cs *ClusterState, address string, penalty float64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if score, ok := cs.PeerScores[address]; ok {
		cs.PeerScores[address] = score - penalty
		if cs.PeerScores[address] < 0 {
			cs.PeerScores[address] = 0
		}
	}
}

// GetHeartbeatManager returns the heartbeat manager
func (h *Host) GetHeartbeatManager() *HeartbeatManager {
	return h.heartbeat
}

// signPayload signs a payload with the node's ETH private key
func (h *Host) signPayload(payload string) (string, error) {
	hash := ethcrypto.Keccak256Hash([]byte(payload))
	sig, err := ethcrypto.Sign(hash.Bytes(), h.ethPrivKey)
	if err != nil {
		return "", err
	}
	// Ethereum signature format (v = 27 or 28)
	if sig[64] < 27 {
		sig[64] += 27
	}
	return hex.EncodeToString(sig), nil
}

// verifyPayloadSignature verifies a payload signature matches the claimed address
func verifyPayloadSignature(payload, signature, claimedAddress string) bool {
	sigBytes, err := hex.DecodeString(signature)
	if err != nil || len(sigBytes) != 65 {
		return false
	}

	// Adjust v for recovery
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}

	hash := ethcrypto.Keccak256Hash([]byte(payload))
	pubKey, err := ethcrypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return false
	}

	recoveredAddr := strings.ToLower(ethcrypto.PubkeyToAddress(*pubKey).Hex())
	return recoveredAddr == strings.ToLower(claimedAddress)
}

// ComputePayloadConsensusHash computes a deterministic hash of all payloads for a round
// The hash includes sorted addresses and their payloads to ensure all nodes agree
func (h *Host) ComputePayloadConsensusHash(clusterID string, round int, members []string) string {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return ""
	}

	// Sort members for deterministic ordering
	sortedMembers := make([]string, len(members))
	copy(sortedMembers, members)
	for i := range sortedMembers {
		sortedMembers[i] = strings.ToLower(sortedMembers[i])
	}
	// Simple sort (alphabetical)
	for i := 0; i < len(sortedMembers)-1; i++ {
		for j := i + 1; j < len(sortedMembers); j++ {
			if sortedMembers[i] > sortedMembers[j] {
				sortedMembers[i], sortedMembers[j] = sortedMembers[j], sortedMembers[i]
			}
		}
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	roundData := cs.RoundData[round]
	if roundData == nil {
		return ""
	}

	// Build canonical string: "addr1:payload1|addr2:payload2|..."
	var parts []string
	for _, addr := range sortedMembers {
		if payload, ok := roundData[addr]; ok {
			parts = append(parts, fmt.Sprintf("%s:%s", addr, payload))
		}
	}

	canonical := strings.Join(parts, "|")
	hash := ethcrypto.Keccak256Hash([]byte(canonical))
	return hash.Hex()
}

// RunConsensus runs PBFT consensus for a cluster phase
// This is the main entry point for PBFT consensus from RPC
func (h *Host) RunConsensus(clusterID, phase, data string, members []string) (*ConsensusResult, error) {
	if h.pbft == nil {
		return nil, fmt.Errorf("PBFT manager not initialized")
	}
	return h.pbft.RunConsensus(clusterID, phase, data, members)
}

// GetPBFTManager returns the PBFT manager
func (h *Host) GetPBFTManager() *PBFTManager {
	return h.pbft
}

// getPeerIDForAddress returns the peer ID for an Ethereum address
// Checks cluster bindings first, then heartbeat data
func (h *Host) getPeerIDForAddress(address string) string {
	address = strings.ToLower(address)

	// Check cluster bindings first
	h.mu.RLock()
	for _, cs := range h.clusters {
		cs.mu.RLock()
		if peerID, ok := cs.PeerBindings[address]; ok {
			cs.mu.RUnlock()
			h.mu.RUnlock()
			return peerID
		}
		cs.mu.RUnlock()
	}
	h.mu.RUnlock()

	// Check heartbeat manager for recent peer info
	if h.heartbeat != nil {
		if peerID := h.heartbeat.GetPeerIDForAddress(address); peerID != "" {
			return peerID
		}
	}

	return ""
}

// GetPreSelectionManager returns the pre-selection manager
func (h *Host) GetPreSelectionManager() *PreSelectionManager {
	return h.presel
}

func (h *Host) GetRoundData(clusterID string, round int) map[string]string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cs, ok := h.clusters[clusterID]
	if !ok {
		return nil
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.RoundData[round] == nil {
		return nil
	}
	result := make(map[string]string)
	for k, v := range cs.RoundData[round] {
		result[k] = v
	}
	return result
}
