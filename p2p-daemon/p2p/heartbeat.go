package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	ProtocolHeartbeat = "/znode/heartbeat/1.0.0"
)

// HeartbeatConfig holds EIP-712 domain configuration
type HeartbeatConfig struct {
	ChainID        int64
	StakingAddress string
}

// HeartbeatMessage represents a P2P heartbeat
type HeartbeatMessage struct {
	Type      string `json:"type"`
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
}

// PeerPresence tracks when we last saw a peer
type PeerPresence struct {
	Address   string
	PeerID    string
	LastSeen  time.Time
	Signature string
}

// HeartbeatManager handles heartbeat broadcasting and tracking
type HeartbeatManager struct {
	host           *Host
	config         *HeartbeatConfig
	recentPeers    map[string]*PeerPresence // address -> presence
	mu             sync.RWMutex
	stopChan       chan struct{}
	running        bool
	intervalMs     int
	ttlMs          int
}

// NewHeartbeatManager creates a new heartbeat manager
func NewHeartbeatManager(host *Host) *HeartbeatManager {
	return &HeartbeatManager{
		host:        host,
		recentPeers: make(map[string]*PeerPresence),
		intervalMs:  30000, // 30 seconds default
		ttlMs:       300000, // 5 minutes TTL
	}
}

// SetDomain sets the EIP-712 domain for heartbeat signing
func (hm *HeartbeatManager) SetDomain(chainID int64, stakingAddress string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.config = &HeartbeatConfig{
		ChainID:        chainID,
		StakingAddress: strings.ToLower(stakingAddress),
	}
	log.Printf("[Heartbeat] Domain set: chainId=%d, staking=%s", chainID, truncateStr(stakingAddress, 10))
}

// Start begins periodic heartbeat broadcasting
func (hm *HeartbeatManager) Start(intervalMs int) error {
	hm.mu.Lock()
	if hm.running {
		hm.mu.Unlock()
		return nil
	}
	hm.running = true
	hm.intervalMs = intervalMs
	hm.stopChan = make(chan struct{})
	hm.mu.Unlock()

	go hm.broadcastLoop()
	log.Printf("[Heartbeat] Started broadcasting every %dms", intervalMs)
	return nil
}

// Stop stops heartbeat broadcasting
func (hm *HeartbeatManager) Stop() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	if hm.running {
		close(hm.stopChan)
		hm.running = false
	}
}

// broadcastLoop periodically broadcasts heartbeats
func (hm *HeartbeatManager) broadcastLoop() {
	ticker := time.NewTicker(time.Duration(hm.intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-hm.stopChan:
			return
		case <-ticker.C:
			if err := hm.Broadcast(); err != nil {
				log.Printf("[Heartbeat] Broadcast error: %v", err)
			}
		}
	}
}

// Broadcast sends a heartbeat to all connected peers
func (hm *HeartbeatManager) Broadcast() error {
	hm.mu.RLock()
	config := hm.config
	hm.mu.RUnlock()

	if config == nil {
		return fmt.Errorf("heartbeat domain not configured")
	}

	// Create heartbeat message
	timestamp := time.Now().UnixMilli()
	nonce := fmt.Sprintf("0x%x", timestamp)

	// Sign the heartbeat using EIP-712
	signature, err := hm.signHeartbeat(timestamp, nonce)
	if err != nil {
		return fmt.Errorf("failed to sign heartbeat: %w", err)
	}

	msg := &HeartbeatMessage{
		Type:      "p2p/heartbeat",
		Address:   hm.host.ethAddress,
		PeerID:    hm.host.PeerID(),
		Timestamp: timestamp,
		Nonce:     nonce,
		Signature: signature,
	}

	// Send to all connected peers
	peers := hm.host.host.Network().Peers()
	var sent int
	for _, peerID := range peers {
		if err := hm.sendHeartbeat(peerID, msg); err != nil {
			// Silent fail - peer may be disconnecting
			continue
		}
		sent++
	}

	if sent > 0 {
		log.Printf("[Heartbeat] Broadcast to %d peers", sent)
	}

	// Record our own presence
	hm.recordPresence(hm.host.ethAddress, hm.host.PeerID(), signature)

	// Clean up old entries
	hm.cleanupOld()

	return nil
}

// signHeartbeat creates an EIP-712 signature for the heartbeat
func (hm *HeartbeatManager) signHeartbeat(timestamp int64, nonce string) (string, error) {
	hm.mu.RLock()
	config := hm.config
	hm.mu.RUnlock()

	if config == nil {
		return "", fmt.Errorf("domain not configured")
	}

	// EIP-712 typed data
	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Heartbeat": {
				{Name: "node", Type: "address"},
				{Name: "timestamp", Type: "uint256"},
				{Name: "nonce", Type: "string"},
			},
		},
		PrimaryType: "Heartbeat",
		Domain: apitypes.TypedDataDomain{
			Name:              "ZNodeStaking",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(config.ChainID),
			VerifyingContract: config.StakingAddress,
		},
		Message: apitypes.TypedDataMessage{
			"node":      hm.host.ethAddress,
			"timestamp": math.NewHexOrDecimal256(timestamp),
			"nonce":     nonce,
		},
	}

	// Hash the typed data
	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return "", fmt.Errorf("failed to hash domain: %w", err)
	}

	messageHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return "", fmt.Errorf("failed to hash message: %w", err)
	}

	// Create the final hash
	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(messageHash)))
	hash := crypto.Keccak256Hash(rawData)

	// Sign with private key
	sig, err := crypto.Sign(hash.Bytes(), hm.host.ethPrivKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}

	// Convert to Ethereum signature format (v = 27 or 28)
	if sig[64] < 27 {
		sig[64] += 27
	}

	return fmt.Sprintf("0x%x", sig), nil
}

// sendHeartbeat sends a heartbeat to a specific peer
func (hm *HeartbeatManager) sendHeartbeat(peerID peer.ID, msg *HeartbeatMessage) error {
	ctx, cancel := context.WithTimeout(hm.host.ctx, 5*time.Second)
	defer cancel()

	stream, err := hm.host.host.NewStream(ctx, peerID, protocol.ID(ProtocolHeartbeat))
	if err != nil {
		return err
	}
	defer stream.Close()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	stream.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := stream.Write(append(data, '\n')); err != nil {
		return err
	}

	return nil
}

// HandleHeartbeatStream handles incoming heartbeat messages
func (hm *HeartbeatManager) HandleHeartbeatStream(stream network.Stream) {
	defer stream.Close()

	buf := make([]byte, 4096)
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := stream.Read(buf)
	if err != nil {
		return
	}

	var msg HeartbeatMessage
	if err := json.Unmarshal(buf[:n], &msg); err != nil {
		return
	}

	// Verify the heartbeat signature
	if !hm.verifyHeartbeat(&msg) {
		log.Printf("[Heartbeat] Invalid signature from %s", truncateStr(msg.Address, 8))
		return
	}

	// Record the peer's presence
	hm.recordPresence(msg.Address, msg.PeerID, msg.Signature)

	// Update peer binding in host
	hm.host.mu.Lock()
	for _, cs := range hm.host.clusters {
		cs.mu.Lock()
		if _, exists := cs.PeerBindings[strings.ToLower(msg.Address)]; !exists {
			cs.PeerBindings[strings.ToLower(msg.Address)] = msg.PeerID
		}
		cs.mu.Unlock()
	}
	hm.host.mu.Unlock()
}

// verifyHeartbeat verifies the EIP-712 signature on a heartbeat
func (hm *HeartbeatManager) verifyHeartbeat(msg *HeartbeatMessage) bool {
	hm.mu.RLock()
	config := hm.config
	hm.mu.RUnlock()

	if config == nil {
		// Accept without verification if domain not set
		return true
	}

	// Check timestamp is recent (within 5 minutes)
	now := time.Now().UnixMilli()
	if now-msg.Timestamp > 300000 || msg.Timestamp-now > 120000 {
		return false
	}

	// Recreate the typed data hash
	typedData := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Heartbeat": {
				{Name: "node", Type: "address"},
				{Name: "timestamp", Type: "uint256"},
				{Name: "nonce", Type: "string"},
			},
		},
		PrimaryType: "Heartbeat",
		Domain: apitypes.TypedDataDomain{
			Name:              "ZNodeStaking",
			Version:           "1",
			ChainId:           math.NewHexOrDecimal256(config.ChainID),
			VerifyingContract: config.StakingAddress,
		},
		Message: apitypes.TypedDataMessage{
			"node":      strings.ToLower(msg.Address),
			"timestamp": math.NewHexOrDecimal256(msg.Timestamp),
			"nonce":     msg.Nonce,
		},
	}

	domainSeparator, err := typedData.HashStruct("EIP712Domain", typedData.Domain.Map())
	if err != nil {
		return false
	}

	messageHash, err := typedData.HashStruct(typedData.PrimaryType, typedData.Message)
	if err != nil {
		return false
	}

	rawData := []byte(fmt.Sprintf("\x19\x01%s%s", string(domainSeparator), string(messageHash)))
	hash := crypto.Keccak256Hash(rawData)

	// Decode signature
	sigBytes := common.FromHex(msg.Signature)
	if len(sigBytes) != 65 {
		return false
	}

	// Adjust v value for recovery
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}

	// Recover public key
	pubKey, err := crypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return false
	}

	// Check that recovered address matches claimed address
	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return strings.EqualFold(recoveredAddr.Hex(), msg.Address)
}

// recordPresence records a peer's presence
func (hm *HeartbeatManager) recordPresence(address, peerID, signature string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	addrLower := strings.ToLower(address)
	hm.recentPeers[addrLower] = &PeerPresence{
		Address:   addrLower,
		PeerID:    peerID,
		LastSeen:  time.Now(),
		Signature: signature,
	}
}

// cleanupOld removes peers not seen within TTL
func (hm *HeartbeatManager) cleanupOld() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	cutoff := time.Now().Add(-time.Duration(hm.ttlMs) * time.Millisecond)
	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.Before(cutoff) {
			delete(hm.recentPeers, addr)
		}
	}
}

// GetRecentPeers returns addresses of peers seen within TTL
func (hm *HeartbeatManager) GetRecentPeers(ttlMs int) []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	if ttlMs <= 0 {
		ttlMs = hm.ttlMs
	}

	cutoff := time.Now().Add(-time.Duration(ttlMs) * time.Millisecond)
	var peers []string
	for addr, presence := range hm.recentPeers {
		if presence.LastSeen.After(cutoff) {
			peers = append(peers, addr)
		}
	}
	return peers
}

// GetPeerPresence returns presence info for all recent peers
func (hm *HeartbeatManager) GetPeerPresence() map[string]*PeerPresence {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	result := make(map[string]*PeerPresence)
	for addr, presence := range hm.recentPeers {
		result[addr] = &PeerPresence{
			Address:   presence.Address,
			PeerID:    presence.PeerID,
			LastSeen:  presence.LastSeen,
			Signature: presence.Signature,
		}
	}
	return result
}
