package rpc

import (
	"encoding/json"

	"github.com/zerofi-io/znodev2/p2p-daemon/p2p"
)

// Handlers implements all RPC method handlers
type Handlers struct {
	host *p2p.Host
}

// NewHandlers creates a new handlers instance
func NewHandlers(host *p2p.Host) *Handlers {
	return &Handlers{host: host}
}

// --- Lifecycle Methods ---

// StartParams for P2P.Start
type StartParams struct {
	// Host is already started by daemon, this is a no-op
}

// StartResult for P2P.Start
type StartResult struct {
	PeerID     string   `json:"peerId"`
	Multiaddrs []string `json:"multiaddrs"`
}

func (h *Handlers) Start(params json.RawMessage) (interface{}, *Error) {
	// Host already started by daemon
	addrs := h.host.Addrs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}
	return &StartResult{
		PeerID:     h.host.PeerID(),
		Multiaddrs: addrStrs,
	}, nil
}

// StopResult for P2P.Stop
type StopResult struct{}

func (h *Handlers) Stop(params json.RawMessage) (interface{}, *Error) {
	// Daemon handles actual shutdown
	return &StopResult{}, nil
}

// StatusResult for P2P.Status
type StatusResult struct {
	PeerID         string   `json:"peerId"`
	ConnectedPeers int      `json:"connectedPeers"`
	ActiveClusters []string `json:"activeClusters"`
	ListenAddrs    []string `json:"listenAddrs"`
}

func (h *Handlers) Status(params json.RawMessage) (interface{}, *Error) {
	addrs := h.host.Addrs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}
	return &StatusResult{
		PeerID:         h.host.PeerID(),
		ConnectedPeers: h.host.ConnectedPeerCount(),
		ActiveClusters: h.host.ActiveClusters(),
		ListenAddrs:    addrStrs,
	}, nil
}

// --- Cluster Methods ---

// JoinClusterParams for P2P.JoinCluster
type JoinClusterParams struct {
	ClusterID     string `json:"clusterId"`
	Members       []Member `json:"members"`
	IsCoordinator bool   `json:"isCoordinator"`
}

// Member represents a cluster member
type Member struct {
	Address string `json:"address"`
	PeerID  string `json:"peerId,omitempty"`
}

// JoinClusterResult for P2P.JoinCluster
type JoinClusterResult struct {
	Joined bool `json:"joined"`
}

func (h *Handlers) JoinCluster(params json.RawMessage) (interface{}, *Error) {
	var p JoinClusterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	// Convert members to peer format
	peers := make([]p2p.PeerInfo, len(p.Members))
	for i, m := range p.Members {
		peers[i] = p2p.PeerInfo{Address: m.Address, PeerID: m.PeerID}
	}

	if err := h.host.JoinCluster(p.ClusterID, peers, p.IsCoordinator); err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &JoinClusterResult{Joined: true}, nil
}

// LeaveClusterParams for P2P.LeaveCluster
type LeaveClusterParams struct {
	ClusterID string `json:"clusterId"`
}

// LeaveClusterResult for P2P.LeaveCluster
type LeaveClusterResult struct{}

func (h *Handlers) LeaveCluster(params json.RawMessage) (interface{}, *Error) {
	var p LeaveClusterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if err := h.host.LeaveCluster(p.ClusterID); err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &LeaveClusterResult{}, nil
}

// --- Round Coordination Methods ---

// BroadcastRoundDataParams for P2P.BroadcastRoundData
type BroadcastRoundDataParams struct {
	ClusterID string `json:"clusterId"`
	Round     int    `json:"round"`
	Payload   string `json:"payload"`
}

// BroadcastRoundDataResult for P2P.BroadcastRoundData
type BroadcastRoundDataResult struct {
	Delivered int `json:"delivered"`
}

func (h *Handlers) BroadcastRoundData(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastRoundDataParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastRoundData(p.ClusterID, p.Round, p.Payload)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastRoundDataResult{Delivered: delivered}, nil
}

// WaitForRoundCompletionParams for P2P.WaitForRoundCompletion
type WaitForRoundCompletionParams struct {
	ClusterID string   `json:"clusterId"`
	Round     int      `json:"round"`
	Members   []string `json:"members"`
	TimeoutMs int      `json:"timeoutMs"`
}

// WaitForRoundCompletionResult for P2P.WaitForRoundCompletion
type WaitForRoundCompletionResult struct {
	Complete bool     `json:"complete"`
	Received []string `json:"received"`
	Missing  []string `json:"missing"`
}

func (h *Handlers) WaitForRoundCompletion(params json.RawMessage) (interface{}, *Error) {
	var p WaitForRoundCompletionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForRoundCompletion(p.ClusterID, p.Round, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

// GetPeerPayloadsParams for P2P.GetPeerPayloads
type GetPeerPayloadsParams struct {
	ClusterID string   `json:"clusterId"`
	Round     int      `json:"round"`
	Members   []string `json:"members"`
}

// PeerPayload represents a payload from a peer
type PeerPayload struct {
	Address string `json:"address"`
	Payload string `json:"payload"`
}

// GetPeerPayloadsResult for P2P.GetPeerPayloads
type GetPeerPayloadsResult struct {
	Payloads []PeerPayload `json:"payloads"`
}

func (h *Handlers) GetPeerPayloads(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerPayloadsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	payloads := h.host.GetPeerPayloads(p.ClusterID, p.Round, p.Members)
	result := make([]PeerPayload, 0, len(payloads))
	for addr, payload := range payloads {
		result = append(result, PeerPayload{Address: addr, Payload: payload})
	}

	return &GetPeerPayloadsResult{Payloads: result}, nil
}

// --- Signature Barrier Methods ---

// BroadcastSignatureParams for P2P.BroadcastSignature
type BroadcastSignatureParams struct {
	ClusterID string `json:"clusterId"`
	Round     int    `json:"round"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

// BroadcastSignatureResult for P2P.BroadcastSignature
type BroadcastSignatureResult struct {
	Delivered int `json:"delivered"`
}

func (h *Handlers) BroadcastSignature(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastSignatureParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastSignature(p.ClusterID, p.Round, p.DataHash, p.Signature)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastSignatureResult{Delivered: delivered}, nil
}

// WaitForSignatureBarrierParams for P2P.WaitForSignatureBarrier
type WaitForSignatureBarrierParams struct {
	ClusterID string   `json:"clusterId"`
	Round     int      `json:"round"`
	Members   []string `json:"members"`
	TimeoutMs int      `json:"timeoutMs"`
}

// SignatureInfo represents a signature from a peer
type SignatureInfo struct {
	Address   string `json:"address"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

// WaitForSignatureBarrierResult for P2P.WaitForSignatureBarrier
type WaitForSignatureBarrierResult struct {
	Complete      bool            `json:"complete"`
	Signatures    []SignatureInfo `json:"signatures"`
	Missing       []string        `json:"missing"`
	ConsensusHash string          `json:"consensusHash"`
	Mismatched    []string        `json:"mismatched,omitempty"`

}

func (h *Handlers) WaitForSignatureBarrier(params json.RawMessage) (interface{}, *Error) {
	var p WaitForSignatureBarrierParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForSignatureBarrier(p.ClusterID, p.Round, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

// --- Identity Methods ---

// BroadcastIdentityParams for P2P.BroadcastIdentity
type BroadcastIdentityParams struct {
	ClusterID string `json:"clusterId"`
	PublicKey string `json:"publicKey"`
}

// BroadcastIdentityResult for P2P.BroadcastIdentity
type BroadcastIdentityResult struct {
	Delivered int `json:"delivered"`
}

func (h *Handlers) BroadcastIdentity(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastIdentityParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastIdentity(p.ClusterID, p.PublicKey)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastIdentityResult{Delivered: delivered}, nil
}

// WaitForIdentitiesParams for P2P.WaitForIdentities
type WaitForIdentitiesParams struct {
	ClusterID string   `json:"clusterId"`
	Members   []string `json:"members"`
	TimeoutMs int      `json:"timeoutMs"`
}

// IdentityInfo represents identity info from a peer
type IdentityInfo struct {
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	PublicKey string `json:"publicKey"`
}

// WaitForIdentitiesResult for P2P.WaitForIdentities
type WaitForIdentitiesResult struct {
	Identities []IdentityInfo `json:"identities"`
}

func (h *Handlers) WaitForIdentities(params json.RawMessage) (interface{}, *Error) {
	var p WaitForIdentitiesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForIdentities(p.ClusterID, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

// GetPeerBindingsParams for P2P.GetPeerBindings
type GetPeerBindingsParams struct {
	ClusterID string `json:"clusterId"`
}

// PeerBinding represents an address -> peerId binding
type PeerBinding struct {
	Address string `json:"address"`
	PeerID  string `json:"peerId"`
}

// GetPeerBindingsResult for P2P.GetPeerBindings
type GetPeerBindingsResult struct {
	Bindings []PeerBinding `json:"bindings"`
}

func (h *Handlers) GetPeerBindings(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerBindingsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	bindings := h.host.GetPeerBindings(p.ClusterID)
	result := make([]PeerBinding, 0, len(bindings))
	for addr, peerID := range bindings {
		result = append(result, PeerBinding{Address: addr, PeerID: peerID})
	}

	return &GetPeerBindingsResult{Bindings: result}, nil
}

// ClearPeerBindingsParams for P2P.ClearPeerBindings
type ClearPeerBindingsParams struct {
	ClusterID string `json:"clusterId"`
}

// ClearPeerBindingsResult for P2P.ClearPeerBindings
type ClearPeerBindingsResult struct{}

func (h *Handlers) ClearPeerBindings(params json.RawMessage) (interface{}, *Error) {
	var p ClearPeerBindingsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearPeerBindings(p.ClusterID)
	return &ClearPeerBindingsResult{}, nil
}

// --- Health/Scoring Methods ---

// GetPeerScoresParams for P2P.GetPeerScores
type GetPeerScoresParams struct {
	ClusterID string `json:"clusterId"`
}

// PeerScore represents a peer's health score
type PeerScore struct {
	Address string  `json:"address"`
	Score   float64 `json:"score"`
}

// GetPeerScoresResult for P2P.GetPeerScores
type GetPeerScoresResult struct {
	Scores []PeerScore `json:"scores"`
}

func (h *Handlers) GetPeerScores(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerScoresParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	scores := h.host.GetPeerScores(p.ClusterID)
	result := make([]PeerScore, 0, len(scores))
	for addr, score := range scores {
		result = append(result, PeerScore{Address: addr, Score: score})
	}

	return &GetPeerScoresResult{Scores: result}, nil
}

// ReportPeerFailureParams for P2P.ReportPeerFailure
type ReportPeerFailureParams struct {
	ClusterID string `json:"clusterId"`
	Address   string `json:"address"`
	Reason    string `json:"reason"`
}

// ReportPeerFailureResult for P2P.ReportPeerFailure
type ReportPeerFailureResult struct{}

func (h *Handlers) ReportPeerFailure(params json.RawMessage) (interface{}, *Error) {
	var p ReportPeerFailureParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ReportPeerFailure(p.ClusterID, p.Address, p.Reason)
	return &ReportPeerFailureResult{}, nil
}

// SetHeartbeatDomain sets the EIP-712 domain for heartbeat signing
func (h *Handlers) SetHeartbeatDomain(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		ChainID        int64  `json:"chainId"`
		StakingAddress string `json:"stakingAddress"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}

	hm := h.host.GetHeartbeatManager()
	hm.SetDomain(req.ChainID, req.StakingAddress)

	return map[string]interface{}{
		"success": true,
	}, nil
}

// StartHeartbeat starts periodic heartbeat broadcasting
func (h *Handlers) StartHeartbeat(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		IntervalMs int `json:"intervalMs"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}

	if req.IntervalMs <= 0 {
		req.IntervalMs = 30000 // Default 30 seconds
	}

	hm := h.host.GetHeartbeatManager()
	if err := hm.Start(req.IntervalMs); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to start heartbeat: " + err.Error()}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

// StopHeartbeat stops heartbeat broadcasting
func (h *Handlers) StopHeartbeat(params json.RawMessage) (interface{}, *Error) {
	hm := h.host.GetHeartbeatManager()
	hm.Stop()

	return map[string]interface{}{
		"success": true,
	}, nil
}

// BroadcastHeartbeat sends a single heartbeat to all connected peers
func (h *Handlers) BroadcastHeartbeat(params json.RawMessage) (interface{}, *Error) {
	hm := h.host.GetHeartbeatManager()
	if err := hm.Broadcast(); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to broadcast: " + err.Error()}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

// GetRecentPeers returns addresses of peers seen within TTL
func (h *Handlers) GetRecentPeers(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		TTLMs int `json:"ttlMs"`
	}
	if params != nil {
		json.Unmarshal(params, &req)
	}

	hm := h.host.GetHeartbeatManager()
	peers := hm.GetRecentPeers(req.TTLMs)

	return map[string]interface{}{
		"peers": peers,
	}, nil
}

// StartQueueDiscovery - placeholder that starts heartbeat for queue discovery
func (h *Handlers) StartQueueDiscovery(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		StakingAddress string `json:"stakingAddress"`
		ChainID        int64  `json:"chainId"`
		IntervalMs     int    `json:"intervalMs"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}

	if req.IntervalMs <= 0 {
		req.IntervalMs = 30000
	}

	hm := h.host.GetHeartbeatManager()
	hm.SetDomain(req.ChainID, req.StakingAddress)
	if err := hm.Start(req.IntervalMs); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to start: " + err.Error()}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

// ClearClusterSignaturesParams for P2P.ClearClusterSignatures
type ClearClusterSignaturesParams struct {
	ClusterID string `json:"clusterId"`
	Rounds    []int  `json:"rounds"`
}

func (h *Handlers) ClearClusterSignatures(params json.RawMessage) (interface{}, *Error) {
	var p ClearClusterSignaturesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearClusterSignatures(p.ClusterID, p.Rounds)
	return map[string]interface{}{"success": true}, nil
}

// ClearClusterRoundsParams for P2P.ClearClusterRounds
type ClearClusterRoundsParams struct {
	ClusterID string `json:"clusterId"`
	Rounds    []int  `json:"rounds"`
}

func (h *Handlers) ClearClusterRounds(params json.RawMessage) (interface{}, *Error) {
	var p ClearClusterRoundsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearClusterRounds(p.ClusterID, p.Rounds)
	return map[string]interface{}{"success": true}, nil
}

// GetLastHeartbeatParams for P2P.GetLastHeartbeat
type GetLastHeartbeatParams struct {
	Address string `json:"address"`
}

func (h *Handlers) GetLastHeartbeat(params json.RawMessage) (interface{}, *Error) {
	var p GetLastHeartbeatParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	timestamp := h.host.GetLastHeartbeat(p.Address)
	return map[string]interface{}{
		"address":   p.Address,
		"timestamp": timestamp,
	}, nil
}

// GetHeartbeatsParams for P2P.GetHeartbeats
type GetHeartbeatsParams struct {
	TTLMs int `json:"ttlMs"`
}

func (h *Handlers) GetHeartbeats(params json.RawMessage) (interface{}, *Error) {
	var p GetHeartbeatsParams
	if params != nil {
		json.Unmarshal(params, &p)
	}

	heartbeats := h.host.GetHeartbeats(p.TTLMs)
	// Convert to array of {address, timestamp} for easier JS consumption
	result := make([]map[string]interface{}, 0, len(heartbeats))
	for addr, ts := range heartbeats {
		result = append(result, map[string]interface{}{
			"address":   addr,
			"timestamp": ts,
		})
	}

	return map[string]interface{}{
		"heartbeats": result,
	}, nil
}

// WaitForIdentityBarrierParams for P2P.WaitForIdentityBarrier
type WaitForIdentityBarrierParams struct {
	ClusterID string   `json:"clusterId"`
	Members   []string `json:"members"`
	TimeoutMs int      `json:"timeoutMs"`
}

func (h *Handlers) WaitForIdentityBarrier(params json.RawMessage) (interface{}, *Error) {
	var p WaitForIdentityBarrierParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	// Reuse WaitForIdentities logic
	result, err := h.host.WaitForIdentities(p.ClusterID, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}


// --- Consensus Hash Methods ---

// ComputeConsensusHashParams for P2P.ComputeConsensusHash
type ComputeConsensusHashParams struct {
	ClusterID string   `json:"clusterId"`
	Round     int      `json:"round"`
	Members   []string `json:"members"`
}

// ComputeConsensusHashResult for P2P.ComputeConsensusHash
type ComputeConsensusHashResult struct {
	Hash string `json:"hash"`
}

func (h *Handlers) ComputeConsensusHash(params json.RawMessage) (interface{}, *Error) {
	var p ComputeConsensusHashParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	hash := h.host.ComputePayloadConsensusHash(p.ClusterID, p.Round, p.Members)
	if hash == "" {
		return nil, &Error{Code: ErrCodeInternal, Message: "cannot compute hash - missing round data"}
	}

	return &ComputeConsensusHashResult{Hash: hash}, nil
}

// --- PBFT Consensus Methods ---



// PBFTDebugParams for P2P.PBFTDebug
type PBFTDebugParams struct {
	ClusterID string `json:"clusterId"`
	Phase     string `json:"phase"`
}

// PBFTDebugResult for P2P.PBFTDebug
type PBFTDebugResult struct {
	Exists bool                          `json:"exists"`
	State  *p2p.PBFTConsensusDebugState  `json:"state,omitempty"`
}

func (h *Handlers) PBFTDebug(params json.RawMessage) (interface{}, *Error) {
	var p PBFTDebugParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if p.ClusterID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "clusterId is required"}
	}
	if p.Phase == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "phase is required"}
	}

	mgr := h.host.GetPBFTManager()
	if mgr == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "PBFT manager not initialized"}
	}

	state := mgr.GetConsensusDebugState(p.ClusterID, p.Phase)
	if state == nil {
		return &PBFTDebugResult{Exists: false}, nil
	}

	return &PBFTDebugResult{
		Exists: true,
		State:  state,
	}, nil
}

// RunConsensusParams for P2P.RunConsensus
type RunConsensusParams struct {
	ClusterID string   `json:"clusterId"`
	Phase     string   `json:"phase"`
	Data      string   `json:"data"`
	Members   []string `json:"members"`
}

// RunConsensusResult for P2P.RunConsensus
type RunConsensusResult struct {
	Success      bool     `json:"success"`
	Phase        string   `json:"phase"`
	ViewNumber   int      `json:"viewNumber"`
	Digest       string   `json:"digest"`
	Participants []string `json:"participants,omitempty"`
	Missing      []string `json:"missing,omitempty"`
	Aborted      bool     `json:"aborted"`
	AbortReason  string   `json:"abortReason,omitempty"`
}

func (h *Handlers) RunConsensus(params json.RawMessage) (interface{}, *Error) {
	var p RunConsensusParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if p.ClusterID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "clusterId is required"}
	}
	if p.Phase == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "phase is required"}
	}
	if len(p.Members) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "members is required"}
	}

	result, err := h.host.RunConsensus(p.ClusterID, p.Phase, p.Data, p.Members)
	if err != nil {
		// Return result with error info (for timeout/abort cases)
		if result != nil {
			return &RunConsensusResult{
				Success:     result.Success,
				Phase:       result.Phase,
				ViewNumber:  result.ViewNumber,
				Digest:      result.Digest,
				Missing:     result.Missing,
				Aborted:     result.Aborted,
				AbortReason: result.AbortReason,
			}, &Error{Code: ErrCodeTimeout, Message: err.Error()}
		}
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &RunConsensusResult{
		Success:      result.Success,
		Phase:        result.Phase,
		ViewNumber:   result.ViewNumber,
		Digest:       result.Digest,
		Participants: result.Participants,
		Aborted:      result.Aborted,
	}, nil
}

// BroadcastPreSelectionParams for P2P.BroadcastPreSelection
type BroadcastPreSelectionParams struct {
	BlockNumber uint64   `json:"blockNumber"`
	EpochSeed   string   `json:"epochSeed"`
	Candidates  []string `json:"candidates"`
}

// BroadcastPreSelectionResult for P2P.BroadcastPreSelection
type BroadcastPreSelectionResult struct {
	ProposalID string `json:"proposalId"`
	Success    bool   `json:"success"`
}

func (h *Handlers) BroadcastPreSelection(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastPreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.BlockNumber == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "blockNumber is required"}
	}
	if len(p.Candidates) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "candidates is required"}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	proposal, err := psm.BroadcastProposal(p.BlockNumber, p.EpochSeed, p.Candidates)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastPreSelectionResult{
		ProposalID: proposal.ProposalID,
		Success:    true,
	}, nil
}

// VotePreSelectionParams for P2P.VotePreSelection
type VotePreSelectionParams struct {
	ProposalID      string   `json:"proposalId"`
	Approved        bool     `json:"approved"`
	LocalCandidates []string `json:"localCandidates,omitempty"`
}

// VotePreSelectionResult for P2P.VotePreSelection
type VotePreSelectionResult struct {
	Success bool `json:"success"`
}

func (h *Handlers) VotePreSelection(params json.RawMessage) (interface{}, *Error) {
	var p VotePreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.ProposalID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "proposalId is required"}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	err := psm.VoteOnProposal(p.ProposalID, p.Approved, p.LocalCandidates)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &VotePreSelectionResult{Success: true}, nil
}

// WaitPreSelectionParams for P2P.WaitPreSelection
type WaitPreSelectionParams struct {
	ProposalID     string   `json:"proposalId"`
	ExpectedVoters []string `json:"expectedVoters"`
	TimeoutMs      int      `json:"timeoutMs,omitempty"`
}

// WaitPreSelectionResult for P2P.WaitPreSelection
type WaitPreSelectionResult struct {
	ProposalID string   `json:"proposalId"`
	Success    bool     `json:"success"`
	Approved   int      `json:"approved"`
	Total      int      `json:"total"`
	Missing    []string `json:"missing,omitempty"`
}

func (h *Handlers) WaitPreSelection(params json.RawMessage) (interface{}, *Error) {
	var p WaitPreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.ProposalID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "proposalId is required"}
	}
	if len(p.ExpectedVoters) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "expectedVoters is required"}
	}

	timeoutMs := p.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 60000
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	result, err := psm.WaitForConsensus(p.ProposalID, p.ExpectedVoters, timeoutMs)
	if err != nil {
		if result != nil {
			return &WaitPreSelectionResult{
				ProposalID: result.ProposalID,
				Success:    result.Success,
				Approved:   result.Approved,
				Total:      result.Total,
				Missing:    result.Missing,
			}, &Error{Code: ErrCodeTimeout, Message: err.Error()}
		}
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &WaitPreSelectionResult{
		ProposalID: result.ProposalID,
		Success:    result.Success,
		Approved:   result.Approved,
		Total:      result.Total,
	}, nil
}

// GetPreSelectionProposalParams for P2P.GetPreSelectionProposal
type GetPreSelectionProposalParams struct {
	ProposalID string `json:"proposalId,omitempty"`
}

func (h *Handlers) GetPreSelectionProposal(params json.RawMessage) (interface{}, *Error) {
	var p GetPreSelectionProposalParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	var proposal *p2p.PreSelectionProposal
	if p.ProposalID != "" {
		proposal = psm.GetPendingProposal(p.ProposalID)
	} else {
		proposal = psm.GetLatestProposal()
	}

	if proposal == nil {
		return map[string]interface{}{"found": false}, nil
	}

	return map[string]interface{}{
		"found":       true,
		"proposalId":  proposal.ProposalID,
		"blockNumber": proposal.BlockNumber,
		"epochSeed":   proposal.EpochSeed,
		"candidates":  proposal.Candidates,
		"coordinator": proposal.Coordinator,
		"timestamp":   proposal.Timestamp,
	}, nil
}

// GetRoundDataParams for P2P.GetRoundData
type GetRoundDataParams struct {
	ClusterID string `json:"clusterId"`
	Round     int    `json:"round"`
}

// GetRoundDataResult for P2P.GetRoundData
type GetRoundDataResult struct {
	Data map[string]string `json:"data"`
}

func (h *Handlers) GetRoundData(params json.RawMessage) (interface{}, *Error) {
	var p GetRoundDataParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}
	data := h.host.GetRoundData(p.ClusterID, p.Round)
	return GetRoundDataResult{Data: data}, nil
}
