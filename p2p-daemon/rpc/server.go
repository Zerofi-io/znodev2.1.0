package rpc

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"

	"github.com/zerofi-io/znodev2/p2p-daemon/p2p"
)

// JSON-RPC 2.0 structures
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id"`
}

type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Error codes
const (
	ErrCodeParse       = -32700
	ErrCodeInvalidReq  = -32600
	ErrCodeMethodNF    = -32601
	ErrCodeInvalidArgs = -32602
	ErrCodeInternal    = -32603
	ErrCodeTimeout     = -32000
	ErrCodePeerFailed  = -32001
)

// Server handles JSON-RPC requests
type Server struct {
	host     *p2p.Host
	handlers *Handlers
}

// NewServer creates a new RPC server
func NewServer(host *p2p.Host) *Server {
	return &Server{
		host:     host,
		handlers: NewHandlers(host),
	}
}

// HandleConnection handles a single client connection
func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)

	for {
		// Read line-delimited JSON
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// Parse request
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			resp := Response{
				JSONRPC: "2.0",
				Error: &Error{
					Code:    ErrCodeParse,
					Message: "Parse error",
				},
				ID: nil,
			}
			encoder.Encode(resp)
			continue
		}

		// Validate request
		if req.JSONRPC != "2.0" {
			resp := Response{
				JSONRPC: "2.0",
				Error: &Error{
					Code:    ErrCodeInvalidReq,
					Message: "Invalid request: must use JSON-RPC 2.0",
				},
				ID: req.ID,
			}
			encoder.Encode(resp)
			continue
		}

		// Dispatch to handler
		result, rpcErr := s.dispatch(req.Method, req.Params)

		// Build response
		resp := Response{
			JSONRPC: "2.0",
			ID:      req.ID,
		}
		if rpcErr != nil {
			resp.Error = rpcErr
		} else {
			resp.Result = result
		}

		if err := encoder.Encode(resp); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}


// dispatch routes method calls to handlers
func (s *Server) dispatch(method string, params json.RawMessage) (interface{}, *Error) {
	switch method {
	case "P2P.Start":
		return s.handlers.Start(params)
	case "P2P.Stop":
		return s.handlers.Stop(params)
	case "P2P.Status":
		return s.handlers.Status(params)
	case "P2P.JoinCluster":
		return s.handlers.JoinCluster(params)
	case "P2P.LeaveCluster":
		return s.handlers.LeaveCluster(params)
	case "P2P.BroadcastRoundData":
		return s.handlers.BroadcastRoundData(params)
	case "P2P.WaitForRoundCompletion":
		return s.handlers.WaitForRoundCompletion(params)
	case "P2P.GetPeerPayloads":
		return s.handlers.GetPeerPayloads(params)
	case "P2P.BroadcastSignature":
		return s.handlers.BroadcastSignature(params)
	case "P2P.WaitForSignatureBarrier":
		return s.handlers.WaitForSignatureBarrier(params)
	case "P2P.ComputeConsensusHash":
		return s.handlers.ComputeConsensusHash(params)
	case "P2P.RunConsensus":
		return s.handlers.RunConsensus(params)
	case "P2P.PBFTDebug":
		return s.handlers.PBFTDebug(params)
	case "P2P.BroadcastIdentity":
		return s.handlers.BroadcastIdentity(params)
	case "P2P.WaitForIdentities":
		return s.handlers.WaitForIdentities(params)
	case "P2P.GetPeerBindings":
		return s.handlers.GetPeerBindings(params)
	case "P2P.ClearPeerBindings":
		return s.handlers.ClearPeerBindings(params)
	case "P2P.GetPeerScores":
		return s.handlers.GetPeerScores(params)
	case "P2P.ReportPeerFailure":
		return s.handlers.ReportPeerFailure(params)
	case "P2P.SetHeartbeatDomain":
		return s.handlers.SetHeartbeatDomain(params)
	case "P2P.StartHeartbeat":
		return s.handlers.StartHeartbeat(params)
	case "P2P.StopHeartbeat":
		return s.handlers.StopHeartbeat(params)
	case "P2P.BroadcastHeartbeat":
		return s.handlers.BroadcastHeartbeat(params)
	case "P2P.GetRecentPeers":
		return s.handlers.GetRecentPeers(params)
	case "P2P.StartQueueDiscovery":
		return s.handlers.StartQueueDiscovery(params)
	case "P2P.GetLastHeartbeat":
		return s.handlers.GetLastHeartbeat(params)
	case "P2P.ClearClusterSignatures":
		return s.handlers.ClearClusterSignatures(params)
	case "P2P.ClearClusterRounds":
		return s.handlers.ClearClusterRounds(params)
	case "P2P.GetHeartbeats":
		return s.handlers.GetHeartbeats(params)
	case "P2P.WaitForIdentityBarrier":
		return s.handlers.WaitForIdentityBarrier(params)
	default:
		return nil, &Error{
			Code:    ErrCodeMethodNF,
			Message: "Method not found: " + method,
		}
	}
}
