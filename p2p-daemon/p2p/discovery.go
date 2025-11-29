package p2p

import (
	"context"
	"log"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
)

const (
	// DiscoveryNamespace is the rendezvous string for finding znode peers
	DiscoveryNamespace = "znode/v1"
	// DiscoveryInterval is how often to re-announce and search for peers
	DiscoveryInterval = 30 * time.Second
)

// DiscoveryManager handles DHT-based peer discovery
type DiscoveryManager struct {
	host       host.Host
	ctx        context.Context
	cancel     context.CancelFunc
	dht        *dht.IpfsDHT
	discovery  *drouting.RoutingDiscovery
	mu         sync.RWMutex
	running    bool
	onPeerFound func(peer.AddrInfo)
}

// NewDiscoveryManager creates a new DHT-based discovery manager
func NewDiscoveryManager(ctx context.Context, h host.Host, bootstrapPeers []peer.AddrInfo) (*DiscoveryManager, error) {
	dctx, cancel := context.WithCancel(ctx)

	// Create DHT in server mode so we can help other peers discover each other
	kdht, err := dht.New(dctx, h,
		dht.Mode(dht.ModeAutoServer),
		dht.BootstrapPeers(bootstrapPeers...),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	// Bootstrap the DHT
	if err := kdht.Bootstrap(dctx); err != nil {
		cancel()
		return nil, err
	}

	// Create routing discovery
	routingDiscovery := drouting.NewRoutingDiscovery(kdht)

	dm := &DiscoveryManager{
		host:      h,
		ctx:       dctx,
		cancel:    cancel,
		dht:       kdht,
		discovery: routingDiscovery,
	}

	return dm, nil
}

// SetPeerFoundCallback sets the callback for when new peers are found
func (dm *DiscoveryManager) SetPeerFoundCallback(cb func(peer.AddrInfo)) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.onPeerFound = cb
}

// Start begins advertising ourselves and searching for peers
func (dm *DiscoveryManager) Start() {
	dm.mu.Lock()
	if dm.running {
		dm.mu.Unlock()
		return
	}
	dm.running = true
	dm.mu.Unlock()

	log.Printf("[Discovery] Starting DHT-based peer discovery (namespace: %s)", DiscoveryNamespace)

	// Advertise ourselves
	go dm.advertiseLoop()

	// Search for peers
	go dm.discoverLoop()
}

// Stop stops the discovery manager
func (dm *DiscoveryManager) Stop() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if !dm.running {
		return
	}
	dm.running = false
	dm.cancel()
	dm.dht.Close()
}

// advertiseLoop periodically announces our presence
func (dm *DiscoveryManager) advertiseLoop() {
	// Initial advertisement
	dm.advertise()

	ticker := time.NewTicker(DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.advertise()
		}
	}
}

// advertise announces ourselves to the DHT
func (dm *DiscoveryManager) advertise() {
	ttl, err := dm.discovery.Advertise(dm.ctx, DiscoveryNamespace)
	if err != nil {
		log.Printf("[Discovery] Advertise error: %v", err)
		return
	}
	log.Printf("[Discovery] Advertised to DHT (TTL: %v)", ttl)
}

// discoverLoop periodically searches for new peers
func (dm *DiscoveryManager) discoverLoop() {
	// Initial discovery after short delay to let DHT bootstrap
	time.Sleep(5 * time.Second)
	dm.findPeers()

	ticker := time.NewTicker(DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.findPeers()
		}
	}
}

// findPeers searches for other znode peers
func (dm *DiscoveryManager) findPeers() {
	peerChan, err := dm.discovery.FindPeers(dm.ctx, DiscoveryNamespace)
	if err != nil {
		log.Printf("[Discovery] FindPeers error: %v", err)
		return
	}

	// Use dutil for TTL-based peer finding which is more efficient
	dutil.Advertise(dm.ctx, dm.discovery, DiscoveryNamespace)

	foundCount := 0
	for peerInfo := range peerChan {
		// Skip self
		if peerInfo.ID == dm.host.ID() {
			continue
		}

		// Skip peers we're already connected to
		if dm.host.Network().Connectedness(peerInfo.ID) == 1 { // Connected
			continue
		}

		// Skip if no addresses
		if len(peerInfo.Addrs) == 0 {
			continue
		}

		foundCount++
		log.Printf("[Discovery] Found peer: %s", peerInfo.ID.String()[:16])

		// Try to connect
		go dm.connectToPeer(peerInfo)
	}

	if foundCount > 0 {
		log.Printf("[Discovery] Found %d new peers", foundCount)
	}
}

// connectToPeer attempts to connect to a discovered peer
func (dm *DiscoveryManager) connectToPeer(peerInfo peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(dm.ctx, 30*time.Second)
	defer cancel()

	if err := dm.host.Connect(ctx, peerInfo); err != nil {
		log.Printf("[Discovery] Failed to connect to %s: %v", peerInfo.ID.String()[:16], err)
		return
	}

	log.Printf("[Discovery] Connected to peer: %s", peerInfo.ID.String()[:16])

	// Notify callback
	dm.mu.RLock()
	cb := dm.onPeerFound
	dm.mu.RUnlock()

	if cb != nil {
		cb(peerInfo)
	}
}

// GetConnectedPeers returns the number of connected peers
func (dm *DiscoveryManager) GetConnectedPeers() int {
	return len(dm.host.Network().Peers())
}

// GetDHT returns the underlying DHT for advanced usage
func (dm *DiscoveryManager) GetDHT() *dht.IpfsDHT {
	return dm.dht
}
