package cmd

import (
	"context"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/zerofi-io/znodev2/p2p-daemon/p2p"
	"github.com/zerofi-io/znodev2/p2p-daemon/rpc"
)

// Config holds daemon configuration
type Config struct {
	SocketPath     string
	PrivateKey     string
	EthAddress     string
	ListenAddr     string
	BootstrapPeers string
}

// Daemon is the main p2p-daemon process
type Daemon struct {
	config   *Config
	host     *p2p.Host
	rpcSrv   *rpc.Server
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewDaemon creates a new daemon instance
func NewDaemon(config *Config) (*Daemon, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Daemon{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Start starts the daemon
func (d *Daemon) Start() error {
	// Remove existing socket file if present
	if _, err := os.Stat(d.config.SocketPath); err == nil {
		os.Remove(d.config.SocketPath)
	}

	// Parse bootstrap peers
	var bootstrapAddrs []string
	if d.config.BootstrapPeers != "" {
		bootstrapAddrs = strings.Split(d.config.BootstrapPeers, ",")
	}

	// Create libp2p host
	hostConfig := &p2p.HostConfig{
		PrivateKey:     d.config.PrivateKey,
		EthAddress:     d.config.EthAddress,
		ListenAddr:     d.config.ListenAddr,
		BootstrapAddrs: bootstrapAddrs,
	}

	host, err := p2p.NewHost(d.ctx, hostConfig)
	if err != nil {
		return err
	}
	d.host = host

	// Create RPC server
	d.rpcSrv = rpc.NewServer(d.host)

	// Create Unix socket listener
	listener, err := net.Listen("unix", d.config.SocketPath)
	if err != nil {
		return err
	}
	d.listener = listener

	// Set socket permissions (readable/writable by owner only)
	if err := os.Chmod(d.config.SocketPath, 0600); err != nil {
		log.Printf("Warning: could not set socket permissions: %v", err)
	}

	log.Printf("JSON-RPC server listening on %s", d.config.SocketPath)
	log.Printf("libp2p host started with PeerID: %s", d.host.PeerID())
	log.Printf("Listening on: %v", d.host.Addrs())

	// Start accepting connections
	for {
		conn, err := d.listener.Accept()
		if err != nil {
			select {
			case <-d.ctx.Done():
				return nil // Graceful shutdown
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.rpcSrv.HandleConnection(conn)
		}()
	}
}

// Stop stops the daemon gracefully
func (d *Daemon) Stop() {
	d.cancel()

	if d.listener != nil {
		d.listener.Close()
	}

	if d.host != nil {
		d.host.Close()
	}

	// Wait for all connections to finish
	d.wg.Wait()

	// Clean up socket file
	os.Remove(d.config.SocketPath)

	log.Println("Daemon stopped")
}
