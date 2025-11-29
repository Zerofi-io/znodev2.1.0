// p2p-daemon: Go-based P2P coordination daemon for znode cluster formation
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zerofi-io/znodev2/p2p-daemon/cmd"
)

var (
	version = "0.1.0"
)

func main() {
	// CLI flags
	socketPath := flag.String("socket", "/tmp/znode-p2p.sock", "Unix socket path for JSON-RPC")
	privateKey := flag.String("key", "", "Ethereum private key (hex)")
	ethAddress := flag.String("eth-address", "", "Ethereum address")
	listenAddr := flag.String("listen", "/ip4/0.0.0.0/tcp/9000", "libp2p listen address")
	bootstrapPeers := flag.String("bootstrap", "", "Comma-separated bootstrap peer multiaddrs")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("p2p-daemon version %s\n", version)
		os.Exit(0)
	}

	// Validate required flags
	if *privateKey == "" {
		log.Fatal("--key is required")
	}
	if *ethAddress == "" {
		log.Fatal("--eth-address is required")
	}

	// Create daemon config
	config := &cmd.Config{
		SocketPath:     *socketPath,
		PrivateKey:     *privateKey,
		EthAddress:     *ethAddress,
		ListenAddr:     *listenAddr,
		BootstrapPeers: *bootstrapPeers,
	}

	// Create and start daemon
	daemon, err := cmd.NewDaemon(config)
	if err != nil {
		log.Fatalf("Failed to create daemon: %v", err)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		daemon.Stop()
		os.Exit(0)
	}()

	// Start daemon (blocks)
	log.Printf("Starting p2p-daemon %s", version)
	if err := daemon.Start(); err != nil {
		log.Fatalf("Daemon error: %v", err)
	}
}
