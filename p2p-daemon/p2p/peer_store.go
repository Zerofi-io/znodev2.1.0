package p2p

import (
    "encoding/json"
    "log"
    "os"
    "path/filepath"
    "sync"
    "time"

    "github.com/libp2p/go-libp2p/core/peer"
)

// Environment variable to override the default peers file path.
const peersFileEnvKey = "P2P_PEERS_FILE"

// storedPeer represents a peer entry persisted to disk.
type storedPeer struct {
    ID       string   `json:"id"`
    Addrs    []string `json:"addrs"`
    LastSeen int64    `json:"lastSeen"`
}

// storedPeerFile is the on-disk format for persisted peers.
type storedPeerFile struct {
    Version int          `json:"version"`
    Peers   []storedPeer `json:"peers"`
}

// PersistentPeerStore handles durable storage of discovered peers.
type PersistentPeerStore struct {
    path string
    mu   sync.Mutex
}

// NewPersistentPeerStoreFromEnv constructs a peer store using either
// P2P_PEERS_FILE or a default location under the user's home directory.
// If no suitable path can be determined, it returns nil and persistence is disabled.
func NewPersistentPeerStoreFromEnv() *PersistentPeerStore {
    path := os.Getenv(peersFileEnvKey)
    if path == "" {
        home, err := os.UserHomeDir()
        if err != nil {
            log.Printf("[Peers] Could not determine home directory for peer store: %v (persistence disabled)", err)
            return nil
        }
        path = filepath.Join(home, ".znode", "p2p-peers.json")
    }
    return &PersistentPeerStore{path: path}
}

// LoadBootstrapMultiaddrs returns all stored multiaddrs that can be used as
// additional bootstrap addresses. It is safe to call with a nil store.
func (s *PersistentPeerStore) LoadBootstrapMultiaddrs() []string {
    if s == nil || s.path == "" {
        return nil
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    file, err := s.loadLocked()
    if err != nil {
        log.Printf("[Peers] Failed to load persisted peers from %s: %v", s.path, err)
        return nil
    }

    addrSet := make(map[string]struct{})
    for _, p := range file.Peers {
        for _, a := range p.Addrs {
            if a == "" {
                continue
            }
            addrSet[a] = struct{}{}
        }
    }

    if len(addrSet) == 0 {
        return nil
    }

    result := make([]string, 0, len(addrSet))
    for a := range addrSet {
        result = append(result, a)
    }
    return result
}

// RememberPeer records a successfully connected peer to disk. It is safe to
// call with a nil store.
func (s *PersistentPeerStore) RememberPeer(info peer.AddrInfo) {
    if s == nil || s.path == "" {
        return
    }

    s.mu.Lock()
    defer s.mu.Unlock()

    file, err := s.loadLocked()
    if err != nil {
        log.Printf("[Peers] Failed to load persisted peers from %s: %v", s.path, err)
        // proceed with empty file to avoid dropping this peer entirely
        file = &storedPeerFile{Version: 1}
    }

    now := time.Now().Unix()

    // Convert AddrInfo to multiaddrs including /p2p/ component.
    addrs, err := peer.AddrInfoToP2pAddrs(&info)
    if err != nil {
        log.Printf("[Peers] Failed to convert peer %s addrs for persistence: %v", info.ID, err)
        return
    }
    newAddrSet := make(map[string]struct{}, len(addrs))
    for _, a := range addrs {
        if a == nil {
            continue
        }
        s := a.String()
        if s == "" {
            continue
        }
        newAddrSet[s] = struct{}{}
    }
    if len(newAddrSet) == 0 {
        return
    }

    // Merge into existing peers list.
    updated := false
    for i := range file.Peers {
        if file.Peers[i].ID == info.ID.String() {
            // Merge addresses
            existing := make(map[string]struct{}, len(file.Peers[i].Addrs))
            for _, a := range file.Peers[i].Addrs {
                if a == "" {
                    continue
                }
                existing[a] = struct{}{}
            }
            for a := range newAddrSet {
                if _, ok := existing[a]; !ok {
                    file.Peers[i].Addrs = append(file.Peers[i].Addrs, a)
                    existing[a] = struct{}{}
                }
            }
            file.Peers[i].LastSeen = now
            updated = true
            break
        }
    }

    if !updated {
        addrsSlice := make([]string, 0, len(newAddrSet))
        for a := range newAddrSet {
            addrsSlice = append(addrsSlice, a)
        }
        file.Peers = append(file.Peers, storedPeer{
            ID:       info.ID.String(),
            Addrs:    addrsSlice,
            LastSeen: now,
        })
    }

    if err := s.saveLocked(file); err != nil {
        log.Printf("[Peers] Failed to persist peers to %s: %v", s.path, err)
    }
}

// loadLocked reads and decodes the peers file. Caller must hold s.mu.
func (s *PersistentPeerStore) loadLocked() (*storedPeerFile, error) {
    data, err := os.ReadFile(s.path)
    if err != nil {
        if os.IsNotExist(err) {
            return &storedPeerFile{Version: 1}, nil
        }
        return &storedPeerFile{Version: 1}, err
    }

    var file storedPeerFile
    if err := json.Unmarshal(data, &file); err != nil {
        log.Printf("[Peers] Invalid peers file %s (starting fresh): %v", s.path, err)
        return &storedPeerFile{Version: 1}, nil
    }
    if file.Version == 0 {
        file.Version = 1
    }
    return &file, nil
}

// saveLocked atomically writes the peers file. Caller must hold s.mu.
func (s *PersistentPeerStore) saveLocked(file *storedPeerFile) error {
    if file == nil {
        file = &storedPeerFile{Version: 1}
    }

    dir := filepath.Dir(s.path)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return err
    }

    tmp, err := os.CreateTemp(dir, "peers-*.json")
    if err != nil {
        return err
    }
    enc := json.NewEncoder(tmp)
    enc.SetIndent("", "  ")
    if err := enc.Encode(file); err != nil {
        tmp.Close()
        os.Remove(tmp.Name())
        return err
    }
    if err := tmp.Sync(); err != nil {
        tmp.Close()
        os.Remove(tmp.Name())
        return err
    }
    if err := tmp.Close(); err != nil {
        os.Remove(tmp.Name())
        return err
    }

    return os.Rename(tmp.Name(), s.path)
}
