package session

import (
	"io"
	"sync"
	"time"
)

// PoolEntry is a cached raw connection.
type PoolEntry struct {
	DCOption  DataCenter
	Conn      io.Closer
	CreatedAt time.Time
}

// ConnectionPool caches recently successful raw connections to avoid redundant
// TCP handshakes on reconnect. Entries are consumed on first use (single-use).
// Ported from td/td/telegram/net/ConnectionCreator.cpp (ready_connections, READY_CONNECTIONS_TIMEOUT).
type ConnectionPool struct {
	entries []PoolEntry
	ttl     time.Duration
	mu      sync.Mutex
}

// NewConnectionPool creates a connection pool with the given TTL.
func NewConnectionPool(ttl time.Duration) *ConnectionPool {
	return &ConnectionPool{
		ttl: ttl,
	}
}

// Get returns a cached connection for the given endpoint if it exists and has
// not expired. The entry is consumed (removed) on cache hit. Returns nil, false
// on cache miss or expiry.
func (p *ConnectionPool) Get(dcID int, option DataCenter) (io.Closer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for i, entry := range p.entries {
		if entry.DCOption.ID == dcID && entry.DCOption == option {
			if now.Sub(entry.CreatedAt) < p.ttl {
				// Cache hit — consume the entry
				conn := entry.Conn
				p.entries = append(p.entries[:i], p.entries[i+1:]...)
				return conn, true
			}
			// Expired — evict
			p.entries = append(p.entries[:i], p.entries[i+1:]...)
			return nil, false
		}
	}
	return nil, false
}

// Put caches a successful connection for the given endpoint.
func (p *ConnectionPool) Put(dcID int, option DataCenter, conn io.Closer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entries = append(p.entries, PoolEntry{
		DCOption:  option,
		Conn:      conn,
		CreatedAt: time.Now(),
	})
}

// Evict removes a specific cached entry (e.g., on connection failure).
func (p *ConnectionPool) Evict(dcID int, option DataCenter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, entry := range p.entries {
		if entry.DCOption.ID == dcID && entry.DCOption == option {
			p.entries = append(p.entries[:i], p.entries[i+1:]...)
			return
		}
	}
}

// Purge removes all expired entries. Call periodically to free resources.
func (p *ConnectionPool) Purge() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	purged := 0
	for i := len(p.entries) - 1; i >= 0; i-- {
		if now.Sub(p.entries[i].CreatedAt) >= p.ttl {
			p.entries = append(p.entries[:i], p.entries[i+1:]...)
			purged++
		}
	}
	return purged
}

// Count returns the number of cached entries.
func (p *ConnectionPool) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.entries)
}
