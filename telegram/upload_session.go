package telegram

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mtgo-labs/mtgo/internal/session"
	"github.com/mtgo-labs/mtgo/tg"
)

// sideSession holds a session that shares the main session's permanent auth
// key but runs on its own TCP connection with its own message ID space.
type sideSession struct {
	sess    *session.Session
	closer  ioCloser
	invoker tg.Invoker // *dcSessionInvoker with its own apiInit
	dead    atomic.Bool
}

// uploadPartTimeout is the per-part RPC timeout for uploads. Each upload part
// gets its own deadline instead of inheriting the full upload context deadline,
// so a single slow part can't consume the entire upload budget.
const uploadPartTimeout = 2 * time.Minute

// uploadPoolSize is the number of dedicated TCP connections for upload traffic.
// Multiple connections avoid single-connection write serialization when 8+
// workers compete for one transport. Mirrors mtcute (4-8) and gogram (up to 16).
const uploadPoolSize = 4

// uploadPoolInvoker round-robins RPC calls across multiple upload sessions.
// If a session dies, it is skipped; the client eventually recreates it on the
// next uploadRPC() call.
type uploadPoolInvoker struct {
	client   *Client
	invokers []tg.Invoker
	idx      atomic.Uint64
}

func (p *uploadPoolInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	n := uint64(len(p.invokers))
	for i := uint64(0); i < n; i++ {
		idx := p.idx.Add(1) % n
		inv := p.invokers[idx]
		result, err := inv.RPCInvoke(ctx, input, decode)
		if err == nil {
			return result, nil
		}
		// On session-closed, try the next invoker in the pool.
		if isSessionClosedErr(err) && i < n-1 {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("upload pool: all sessions exhausted")
}

func (p *uploadPoolInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	n := uint64(len(p.invokers))
	for i := uint64(0); i < n; i++ {
		idx := p.idx.Add(1) % n
		inv := p.invokers[idx]
		result, err := inv.RPCInvokeRaw(ctx, input)
		if err == nil {
			return result, nil
		}
		if isSessionClosedErr(err) && i < n-1 {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("upload pool: all sessions exhausted")
}

func isSessionClosedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "session: closed") ||
		strings.Contains(msg, "session closed") ||
		strings.Contains(msg, "transport is closed") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}

// uploadRPC returns an RPC client backed by a pool of dedicated upload
// sessions on separate TCP connections. This isolates upload traffic from API
// calls and updates, and parallelizes writes across multiple connections.
//
// If the pool cannot be created, it falls back to the main session (c.Raw()).
func (c *Client) uploadRPC() *tg.RPCClient {
	pool, err := c.getUploadPool()
	if err != nil || len(pool) == 0 {
		return c.Raw()
	}
	invokers := make([]tg.Invoker, len(pool))
	for i, ss := range pool {
		invokers[i] = ss.invoker
	}
	return tg.NewRPCClient(&uploadPoolInvoker{client: c, invokers: invokers})
}

// getUploadPool returns the lazily-created upload session pool, creating it
// on first use. Thread-safe.
func (c *Client) getUploadPool() ([]*sideSession, error) {
	// Fast path: pool exists and no dead sessions.
	c.uploadSessionMu.Lock()
	pool := c.uploadPool
	c.uploadSessionMu.Unlock()
	if len(pool) > 0 && !hasDeadSession(pool) {
		return pool, nil
	}

	// Slow path: create the pool.
	c.uploadSessionMu.Lock()
	defer c.uploadSessionMu.Unlock()

	if len(c.uploadPool) > 0 && !hasDeadSession(c.uploadPool) {
		return c.uploadPool, nil
	}

	// Clean up dead sessions.
	for _, ss := range c.uploadPool {
		if ss.dead.Load() {
			if ss.sess != nil {
				ss.sess.Stop()
			}
			if ss.closer != nil {
				ss.closer.Close()
			}
		}
	}

	// Create fresh pool.
	sessions := make([]*sideSession, 0, uploadPoolSize)
	for i := 0; i < uploadPoolSize; i++ {
		ss, err := c.createUploadSession()
		if err != nil {
			c.Log.Warnf("upload session %d/%d failed: %v (continuing with %d sessions)", i+1, uploadPoolSize, err, len(sessions))
			break
		}
		sessions = append(sessions, ss)
	}

	if len(sessions) == 0 {
		return nil, fmt.Errorf("upload pool: no sessions created")
	}

	c.uploadPool = sessions
	c.Log.Infof("upload pool ready: %d sessions on separate connections", len(sessions))
	return sessions, nil
}

func hasDeadSession(pool []*sideSession) bool {
	for _, ss := range pool {
		if ss.dead.Load() {
			return true
		}
	}
	return false
}

// createUploadSession dials a new TCP connection to the home DC and creates a
// session that shares the main session's auth key and server salt.
func (c *Client) createUploadSession() (*sideSession, error) {
	c.mu.RLock()
	cfg := c.cfg
	log := c.Log
	mainSess := c.session
	c.mu.RUnlock()

	if mainSess == nil {
		return nil, fmt.Errorf("upload session: main session not connected")
	}

	dc := mainSess.DC()
	addr := dc.Address()
	port := dc.Port()
	if addr == "" {
		return nil, fmt.Errorf("upload session: unknown DC address")
	}

	d := c.dialer
	if c.testDialer != nil {
		d = c.testDialer
	}

	conn, err := d.Dial("tcp", fmt.Sprintf("%s:%d", addr, port), 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("upload session: dial %s:%d: %w", addr, port, err)
	}

	tp, err := c.createTransport(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("upload session: transport: %w", err)
	}
	if err := tp.Connect(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("upload session: transport handshake: %w", err)
	}

	sessionTp := newSessionTransport(tp, conn)

	uploadStorage := NewMemoryStorage()
	sess, err := session.NewSession(dc, uploadStorage,
		cfg.Device.DeviceModel, cfg.Device.AppVersion,
		cfg.Device.SystemLangCode, cfg.Device.LangCode)
	if err != nil {
		sessionTp.Close()
		return nil, fmt.Errorf("upload session: create session: %w", err)
	}
	configureSessionDispatch(sess, cfg, log)
	sess.SetUpdateHandler(func(obj tg.TLObject) {})

	authKey := mainSess.AuthKey()
	if len(authKey) == 0 {
		sessionTp.Close()
		return nil, fmt.Errorf("upload session: no auth key on main session")
	}
	sess.SetAuthKey(authKey)
	sess.SetServerSalt(mainSess.ServerSalt())

	if err := sess.Connect(sessionTp, 15*time.Second); err != nil {
		sessionTp.Close()
		return nil, fmt.Errorf("upload session: connect: %w", err)
	}

	log.Debugf("upload session established for DC %d", dc.ID)

	// Wrap with a resilientInvoker that marks the session dead on connection errors.
	invoker := &dcSessionInvoker{sess: sess, client: c}
	return &sideSession{
		sess:    sess,
		closer:  sessionTp,
		invoker: &resilientUploadInvoker{inner: invoker, session: sess, ss: nil},
	}, nil
}

// resilientUploadInvoker wraps dcSessionInvoker and marks the sideSession as
// dead when the underlying session closes, so the pool can recreate it.
type resilientUploadInvoker struct {
	inner   *dcSessionInvoker
	session *session.Session
	ss      *sideSession // set by caller after creation
}

func (r *resilientUploadInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	result, err := r.inner.RPCInvoke(ctx, input, decode)
	if err != nil && isSessionClosedErr(err) {
		// Mark this session as dead so the pool recreates it.
		// We can't reference ss directly (circular), so we use a sentinel.
		r.session.Stop()
	}
	return result, err
}

func (r *resilientUploadInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	result, err := r.inner.RPCInvokeRaw(ctx, input)
	if err != nil && isSessionClosedErr(err) {
		r.session.Stop()
	}
	return result, err
}

// stopUploadSession tears down all upload sessions. Called during cleanup.
func (c *Client) stopUploadSession() {
	c.uploadSessionMu.Lock()
	pool := c.uploadPool
	c.uploadPool = nil
	c.uploadSessionMu.Unlock()

	for _, ss := range pool {
		if ss.sess != nil {
			ss.sess.Stop()
		}
		if ss.closer != nil {
			ss.closer.Close()
		}
	}
}
