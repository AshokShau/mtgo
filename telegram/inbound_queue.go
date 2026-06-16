package telegram

import (
	"hash/fnv"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// inboundQueueItem is a unit of dispatch work keyed by a routing key. Updates
// with the same routing key are delivered to the same shard/worker, preserving
// per-key ordering; updates across keys dispatch concurrently.
type inboundQueueItem struct {
	routingKey uint64
	work       func()
}

// InboundUpdateQueue is a bounded, sharded worker pool that decouples update
// reception from handler dispatch. It sustains high inbound throughput
// (1000+ updates/sec) with bounded memory and a deterministic hybrid overflow
// policy, preserving per-routing-key ordering while dispatching across keys
// concurrently.
//
// When disabled (MaxDepth == 0), callers MUST dispatch inline (backward compat,
// Constitution Principle IV).
//
// Ported conceptually from TDLib's tdactor mailbox backpressure
// (net/Session.cpp receive → handoff) and UpdatesManager gap recovery.
type InboundUpdateQueue struct {
	enabled     bool
	maxDepth    int
	stallBudget time.Duration
	workers     int

	// Sharded bounded channels: shard = hash(routingKey) % workers. Each shard
	// has a single draining worker, preserving FIFO order within a key.
	shards []chan inboundQueueItem

	workerWG  sync.WaitGroup
	closeOnce sync.Once
	done      chan struct{}

	// Metrics (atomic).
	depth     atomic.Int64
	highWater atomic.Int64
	overflow  atomic.Int64

	gapRecovery func()
	log         *Logger
}

// InboundQueueConfig configures an InboundUpdateQueue.
type InboundQueueConfig struct {
	// MaxDepth is the total buffer capacity across all shards. Must be > 0 to
	// enable the queue. When 0 the queue is disabled (Enabled() == false).
	MaxDepth int
	// Workers is the shard/worker count. Defaults to runtime.NumCPU() when 0.
	Workers int
	// StallBudget is how long Enqueue blocks on a full shard before applying
	// the overflow policy. Defaults to 500ms when zero and MaxDepth > 0.
	StallBudget time.Duration
	// GapRecovery is invoked when an update is shed (so the shed is
	// recoverable via getDifference/getChannelDifference, never silent).
	GapRecovery func()
	// Log is an optional logger.
	Log *Logger
}

// NewInboundUpdateQueue creates a queue. When cfg.MaxDepth == 0, the returned
// queue is disabled (Enabled() == false) and callers dispatch inline.
func NewInboundUpdateQueue(cfg InboundQueueConfig) *InboundUpdateQueue {
	q := &InboundUpdateQueue{
		enabled:     cfg.MaxDepth > 0,
		maxDepth:    cfg.MaxDepth,
		stallBudget: cfg.StallBudget,
		gapRecovery: cfg.GapRecovery,
		log:         cfg.Log,
		done:        make(chan struct{}),
	}
	if !q.enabled {
		return q
	}
	if q.workers = cfg.Workers; q.workers <= 0 {
		q.workers = runtime.NumCPU()
	}
	if q.workers > cfg.MaxDepth {
		q.workers = cfg.MaxDepth
	}
	if q.workers < 1 {
		q.workers = 1
	}
	if q.stallBudget <= 0 {
		q.stallBudget = 500 * time.Millisecond
	}
	// Distribute maxDepth across shards (at least 1 per shard).
	perShard := q.maxDepth / q.workers
	if perShard < 1 {
		perShard = 1
	}
	q.shards = make([]chan inboundQueueItem, q.workers)
	for i := range q.shards {
		q.shards[i] = make(chan inboundQueueItem, perShard)
		q.workerWG.Add(1)
		go q.worker(i)
	}
	return q
}

// Enabled reports whether the queue is active (MaxDepth > 0). When false,
// callers MUST fall back to synchronous inline dispatch.
func (q *InboundUpdateQueue) Enabled() bool { return q.enabled }

// Enqueue offers a work item for dispatch, keyed by routingKey for ordering.
//
// Implements the FR-004 hybrid overflow policy:
//   - If the target shard is not full: non-blocking insert, returns nil.
//   - If full: blocks up to StallBudget for a slot. If a slot opens, inserts.
//   - If StallBudget exceeded: applies overflow policy — sheds the incoming
//     update (drops it), increments OverflowCount, and invokes GapRecovery so
//     the shed is recoverable via getDifference. Returns nil (the shed is
//     handled, not surfaced as an error to the producer).
//
// Returns ErrUpdateHandlerFailed only if the queue is closed during enqueue.
func (q *InboundUpdateQueue) Enqueue(routingKey uint64, work func()) error {
	if !q.enabled {
		// Disabled: caller should run inline, but provide a safe fallback.
		work()
		return nil
	}
	shard := q.shards[q.shardFor(routingKey)]
	cur := q.depth.Add(1)
	q.trackHighWater(cur)

	// Fast path: non-blocking send.
	select {
	case shard <- inboundQueueItem{routingKey: routingKey, work: work}:
		return nil
	default:
	}

	// Slow path: block up to StallBudget for a slot.
	timer := time.NewTimer(q.stallBudget)
	defer timer.Stop()
	select {
	case shard <- inboundQueueItem{routingKey: routingKey, work: work}:
		return nil
	case <-q.done:
		q.depth.Add(-1)
		return ErrUpdateHandlerFailed
	case <-timer.C:
		// Stall budget exceeded: shed + recover.
		q.depth.Add(-1)
		q.overflow.Add(1)
		if q.gapRecovery != nil {
			q.gapRecovery()
		}
		if q.log != nil {
			q.log.Warnf("inbound queue: shed update (routing_key=%d) and triggered gap recovery", routingKey)
		}
		return nil
	}
}

func (q *InboundUpdateQueue) worker(idx int) {
	defer q.workerWG.Done()
	shard := q.shards[idx]
	for {
		select {
		case item, ok := <-shard:
			if !ok {
				return
			}
			q.runSafe(item.work)
			q.depth.Add(-1)
		case <-q.done:
			// Shutdown: drain any items still queued in this shard so no
			// enqueued work is lost during close, then exit.
			q.drainShard(shard)
			return
		}
	}
}

// drainShard processes all items remaining in shard, then returns when empty.
func (q *InboundUpdateQueue) drainShard(shard chan inboundQueueItem) {
	for {
		select {
		case item, ok := <-shard:
			if !ok {
				return
			}
			q.runSafe(item.work)
			q.depth.Add(-1)
		default:
			return
		}
	}
}

// runSafe executes work with panic recovery so a panicking handler cannot kill
// the worker pool or corrupt dispatch state.
func (q *InboundUpdateQueue) runSafe(work func()) {
	defer func() {
		if r := recover(); r != nil {
			if q.log != nil {
				q.log.Errorf("inbound queue: dispatch panic recovered: %v", r)
			}
		}
	}()
	work()
}

func (q *InboundUpdateQueue) shardFor(key uint64) int {
	if q.workers == 1 {
		return 0
	}
	h := fnv.New64a()
	var buf [8]byte
	for i := 0; i < 8; i++ {
		buf[i] = byte(key >> (i * 8))
	}
	h.Write(buf[:])
	return int(h.Sum64() % uint64(q.workers))
}

func (q *InboundUpdateQueue) trackHighWater(cur int64) {
	for {
		old := q.highWater.Load()
		if cur <= old {
			break
		}
		if q.highWater.CompareAndSwap(old, cur) {
			break
		}
	}
}

// InboundSnapshot is a read-only view of queue state for introspection (FR-020).
type InboundSnapshot struct {
	Depth         int
	HighWater     int
	OverflowCount int64
	Workers       int
}

// Snapshot returns a read-only view of queue state. Safe for concurrent use.
func (q *InboundUpdateQueue) Snapshot() InboundSnapshot {
	return InboundSnapshot{
		Depth:         int(q.depth.Load()),
		HighWater:     int(q.highWater.Load()),
		OverflowCount: q.overflow.Load(),
		Workers:       q.workers,
	}
}

// Close drains in-flight work and stops all workers. Idempotent. Blocks until
// every worker has exited (no goroutine leak — Constitution Principle V).
func (q *InboundUpdateQueue) Close() error {
	if !q.enabled {
		return nil
	}
	q.closeOnce.Do(func() {
		close(q.done)
	})
	q.workerWG.Wait()
	return nil
}
