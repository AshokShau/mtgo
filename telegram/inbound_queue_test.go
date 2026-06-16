package telegram

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInboundQueue_DisabledDispatchesInline(t *testing.T) {
	var ran atomic.Int32
	q := NewInboundUpdateQueue(InboundQueueConfig{MaxDepth: 0})
	if q.Enabled() {
		t.Fatal("MaxDepth==0 queue should be disabled")
	}
	if err := q.Enqueue(1, func() { ran.Add(1) }); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if ran.Load() != 1 {
		t.Fatalf("inline dispatch: want 1, got %d", ran.Load())
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestInboundQueue_NoLoss(t *testing.T) {
	const n = 5000
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    1000,
		Workers:     4,
		StallBudget: 10 * time.Second, // large budget so producer always waits, never sheds
	})

	var delivered atomic.Int64
	for i := 0; i < n; i++ {
		if err := q.Enqueue(uint64(i%8), func() { delivered.Add(1) }); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := delivered.Load(); got != n {
		t.Fatalf("NoLoss: delivered %d, want %d", got, n)
	}
}

func TestInboundQueue_ExactlyOnce(t *testing.T) {
	const n = 2000
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    500,
		Workers:     4,
		StallBudget: 10 * time.Second, // large budget so producer never sheds
	})

	seen := make([]bool, n)
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		idx := i
		if err := q.Enqueue(uint64(idx), func() {
			mu.Lock()
			if seen[idx] {
				t.Errorf("update %d delivered twice", idx)
			}
			seen[idx] = true
			mu.Unlock()
		}); err != nil {
			t.Fatalf("enqueue %d: %v", idx, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i, s := range seen {
		if !s {
			t.Fatalf("update %d never delivered", i)
		}
	}
}

func TestInboundQueue_BoundedUnderSlowHandler(t *testing.T) {
	// A slow handler that processes one update every ~1ms, with a tiny queue
	// and short stall budget so the queue saturates quickly.
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    8,
		Workers:     1,
		StallBudget: 5 * time.Millisecond,
	})

	// Fire a burst that exceeds capacity; the overflow policy should keep
	// memory bounded. We don't assert exact delivery count (some will shed),
	// only that HighWater stays within the configured bound and Close returns.
	stop := make(chan struct{})
	var produced atomic.Int64
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = q.Enqueue(1, func() { time.Sleep(time.Millisecond) })
				produced.Add(1)
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	close(stop)

	snap := q.Snapshot()
	// HighWater is bounded by maxDepth (with small slack for in-flight).
	maxAllowed := 8 + 8 // maxDepth + workers
	if snap.HighWater > maxAllowed {
		t.Fatalf("HighWater %d exceeds bound %d", snap.HighWater, maxAllowed)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestInboundQueue_OverflowShedsAndRecovers(t *testing.T) {
	var recovered atomic.Int64
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    4,
		Workers:     1,
		StallBudget: 2 * time.Millisecond,
		GapRecovery: func() { recovered.Add(1) },
	})

	// Block the single worker so the queue fills and subsequent enqueues shed.
	block := make(chan struct{})
	for i := 0; i < 4; i++ {
		if err := q.Enqueue(0, func() { <-block }); err != nil {
			t.Fatalf("fill enqueue %d: %v", i, err)
		}
	}
	// The queue is now full (4 items, worker blocked). Enqueue more — these
	// should shed after the stall budget and trigger gap recovery.
	for i := 0; i < 10; i++ {
		_ = q.Enqueue(0, func() {})
	}

	close(block) // unblock the worker
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if recovered.Load() == 0 {
		t.Fatal("expected gap recovery to be triggered on overflow, got 0")
	}
	snap := q.Snapshot()
	if snap.OverflowCount == 0 {
		t.Fatal("expected OverflowCount > 0")
	}
}

func TestInboundQueue_OrderingWithinKey(t *testing.T) {
	const n = 200
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    n,
		Workers:     4,
		StallBudget: 10 * time.Second, // large budget so all items are delivered
	})

	var mu sync.Mutex
	var seq []int
	for i := 0; i < n; i++ {
		idx := i
		// All same routing key → same shard → FIFO order preserved.
		if err := q.Enqueue(42, func() {
			mu.Lock()
			seq = append(seq, idx)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("enqueue %d: %v", idx, err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seq) != n {
		t.Fatalf("delivered %d, want %d", len(seq), n)
	}
	for i, v := range seq {
		if v != i {
			t.Fatalf("ordering broken at index %d: got %d, want %d", i, v, i)
		}
	}
}

func TestInboundQueue_ConcurrentAcrossKeys(t *testing.T) {
	// With multiple workers, different keys can dispatch concurrently. We
	// verify by having a handler that sleeps briefly; if dispatch were serial
	// the total time would be ~keys*sleep, but concurrent is ~sleep.
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    100,
		Workers:     8,
		StallBudget: 100 * time.Millisecond,
	})

	const keys = 8
	const sleep = 30 * time.Millisecond
	start := time.Now()
	for i := 0; i < keys; i++ {
		k := uint64(i)
		if err := q.Enqueue(k, func() { time.Sleep(sleep) }); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	elapsed := time.Since(start)
	// Serial would be keys*sleep = 240ms; concurrent with 8 workers ~30ms.
	// Allow generous slack for scheduling but require clear concurrency.
	if elapsed > time.Duration(keys-1)*sleep {
		t.Fatalf("dispatch appears serial: elapsed %v > %v", elapsed, time.Duration(keys-1)*sleep)
	}
}

func TestInboundQueue_HandlerPanicIsolated(t *testing.T) {
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    100,
		Workers:     1,
		StallBudget: 10 * time.Second, // large budget so all items are delivered
	})

	var delivered atomic.Int32
	// First handler panics; subsequent handlers must still run.
	if err := q.Enqueue(1, func() { panic("boom") }); err != nil {
		t.Fatalf("enqueue panic: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(1, func() { delivered.Add(1) }); err != nil {
			t.Fatalf("enqueue after panic: %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := delivered.Load(); got != 5 {
		t.Fatalf("after panic: delivered %d, want 5", got)
	}
}

func TestInboundQueue_NoLeak(t *testing.T) {
	// Verify no goroutine leak after start/exercise/close.
	before := runtime.NumGoroutine()
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    50,
		Workers:     4,
		StallBudget: 20 * time.Millisecond,
	})
	for i := 0; i < 100; i++ {
		_ = q.Enqueue(uint64(i), func() {})
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Give workers a moment to fully exit.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	if leaked := after - before; leaked > 0 {
		t.Fatalf("goroutine leak: before=%d after=%d (leaked %d)", before, after, leaked)
	}
}

func BenchmarkInboundQueue_1000PerSec(b *testing.B) {
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth:    4096,
		Workers:     runtime.NumCPU(),
		StallBudget: 100 * time.Millisecond,
	})
	var done atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = q.Enqueue(uint64(i), func() { done.Add(1) })
			i++
		}
	})
	if err := q.Close(); err != nil {
		b.Fatal(err)
	}
	if got := done.Load(); got != int64(b.N) {
		b.Fatalf("delivered %d, want %d (some enqueues may have shed under pressure)", got, b.N)
	}
	b.ReportMetric(float64(done.Load())/b.Elapsed().Seconds(), "updates/sec")
}

// Smoke test the shard distribution to ensure keys spread across workers.
func TestInboundQueue_ShardDistribution(t *testing.T) {
	q := NewInboundUpdateQueue(InboundQueueConfig{
		MaxDepth: 1000,
		Workers:  4,
	})
	// All keys go to the same shard (key=1) — verify workers > 1 configured.
	if q.workers < 2 {
		t.Fatalf("expected >=2 workers, got %d", q.workers)
	}
	// Different keys should (probabilistically) land on different shards.
	seen := map[int]bool{}
	for k := uint64(0); k < 1000; k++ {
		seen[q.shardFor(k)] = true
	}
	if len(seen) < 2 {
		t.Fatalf("poor shard distribution: only %d distinct shards used", len(seen))
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	_ = fmt.Sprintf // keep fmt import if future assertions need it
}
