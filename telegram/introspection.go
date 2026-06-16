package telegram

import (
	"time"
)

// LoadSnapshot is a read-only runtime snapshot of client load for operator
// observability (FR-020). All fields are populated from atomic reads with zero
// hot-path allocation. Safe for concurrent use.
type LoadSnapshot struct {
	InboundQueueDepth int
	InboundHighWater  int
	OutboundHighDepth int
	OutboundLowDepth  int
	InFlightRPCs      int64
	ContainersSent    int64
	MessagesPacked    int64
	ThrottleLevel     ThrottleLevel
	OverloadActive    bool
}

// LoadSnapshot returns an aggregated view of the client's internal load state.
// When production-hardening features are disabled (all config zero), most fields
// will be zero — this is expected (backward compat, Principle IV).
func (c *Client) LoadSnapshot() LoadSnapshot {
	snap := LoadSnapshot{}

	c.mu.RLock()
	um := c.updateManager
	sess := c.session
	oc := c.overloadController
	c.mu.RUnlock()

	// Inbound queue depth (from update manager's inbound queue).
	if um != nil && um.cfg.InboundQueue != nil {
		is := um.cfg.InboundQueue.Snapshot()
		snap.InboundQueueDepth = is.Depth
		snap.InboundHighWater = is.HighWater
	}

	// Outbound batcher metrics (from session).
	if sess != nil {
		os := sess.BatcherSnapshot()
		snap.OutboundHighDepth = os.HighDepth
		snap.OutboundLowDepth = os.LowDepth
		snap.ContainersSent = os.ContainersSent
		snap.MessagesPacked = os.MessagesPacked
	}

	// Overload controller.
	if oc != nil {
		os := oc.Snapshot()
		snap.InFlightRPCs = os.InFlight
		snap.ThrottleLevel = os.ThrottleLevel
		snap.OverloadActive = os.OverloadActive
	}

	return snap
}

// Ensure time import is used for potential future latency fields.
var _ = time.Second
