package session

import (
	"sync"
)

// ContainerTracker tracks which queries are in which containers.
// Ported from td/td/telegram/net/Session.h:82-104 (Query with container_message_id_).
type ContainerTracker struct {
	containers map[int64]*ContainerEntry
	logger     sessionLogger
	mu         sync.Mutex
}

// SetLogger sets the logger for the container tracker.
func (t *ContainerTracker) SetLogger(l sessionLogger) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = l
}

// ContainerEntry tracks a container and its children.
type ContainerEntry struct {
	ContainerMsgID int64
	ChildMsgIDs    []int64
	RefCount       int
}

// NewContainerTracker creates a new container tracker.
func NewContainerTracker() *ContainerTracker {
	return &ContainerTracker{
		containers: make(map[int64]*ContainerEntry),
	}
}

// TrackContainer registers a container with its children.
func (t *ContainerTracker) TrackContainer(containerMsgID int64, childMsgIDs []int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.containers[containerMsgID] = &ContainerEntry{
		ContainerMsgID: containerMsgID,
		ChildMsgIDs:    childMsgIDs,
		RefCount:       len(childMsgIDs),
	}
	if t.logger != nil {
		t.logger.Warnf("container tracked container_msg_id=%d children=%d", containerMsgID, len(childMsgIDs))
	}
}

// AckContainer ACKs all children of a container. Returns the child message IDs.
func (t *ContainerTracker) AckContainer(containerMsgID int64) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.containers[containerMsgID]
	if !ok {
		return nil
	}
	childIDs := entry.ChildMsgIDs
	delete(t.containers, containerMsgID)
	return childIDs
}

// NackContainer NACKs all children of a container. Returns the child message IDs.
func (t *ContainerTracker) NackContainer(containerMsgID int64) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.containers[containerMsgID]
	if !ok {
		return nil
	}
	childIDs := entry.ChildMsgIDs
	if t.logger != nil {
		t.logger.Warnf("container nacked container_msg_id=%d children=%d", containerMsgID, len(childIDs))
	}
	delete(t.containers, containerMsgID)
	return childIDs
}

// AckChild ACKs a single child, decrementing the ref count.
// Returns true if the container is now fully ACKed.
func (t *ContainerTracker) AckChild(childMsgID int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, entry := range t.containers {
		for _, id := range entry.ChildMsgIDs {
			if id == childMsgID {
				entry.RefCount--
				if t.logger != nil {
					t.logger.Warnf("container child acked child_msg_id=%d container_msg_id=%d remaining=%d", childMsgID, entry.ContainerMsgID, entry.RefCount)
				}
				if entry.RefCount <= 0 {
					delete(t.containers, entry.ContainerMsgID)
					return true
				}
				return false
			}
		}
	}
	return false
}

// Cleanup removes all entries (called on session close).
func (t *ContainerTracker) Cleanup() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.containers = make(map[int64]*ContainerEntry)
}

// Count returns the number of tracked containers.
func (t *ContainerTracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.containers)
}
