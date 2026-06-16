package session

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type testSessionLogger struct {
	warns []string
}

func (l *testSessionLogger) Debugf(string, ...any) {}

func (l *testSessionLogger) Warnf(format string, v ...any) {
	l.warns = append(l.warns, sprintf(format, v...))
}

func (l *testSessionLogger) Errorf(string, ...any) {}

func sprintf(format string, v ...any) string {
	return strings.TrimSpace(fmt.Sprintf(format, v...))
}

func TestContainerTracker(t *testing.T) {
	tracker := NewContainerTracker()
	tracker.TrackContainer(1001, []int64{2001, 2002, 2003})

	got := tracker.AckContainer(1001)
	want := []int64{2001, 2002, 2003}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AckContainer() = %v, want %v", got, want)
	}
	if got := tracker.AckContainer(1001); len(got) != 0 {
		t.Fatalf("AckContainer() after removal = %v, want empty", got)
	}
}

func TestContainerTrackerPartialAckAndNack(t *testing.T) {
	tracker := NewContainerTracker()
	tracker.TrackContainer(1001, []int64{2001, 2002})

	tracker.AckChild(2001)
	got := tracker.NackContainer(1001)
	want := []int64{2001, 2002}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NackContainer() = %v, want %v", got, want)
	}

	tracker.TrackContainer(1002, []int64{3001})
	tracker.AckChild(3001)
	if got := tracker.NackContainer(1002); len(got) != 0 {
		t.Fatalf("NackContainer() after child ack = %v, want empty", got)
	}
}

func TestContainerTrackerLogsEvents(t *testing.T) {
	tracker := NewContainerTracker()
	logger := &testSessionLogger{}
	tracker.SetLogger(logger)

	tracker.TrackContainer(1001, []int64{2001, 2002})
	tracker.AckChild(2001)
	tracker.NackContainer(1001)

	joined := strings.Join(logger.warns, "\n")
	for _, want := range []string{
		"container tracked container_msg_id=1001 children=2",
		"container child acked child_msg_id=2001 container_msg_id=1001 remaining=1",
		"container nacked container_msg_id=1001 children=2",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("logs = %q, want %q", joined, want)
		}
	}
}
