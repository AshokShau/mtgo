package telegram

import (
	"testing"
	"time"
)

func TestPerDCBackoff(t *testing.T) {
	backoff := NewPerDCBackoff(time.Second, 4*time.Second)

	backoff.RecordFailure(2)
	if got := backoff.GetDelay(2); got != 2*time.Second {
		t.Fatalf("dc2 delay after first failure = %v, want 2s", got)
	}
	if !backoff.ShouldRetry(4) {
		t.Fatal("dc4 ShouldRetry() = false, want true")
	}
	if backoff.ShouldRetry(2) {
		t.Fatal("dc2 ShouldRetry() = true immediately after failure, want false")
	}

	backoff.RecordFailure(2)
	backoff.RecordFailure(2)
	if got := backoff.GetDelay(2); got != 4*time.Second {
		t.Fatalf("dc2 delay cap = %v, want 4s", got)
	}

	backoff.RecordSuccess(2)
	if got := backoff.GetDelay(2); got != time.Second {
		t.Fatalf("dc2 delay after success = %v, want 1s", got)
	}
	if !backoff.ShouldRetry(2) {
		t.Fatal("dc2 ShouldRetry() after success = false, want true")
	}
}
