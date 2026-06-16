package session

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/internal/crypto"
	"github.com/mtgo-labs/mtgo/tg"
)

func TestFloodWaitHandling(t *testing.T) {
	wait, ok := parseFloodWait("FLOOD_WAIT_5")
	if !ok {
		t.Fatal("parseFloodWait() ok = false, want true")
	}
	if wait != 5*time.Second {
		t.Fatalf("parseFloodWait() = %v, want 5s", wait)
	}
	if _, ok := parseFloodWait("PHONE_CODE_INVALID"); ok {
		t.Fatal("parseFloodWait(non-flood) ok = true, want false")
	}
}

func TestFloodWaitQueue(t *testing.T) {
	q := &FloodWaitQueue{}
	query := &tg.PingRequest{PingID: 1}
	q.Delay(query, 10, -time.Millisecond)

	ready := q.Ready()
	if len(ready) != 1 {
		t.Fatalf("Ready() len = %d, want 1", len(ready))
	}
	if ready[0].Query != query || ready[0].MsgID != 10 {
		t.Fatalf("Ready()[0] = %#v, want query/msg_id", ready[0])
	}
	if ready := q.Ready(); len(ready) != 0 {
		t.Fatalf("Ready() after drain len = %d, want 0", len(ready))
	}
	q.Delay(query, 11, time.Hour)
	q.Cleanup()
	if ready := q.Ready(); len(ready) != 0 {
		t.Fatalf("Ready() after cleanup len = %d, want 0", len(ready))
	}
}

func TestFloodWaitHandlingRetriesAndLogs(t *testing.T) {
	s := newSessionWithAuthKey(t)
	mt := newMockTransport()
	s.SetTransport(mt)
	logger := &testSessionLogger{}
	s.SetLogger(logger)

	cleanup := startTestWorkers(s, mt)
	defer cleanup()

	pingID := time.Now().UnixNano()
	done := make(chan struct {
		obj tg.TLObject
		err error
	}, 1)
	go func() {
		obj, err := s.Invoke(context.Background(), &tg.PingRequest{PingID: pingID}, 1, 5*time.Second)
		done <- struct {
			obj tg.TLObject
			err error
		}{obj: obj, err: err}
	}()

	firstSend := <-mt.sendCh
	first, _, err := crypto.Unpack(firstSend, s.sessionIDBytes(), s.authKey, s.authKeyID)
	if err != nil {
		t.Fatalf("unpack first send: %v", err)
	}
	sendRPCResult(t, s, mt, first.MsgID, &tg.RPCError{
		ErrorCode:    420,
		ErrorMessage: "FLOOD_WAIT_0",
	})

	secondSend := <-mt.sendCh
	second, _, err := crypto.Unpack(secondSend, s.sessionIDBytes(), s.authKey, s.authKeyID)
	if err != nil {
		t.Fatalf("unpack second send: %v", err)
	}
	sendRPCResult(t, s, mt, second.MsgID, &tg.Pong{
		MsgID:  second.MsgID,
		PingID: pingID,
	})

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("Invoke() error: %v", result.err)
		}
		if _, ok := result.obj.(*tg.Pong); !ok {
			t.Fatalf("Invoke() result = %T, want *tg.Pong", result.obj)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Invoke() timed out")
	}

	joined := strings.Join(logger.warns, "\n")
	for _, want := range []string{
		"flood wait detected",
		"flood wait delayed",
		"flood wait retry",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("logs = %q, want %q", joined, want)
		}
	}
}

func sendRPCResult(t *testing.T, s *Session, mt *mockTransport, reqMsgID int64, result tg.TLObject) {
	t.Helper()
	var body bytes.Buffer
	tg.WriteInt(&body, tg.RPCResultTypeID)
	tg.WriteLong(&body, reqMsgID)
	if err := tg.EncodeTLObject(&body, result); err != nil {
		t.Fatalf("encode rpc result: %v", err)
	}
	mt.recvCh <- makeEncryptedRawResponse(s, makeServerMsgID(), uint32(s.msgFactory.AllocateSeqNo(false)), body.Bytes())
}
