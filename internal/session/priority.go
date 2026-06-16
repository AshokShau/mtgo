package session

import (
	"fmt"
	"strings"

	"github.com/mtgo-labs/mtgo/tg"
)

// Priority classifies an outbound RPC for send ordering and overload admission.
// High-priority (interactive) RPCs are sent before low-priority (bulk) RPCs
// and receive bounded deferred admission under overload; low-priority RPCs are
// throttled first and fast-fail under overload.
//
// Ported from TDLib NetQuery::Priority (net/NetQuery.h).
type Priority int

const (
	// PriorityLow is for bulk/background RPCs: upload.*, bulk reads,
	// messages.getHistory, etc. Throttled first on overload; fast-fails at
	// capacity.
	PriorityLow Priority = iota
	// PriorityHigh is for interactive RPCs: messages.send*, account.*, auth.*,
	// presence, etc. Sent ahead of low-priority traffic; receives bounded
	// deferred admission under overload.
	PriorityHigh
)

// RoutePriority classifies a TL query into a Priority using method-name
// heuristics mirroring TDLib's NetQuery::Priority. It inspects the Go type
// name of the query. Unknown methods default to PriorityHigh (safe default —
// never starve a caller's interactive request).
func RoutePriority(query tg.TLObject) Priority {
	if query == nil {
		return PriorityHigh
	}
	return classifyGoType(fmt.Sprintf("%T", query))
}

// classifyGoType inspects a Go type string like "*tg.UploadGetFileRequest" or
// "*tg.MessagesGetHistoryRequest" and returns the priority. The classification
// uses prefixes on the stripped type name.
func classifyGoType(typeStr string) Priority {
	// Strip package prefix: "*tg.UploadGetFileRequest" → "UploadGetFileRequest".
	name := typeStr
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimPrefix(name, "*")
	// Strip trailing "Request" for cleaner prefix matching.
	base := strings.TrimSuffix(name, "Request")

	switch {
	case strings.HasPrefix(base, "Upload"):
		return PriorityLow
	case strings.HasPrefix(base, "MessagesGetDialog"),
		strings.HasPrefix(base, "MessagesGetHistory"),
		strings.HasPrefix(base, "MessagesSearch"),
		strings.HasPrefix(base, "MessagesGetReplies"),
		strings.HasPrefix(base, "MessagesGetMessagesRange"),
		strings.HasPrefix(base, "MessagesGetSearchCounters"),
		strings.HasPrefix(base, "MessagesGetExtendedMedia"),
		strings.HasPrefix(base, "MessagesGetWebPagePreview"):
		return PriorityLow
	default:
		return PriorityHigh
	}
}
