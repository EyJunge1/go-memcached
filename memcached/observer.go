package memcached

import (
	"context"
	"time"
)

// Stable OpEvent.Op vocabulary. Single-command ops use the wire verb; batch and
// ops-plane names are listed below. Observers and exporters should match these
// strings (prefer the Op* constants) rather than inventing aliases.
const (
	OpGet          = "get"
	OpGets         = "gets"
	OpGat          = "gat"
	OpGats         = "gats"
	OpSet          = "set"
	OpAdd          = "add"
	OpReplace      = "replace"
	OpAppend       = "append"
	OpPrepend      = "prepend"
	OpCas          = "cas"
	OpDelete       = "delete"
	OpTouch        = "touch"
	OpIncr         = "incr"
	OpDecr         = "decr"
	OpMg           = "mg"
	OpMs           = "ms"
	OpMd           = "md"
	OpMa           = "ma"
	OpMe           = "me"
	OpMn           = "mn"
	OpStats        = "stats"
	OpVersion      = "version"
	OpFlushAll     = "flush_all"
	OpWatch        = "watch"
	OpMetadump     = "metadump"
	OpMgdump       = "mgdump"
	OpPipeline     = "pipeline"
	OpMetaPipeline = "meta_pipeline"
	OpDiscovery    = "discovery"
	OpUnknown      = "unknown"
)

type (
	// Observer receives operation timing after each logical client call.
	// Implementations must be non-blocking; nil Config.Observer disables observation.
	// OpenTelemetry adapters live in a separate module so this package stays dep-free.
	Observer interface {
		ObserveOp(ctx context.Context, e OpEvent)
	}

	// Attempt is one transport try within a logical operation (including the first).
	Attempt struct {
		Server   Server
		Duration time.Duration
		Err      error
	}

	// OpEvent is one observed client operation (single command or pipeline run).
	OpEvent struct {
		// Op is the command or batch name; see Op* constants for the stable vocabulary.
		Op string
		// Server is the primary node when known; zero when routing failed early.
		Server Server
		// Duration covers the full logical call including retries and fanout wait.
		Duration time.Duration
		// Err is the returned call error (nil on success).
		Err error
		// Keys is the number of keys or queued ops, not key contents.
		Keys int
		// Hits / Misses count retrieval outcomes when known (Get/GetOne); zero otherwise.
		Hits   int
		Misses int
		// RequestBytes is approximate outbound payload size (wire body / stored value).
		RequestBytes int64
		// ResponseBytes is approximate inbound value bytes on retrievals.
		ResponseBytes int64
		// Key is an optional single key for opt-in exporters. The core may set it
		// for single-key calls; exporters must not record it unless explicitly enabled.
		Key []byte
		// Attempts lists each transport try for dispatch paths that retry.
		// Empty when the observer sits above fanout/pipeline aggregation.
		// Retries() is max(0, len(Attempts)-1).
		Attempts []Attempt
		// ConfigID is set for OpDiscovery (Memorystore) when known.
		ConfigID int64
	}
)

// Retries returns how many times the op was retried after the first attempt.
func (e OpEvent) Retries() int {
	if len(e.Attempts) <= 1 {
		return 0
	}
	return len(e.Attempts) - 1
}

func (c *Client) observer() Observer {
	if c == nil || c.rt == nil {
		return nil
	}
	return c.rt.observer
}

func (c *Client) observeOp(ctx context.Context, start time.Time, op string, node *addr, keys int, err error) {
	c.observeEvent(ctx, OpEvent{
		Op:       op,
		Server:   serverFromAddr(node),
		Duration: time.Since(start),
		Err:      err,
		Keys:     keys,
	})
}

func (c *Client) observeEvent(ctx context.Context, e OpEvent) {
	obs := c.observer()
	if obs == nil {
		return
	}
	// Defensive copies: Observer may retain OpEvent slices after return.
	if n := len(e.Attempts); n > 0 {
		e.Attempts = append([]Attempt(nil), e.Attempts...)
	}
	if n := len(e.Key); n > 0 {
		e.Key = append([]byte(nil), e.Key...)
	}
	obs.ObserveOp(ctx, e)
}

func (c *Client) observeDispatch(
	ctx context.Context, start time.Time, node *addr, req *request, err error, attempts []Attempt,
) {
	if observeSkipped(ctx) {
		return
	}
	c.observeEvent(ctx, OpEvent{
		Op:           opName(req),
		Server:       serverFromAddr(node),
		Duration:     time.Since(start),
		Err:          err,
		Keys:         1,
		RequestBytes: requestWireBytes(req),
		Attempts:     attempts,
	})
}

type ctxKeyObserveSkip struct{}

// withObserveSkip marks ctx so nested dispatchToAddr does not emit OpEvents.
// Used when a higher-level call (Get, pipeline, broadcast) already observes.
func withObserveSkip(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyObserveSkip{}, true /* present */)
}

func observeSkipped(ctx context.Context) bool {
	return ctx.Value(ctxKeyObserveSkip{}) != nil
}

func serverFromAddr(a *addr) Server {
	if a == nil {
		return Server{}
	}
	return Server{Network: a.Network, Address: a.Address}
}

func opName(req *request) string {
	if req == nil || len(req.cmd) == 0 {
		return OpUnknown
	}
	return string(req.cmd)
}

func requestWireBytes(req *request) int64 {
	if req == nil {
		return 0
	}
	return int64(len(req.raw) + len(req.body) + len(req.tail))
}
