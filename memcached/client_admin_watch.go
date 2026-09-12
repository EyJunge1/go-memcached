package memcached

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const _adminStreamLineLimit = 64 * 1024

// WatchTopic is a closed set of watch stream names. Other strings are rejected.
type WatchTopic string

// WatchFetchers and the constants below are the accepted watch stream names.
const (
	WatchFetchers    WatchTopic = "fetchers"
	WatchMutations   WatchTopic = "mutations"
	WatchEvictions   WatchTopic = "evictions"
	WatchConnEvents  WatchTopic = "connevents"
	WatchProxyReqs   WatchTopic = "proxyreqs"
	WatchProxyEvents WatchTopic = "proxyevents"
	WatchProxyUser   WatchTopic = "proxyuser"
	WatchDeletions   WatchTopic = "deletions"
)

// WatchEvent is one watcher log line. Err is set when the stream ends in error.
type WatchEvent struct {
	TS  time.Time
	GID uint64
	Raw map[string]string
	Err error
}

// DumpEntry is one metadump or mgdump line. Err is set when the stream ends in error.
type DumpEntry struct {
	Fields map[string]string
	Line   string
	Err    error
}

// WatchStream is a live watcher feed on a dedicated connection.
// Cancel the parent context or call Close to stop; Close waits for the reader
// goroutine to exit. Always call Close when finished.
type WatchStream struct {
	// Events receives watcher lines until the stream ends; it is closed when
	// the reader exits.
	Events <-chan WatchEvent

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// Close cancels the stream and waits for the background reader to exit.
// It is safe to call more than once.
func (s *WatchStream) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		s.wg.Wait()
	})
}

// sendAdminLine writes one admin command on a dedicated connection and maps the status reply.
func (a *AdminNode) sendAdminLine(
	ctx context.Context,
	cn wireConn,
	req *request,
	resp *response,
) error {
	c, err := a.boundClient()
	if err != nil {
		return err
	}
	c.enableUDPFraming(nil, req, resp)
	if err := req.send(ctx, cn, c.rt.writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("send: %w", err))
	}
	if err := resp.recv(ctx, cn, c.rt.readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("recv: %w", err))
	}
	return parseAdminReply(resp)
}

// acquireStream dials a dedicated socket for watch or dump; ctx must carry a deadline.
func (a *AdminNode) acquireStream(ctx context.Context) (wireConn, error) {
	if err := requireDeadline(ctx); err != nil {
		return nil, err
	}
	addr, c, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	cn, err := c.dialDedicated(ctx, addr)
	if err != nil {
		return nil, wrapTransport(err)
	}
	return cn, nil
}

// Watch opens a watcher stream on the bound node.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
// Call Close on the returned stream to wait for the reader goroutine.
func (a *AdminNode) Watch(ctx context.Context, topics ...WatchTopic) (*WatchStream, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("watch: %w", ErrInvalidArgument)
	}
	for _, t := range topics {
		if !allowedWatchTopic(t) {
			return nil, fmt.Errorf("watch topic %q: %w", t, ErrInvalidArgument)
		}
	}
	start := time.Now()
	addr, c, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	cn, err := a.acquireStream(ctx)
	if err != nil {
		c.observeOp(ctx, start, OpWatch, addr, len(topics), err)
		return nil, err
	}

	req, resp := buildWatchCommand(topics)
	if err = a.sendAdminLine(ctx, cn, req, resp); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		c.observeOp(ctx, start, OpWatch, addr, len(topics), err)
		return nil, err
	}
	releaseReqAndResp(req, resp)

	streamCtx, cancel := context.WithCancel(ctx)
	// Size 1: brief consumer stall without unbounded buffering (Uber: 0 or 1).
	out := make(chan WatchEvent, 1)
	s := &WatchStream{Events: out, cancel: cancel}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		a.readWatch(streamCtx, c, cn, out, start, addr, len(topics))
	}()
	return s, nil
}

// readWatch pumps watcher log lines into out until ctx ends or the stream fails.
func (a *AdminNode) readWatch(
	ctx context.Context,
	c *Client,
	cn wireConn,
	out chan<- WatchEvent,
	start time.Time,
	node *addr,
	keys int,
) {
	var streamErr error
	defer func() {
		if c != nil {
			c.observeOp(ctx, start, OpWatch, node, keys, streamErr)
		}
	}()
	defer close(out)
	defer releaseDedicated(cn)
	stop := context.AfterFunc(ctx, func() { _ = cn.Close() }) // best-effort: cancel closes stream
	defer stop()

	for {
		if err := ctx.Err(); err != nil {
			streamErr = err
			return
		}
		_ = cn.setReadDeadline(time.Time{}) // best-effort: clear before ctx deadline
		if dl, ok := ctx.Deadline(); ok {
			_ = cn.setReadDeadline(dl) // best-effort: stream read bound
		}
		line, err := readAdminStreamLine(cn)
		if err != nil {
			if ctx.Err() != nil {
				streamErr = ctx.Err()
				return
			}
			streamErr = wrapRecvError(err)
			select {
			case out <- WatchEvent{Err: streamErr}:
			case <-ctx.Done():
			}
			return
		}
		trimmed := trimCRLF(line)
		if err = adminStreamLineError(trimmed); err != nil {
			streamErr = err
			select {
			case out <- WatchEvent{Err: err}:
			case <-ctx.Done():
			}
			return
		}
		ev := parseWatchEvent(string(trimmed))
		select {
		case out <- ev:
		case <-ctx.Done():
			streamErr = ctx.Err()
			return
		}
	}
}

// releaseDedicated closes an unpooled admin-stream socket.
func releaseDedicated(cn wireConn) {
	if cn == nil {
		return
	}
	_ = cn.Close()
}

// readAdminStreamLine reads one watcher or dump line, capped at _adminStreamLineLimit.
func readAdminStreamLine(cn wireConn) ([]byte, error) {
	var buf []byte
	var tmp [1]byte
	for {
		n, err := cn.Read(tmp[:])
		if n == 1 {
			buf = append(buf, tmp[0])
			if tmp[0] == '\n' {
				return buf, nil
			}
			if len(buf) > _adminStreamLineLimit {
				return nil, fmt.Errorf("admin line too long: %w", ErrMalformedResponse)
			}
		}
		if err != nil {
			if len(buf) > 0 && err == io.EOF {
				return buf, io.EOF
			}
			return buf, err
		}
	}
}

// parseWatchEvent decodes one URI-encoded watcher line into a WatchEvent.
func parseWatchEvent(line string) WatchEvent {
	raw := parseURIPairs(line)
	ev := WatchEvent{Raw: raw}
	if ts, ok := raw["ts"]; ok {
		ev.TS = parseWatchTS(ts)
	}
	if gid, ok := raw["gid"]; ok {
		ev.GID, _ = strconv.ParseUint(gid, 10, 64) // best-effort: invalid gid stays 0
	}
	return ev
}

// parseWatchTS parses a watcher ts field as seconds plus an optional microsecond integer.
func parseWatchTS(ts string) time.Time {
	secStr, frac, ok := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(secStr, 10, 64)
	if err != nil {
		return time.Time{}
	}
	var nsec int64
	if ok && frac != "" {
		usec, uerr := strconv.ParseInt(frac, 10, 64)
		// logger.c writes usec as an integer (0–999999), not a fractional
		// second. Values outside that range keep second precision only.
		if uerr == nil && usec >= 0 && usec <= 999999 {
			nsec = usec * 1000
		}
	}
	return time.Unix(sec, nsec)
}

// parseURIPairs splits a space-separated k=v line and URL-decodes each value.
func parseURIPairs(line string) map[string]string {
	out := make(map[string]string, strings.Count(line, " ")+1)
	for field := range strings.FieldsSeq(line) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if dec, err := url.QueryUnescape(v); err == nil {
			v = dec
		}
		out[k] = v
	}
	return out
}
