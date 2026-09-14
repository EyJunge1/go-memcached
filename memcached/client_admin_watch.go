package memcached

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const adminStreamLineLimit = 64 * 1024

// WatchTopic is a closed set of watch stream names. Other strings are rejected.
type WatchTopic string

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

// releaseDedicated closes an unpooled admin-stream socket.
func releaseDedicated(cn wireConn) {
	if cn == nil {
		return
	}
	_ = cn.Close()
}

// readAdminStreamLine reads one watcher or dump line, capped at adminStreamLineLimit.
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
			if len(buf) > adminStreamLineLimit {
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

// sendAdminLine writes one admin command on a dedicated connection and maps the status reply.
func (a *AdminNode) sendAdminLine(ctx context.Context, cn wireConn, req *request, resp *response) error {
	a.c.enableUDPFraming(nil, req, resp)
	if err := req.send(ctx, cn, a.c.rt.writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, a.c.rt.readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	if err := parseAdminReply(resp); err != nil {
		return err
	}
	return nil
}

// acquireStream dials a dedicated socket for watch or dump; ctx must carry a deadline.
func (a *AdminNode) acquireStream(ctx context.Context) (wireConn, error) {
	if err := requireDeadline(ctx); err != nil {
		return nil, err
	}
	addr, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	cn, err := a.c.dialDedicated(ctx, addr)
	if err != nil {
		return nil, wrapTransport(err)
	}
	return cn, nil
}

// Watch opens a watcher stream on the bound node.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
func (a *AdminNode) Watch(ctx context.Context, topics ...WatchTopic) (<-chan WatchEvent, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("watch: %w", ErrInvalidArgument)
	}
	for _, t := range topics {
		if !allowedWatchTopic(t) {
			return nil, fmt.Errorf("watch topic %q: %w", t, ErrInvalidArgument)
		}
	}
	cn, err := a.acquireStream(ctx)
	if err != nil {
		return nil, err
	}

	req, resp := buildWatchCommand(topics)
	if err = a.sendAdminLine(ctx, cn, req, resp); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		return nil, err
	}
	releaseReqAndResp(req, resp)

	out := make(chan WatchEvent, 16)
	go a.readWatch(ctx, cn, out)
	return out, nil
}

// readWatch pumps watcher log lines into out until ctx ends or the stream fails.
func (a *AdminNode) readWatch(ctx context.Context, cn wireConn, out chan WatchEvent) {
	defer close(out)
	defer releaseDedicated(cn)
	stop := context.AfterFunc(ctx, func() { _ = cn.Close() })
	defer stop()

	for {
		if err := ctx.Err(); err != nil {
			return
		}
		_ = cn.setReadDeadline(time.Time{})
		if dl, ok := ctx.Deadline(); ok {
			_ = cn.setReadDeadline(dl)
		}
		line, err := readAdminStreamLine(cn)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case out <- WatchEvent{Err: wrapRecvError(err)}:
			case <-ctx.Done():
			}
			return
		}
		trimmed := trimCRLF(line)
		if err = adminStreamLineError(trimmed); err != nil {
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
			return
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
		ev.GID, _ = strconv.ParseUint(gid, 10, 64)
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
	out := make(map[string]string)
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

// LRUCrawlerMetaDump streams URI-encoded item metadata until END or EN.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
func (a *AdminNode) LRUCrawlerMetaDump(ctx context.Context, selector string) (<-chan DumpEntry, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	ch := make(chan DumpEntry, 16)
	if err := a.startDumpStream(ctx, "metadump", selector, func(line string) DumpEntry {
		return DumpEntry{Fields: parseURIPairs(line)}
	}, func(e DumpEntry) {
		select {
		case ch <- e:
		case <-ctx.Done():
		}
	}, func() { close(ch) }); err != nil {
		return nil, err
	}
	return ch, nil
}

// LRUCrawlerMgDump streams "mg …" lines until END or EN for a later Meta().Get.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
func (a *AdminNode) LRUCrawlerMgDump(ctx context.Context, selector string) (<-chan DumpEntry, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	ch := make(chan DumpEntry, 16)
	if err := a.startDumpStream(ctx, "mgdump", selector, func(line string) DumpEntry {
		return DumpEntry{Line: line}
	}, func(e DumpEntry) {
		select {
		case ch <- e:
		case <-ctx.Done():
		}
	}, func() { close(ch) }); err != nil {
		return nil, err
	}
	return ch, nil
}

// startDumpStream opens a metadump or mgdump stream and emits entries until END, EN, or error.
func (a *AdminNode) startDumpStream(ctx context.Context, kind, selector string, parse func(string) DumpEntry, emit func(DumpEntry), done func()) error {
	cn, err := a.acquireStream(ctx)
	if err != nil {
		return err
	}

	req, resp := buildLRUCrawlerCommand(kind, selector)
	a.c.enableUDPFraming(nil, req, resp)
	if err = req.send(ctx, cn, a.c.rt.writeTimeout); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	releaseReqAndResp(req, resp)

	stopFirst := context.AfterFunc(ctx, func() { _ = cn.Close() })
	if dl, ok := ctx.Deadline(); ok {
		_ = cn.setReadDeadline(dl)
	} else if a.c.rt.readTimeout > 0 {
		_ = cn.setReadDeadline(time.Now().Add(a.c.rt.readTimeout))
	} else {
		_ = cn.setReadDeadline(time.Time{})
	}
	line, err := readAdminStreamLine(cn)
	stopFirst()
	if err != nil {
		releaseDedicated(cn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	trimmed := trimCRLF(line)
	s := string(trimmed)
	if bytes.Equal(trimmed, _OKBytes) {
		stopAgain := context.AfterFunc(ctx, func() { _ = cn.Close() })
		if dl, ok := ctx.Deadline(); ok {
			_ = cn.setReadDeadline(dl)
		}
		line, err = readAdminStreamLine(cn)
		stopAgain()
		if err != nil {
			releaseDedicated(cn)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return wrapRecvError(fmt.Errorf("recv failed: %w", err))
		}
		trimmed = trimCRLF(line)
		s = string(trimmed)
	}
	if isDumpEnd(s) {
		releaseDedicated(cn)
		go done()
		return nil
	}
	if err = adminStreamLineError(trimmed); err != nil {
		releaseDedicated(cn)
		return err
	}

	go func() {
		defer done()
		defer releaseDedicated(cn)
		stop := context.AfterFunc(ctx, func() { _ = cn.Close() })
		defer stop()
		emit(parse(s))
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			_ = cn.setReadDeadline(time.Time{})
			if dl, ok := ctx.Deadline(); ok {
				_ = cn.setReadDeadline(dl)
			}
			line, err := readAdminStreamLine(cn)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				emit(DumpEntry{Err: wrapRecvError(err)})
				return
			}
			trimmed := trimCRLF(line)
			next := string(trimmed)
			if isDumpEnd(next) {
				return
			}
			if err := adminStreamLineError(trimmed); err != nil {
				emit(DumpEntry{Err: err})
				return
			}
			emit(parse(next))
		}
	}()
	return nil
}
