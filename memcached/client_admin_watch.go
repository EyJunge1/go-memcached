package memcached

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const adminStreamLineLimit = 64 * 1024

// WatchTopic is a typed watch stream name. Free strings are rejected.
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

// WatchEvent is one watcher log line.
type WatchEvent struct {
	TS  time.Time
	GID uint64
	Raw map[string]string
}

func releaseDedicated(cn memcachedConn) {
	if cn == nil {
		return
	}
	_ = cn.Close()
}

func readAdminStreamLine(cn memcachedConn) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 1)
	for {
		n, err := cn.Read(tmp)
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

func (a *AdminClient) sendAdminLine(ctx context.Context, cn memcachedConn, req *request, resp *response) error {
	a.c.enableUDPFraming(nil, req, resp)
	if err := req.send(ctx, cn, a.c.options.writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, a.c.options.readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	if err := parseAdminReply(resp); err != nil {
		return err
	}
	return nil
}

func (a *AdminClient) acquireStream(ctx context.Context) (memcachedConn, error) {
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

// Watch starts a watcher stream on the bound node. ctx must have a deadline.
func (a *AdminClient) Watch(ctx context.Context, topics ...WatchTopic) (<-chan WatchEvent, error) {
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

func (a *AdminClient) readWatch(ctx context.Context, cn memcachedConn, out chan WatchEvent) {
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
			return
		}
		ev := parseWatchEvent(string(trimCRLF(line)))
		select {
		case out <- ev:
		case <-ctx.Done():
			return
		}
	}
}

func parseWatchEvent(line string) WatchEvent {
	raw := parseURIPairs(line)
	ev := WatchEvent{Raw: raw}
	if ts, ok := raw["ts"]; ok {
		if n, err := strconv.ParseInt(ts, 10, 64); err == nil {
			ev.TS = time.Unix(n, 0)
		}
	}
	if gid, ok := raw["gid"]; ok {
		ev.GID, _ = strconv.ParseUint(gid, 10, 64)
	}
	return ev
}

func parseURIPairs(line string) map[string]string {
	out := make(map[string]string)
	for _, field := range strings.Fields(line) {
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

// LRUCrawlerMetaDump streams URI-encoded item metadata until END.
func (a *AdminClient) LRUCrawlerMetaDump(ctx context.Context, selector string) (<-chan map[string]string, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	ch := make(chan map[string]string, 16)
	if err := a.startDumpStream(ctx, "metadump", selector, func(line string, last bool) {
		if last {
			return
		}
		select {
		case ch <- parseURIPairs(line):
		case <-ctx.Done():
		}
	}, func() { close(ch) }); err != nil {
		return nil, err
	}
	return ch, nil
}

// LRUCrawlerMgDump streams "mg …" lines until END for a later MetaGet.
func (a *AdminClient) LRUCrawlerMgDump(ctx context.Context, selector string) (<-chan string, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	ch := make(chan string, 16)
	if err := a.startDumpStream(ctx, "mgdump", selector, func(line string, last bool) {
		if last {
			return
		}
		select {
		case ch <- line:
		case <-ctx.Done():
		}
	}, func() { close(ch) }); err != nil {
		return nil, err
	}
	return ch, nil
}

func (a *AdminClient) startDumpStream(ctx context.Context, kind, selector string, onLine func(string, bool), done func()) error {
	cn, err := a.acquireStream(ctx)
	if err != nil {
		return err
	}

	req, resp := buildLRUCrawlerCommand(kind, selector)
	a.c.enableUDPFraming(nil, req, resp)
	if err = req.send(ctx, cn, a.c.options.writeTimeout); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	releaseReqAndResp(req, resp)

	stopFirst := context.AfterFunc(ctx, func() { _ = cn.Close() })
	if dl, ok := ctx.Deadline(); ok {
		_ = cn.setReadDeadline(dl)
	} else if a.c.options.readTimeout > 0 {
		_ = cn.setReadDeadline(time.Now().Add(a.c.options.readTimeout))
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
	s := string(trimCRLF(line))
	if s == "OK" {
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
		s = string(trimCRLF(line))
	}
	if s == "END" {
		releaseDedicated(cn)
		go func() {
			onLine(s, true)
			done()
		}()
		return nil
	}
	if err = adminStreamLineError(s); err != nil {
		releaseDedicated(cn)
		return err
	}

	go func() {
		defer done()
		defer releaseDedicated(cn)
		stop := context.AfterFunc(ctx, func() { _ = cn.Close() })
		defer stop()
		onLine(s, false)
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
				return
			}
			next := string(trimCRLF(line))
			if next == "END" {
				onLine(next, true)
				return
			}
			if err := adminStreamLineError(next); err != nil {
				return
			}
			onLine(next, false)
		}
	}()
	return nil
}
