package memcached

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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

func (c *Client) pickAdminAddr() (*Addr, error) {
	addrs, picker := c.snapshotCluster()
	addrs = c.liveAddrs(addrs)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if isUDPNetwork(addrs[0].Network) {
		return nil, fmt.Errorf("admin streams: %w", ErrNotSupported)
	}
	if picker == nil {
		return addrs[0], nil
	}
	return picker.Pick(addrs, []byte("version"), nil)
}

func (c *Client) acquireDedicated(ctx context.Context, addr *Addr) (memcachedConn, error) {
	if addr == nil {
		return nil, fmt.Errorf("no available address: %w", ErrInvalidAddress)
	}
	if isUDPNetwork(addr.Network) {
		return nil, fmt.Errorf("admin streams: %w", ErrNotSupported)
	}
	return c.getConn(ctx, addr)
}

func releaseDedicated(cn memcachedConn) {
	if cn == nil {
		return
	}
	if p := cn.getConnPool(); p != nil {
		_ = p.discard(cn)
		return
	}
	_ = cn.Close()
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

// Watch starts a watcher stream. The connection is dedicated until ctx is
// cancelled or the server closes. Always pass a timeout or cancel.
func (a *AdminClient) Watch(ctx context.Context, topics ...WatchTopic) (<-chan WatchEvent, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("watch: %w", ErrInvalidArgument)
	}
	for _, t := range topics {
		if !allowedWatchTopic(t) {
			return nil, fmt.Errorf("watch topic %q: %w", t, ErrInvalidArgument)
		}
	}
	addr, err := a.c.pickAdminAddr()
	if err != nil {
		return nil, err
	}
	cn, err := a.c.acquireDedicated(ctx, addr)
	if err != nil {
		return nil, wrapTransport(err)
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
		line, err := cn.readLine('\n')
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
// The connection is discarded when the stream ends or ctx is cancelled.
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
	addr, err := a.c.pickAdminAddr()
	if err != nil {
		return err
	}
	cn, err := a.c.acquireDedicated(ctx, addr)
	if err != nil {
		return wrapTransport(err)
	}

	req, resp := buildLRUCrawlerCommand(kind, selector)
	// dumps do not send OK then END; they stream lines until END.
	a.c.enableUDPFraming(nil, req, resp)
	if err = req.send(ctx, cn, a.c.options.writeTimeout); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		return wrapTransport(fmt.Errorf("send failed: %w", err))
	}
	releaseReqAndResp(req, resp)

	stopFirst := context.AfterFunc(ctx, func() { _ = cn.Close() })
	if a.c.options.readTimeout > 0 {
		_ = cn.setReadDeadline(time.Now().Add(a.c.options.readTimeout))
	} else {
		_ = cn.setReadDeadline(time.Time{})
	}
	line, err := cn.readLine('\n')
	stopFirst()
	if err != nil {
		releaseDedicated(cn)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return wrapRecvError(fmt.Errorf("recv failed: %w", err))
	}
	s := string(trimCRLF(line))
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
			line, err := cn.readLine('\n')
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
