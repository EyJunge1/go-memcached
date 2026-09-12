package memcached

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

// DumpStream is a metadump or mgdump feed on a dedicated connection.
// Cancel the parent context or call Close to stop; Close waits for the reader
// goroutine (if any) to exit. Always call Close when finished.
type DumpStream struct {
	// Entries receives dump lines until the stream ends; it is closed when
	// the reader exits (or immediately when the dump is empty).
	Entries <-chan DumpEntry

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// Close cancels the stream and waits for the background reader to exit.
// It is safe to call more than once; a stream that never started a goroutine
// returns immediately.
func (s *DumpStream) Close() {
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

// LRUCrawlerMetaDump streams URI-encoded item metadata until END or EN.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
// Call Close on the returned stream to wait for the reader goroutine.
func (a *AdminNode) LRUCrawlerMetaDump(ctx context.Context, selector string) (*DumpStream, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	return a.startDumpStream(ctx, OpMetadump, selector, func(line string) DumpEntry {
		return DumpEntry{Fields: parseURIPairs(line)}
	})
}

// LRUCrawlerMgDump streams "mg …" lines until END or EN for a later Meta().Get.
// ctx must carry a deadline; cancel or expiry closes the dedicated connection.
// Call Close on the returned stream to wait for the reader goroutine.
func (a *AdminNode) LRUCrawlerMgDump(ctx context.Context, selector string) (*DumpStream, error) {
	if err := validCrawlerSelector(selector); err != nil {
		return nil, err
	}
	return a.startDumpStream(ctx, OpMgdump, selector, func(line string) DumpEntry {
		return DumpEntry{Line: line}
	})
}

// startDumpStream opens a metadump or mgdump stream and emits entries until END, EN, or error.
func (a *AdminNode) startDumpStream(
	ctx context.Context,
	kind, selector string,
	parse func(string) DumpEntry,
) (*DumpStream, error) {
	start := time.Now()
	addr, c, err := a.resolveTarget()
	if err != nil {
		return nil, err
	}
	cn, err := a.acquireStream(ctx)
	if err != nil {
		c.observeOp(ctx, start, kind, addr, 1, err)
		return nil, err
	}

	req, resp := buildLRUCrawlerCommand(kind, selector)
	c.enableUDPFraming(nil, req, resp)
	if err = req.send(ctx, cn, c.rt.writeTimeout); err != nil {
		releaseReqAndResp(req, resp)
		releaseDedicated(cn)
		err = wrapTransport(fmt.Errorf("send: %w", err))
		c.observeOp(ctx, start, kind, addr, 1, err)
		return nil, err
	}
	releaseReqAndResp(req, resp)

	stopFirst := context.AfterFunc(ctx, func() { _ = cn.Close() }) // best-effort: cancel closes stream
	if dl, ok := ctx.Deadline(); ok {
		_ = cn.setReadDeadline(dl) // best-effort: dump header read bound
	} else if c.rt.readTimeout > 0 {
		_ = cn.setReadDeadline(time.Now().Add(c.rt.readTimeout)) // best-effort: dump header read bound
	} else {
		_ = cn.setReadDeadline(time.Time{}) // best-effort: clear read deadline
	}
	line, err := readAdminStreamLine(cn)
	stopFirst()
	if err != nil {
		releaseDedicated(cn)
		err = wrapRecvError(fmt.Errorf("recv: %w", err))
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		c.observeOp(ctx, start, kind, addr, 1, err)
		return nil, err
	}
	trimmed := trimCRLF(line)
	s := string(trimmed)
	if bytes.Equal(trimmed, _OKBytes) {
		stopAgain := context.AfterFunc(ctx, func() { _ = cn.Close() }) // best-effort: cancel closes stream
		if dl, ok := ctx.Deadline(); ok {
			_ = cn.setReadDeadline(dl) // best-effort: dump body read bound
		}
		line, err = readAdminStreamLine(cn)
		stopAgain()
		if err != nil {
			releaseDedicated(cn)
			err = wrapRecvError(fmt.Errorf("recv: %w", err))
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			c.observeOp(ctx, start, kind, addr, 1, err)
			return nil, err
		}
		trimmed = trimCRLF(line)
		s = string(trimmed)
	}
	// Size 1: brief consumer stall without unbounded buffering (Uber: 0 or 1).
	ch := make(chan DumpEntry, 1)
	stream := &DumpStream{Entries: ch}
	if isDumpEnd(s) {
		releaseDedicated(cn)
		c.observeOp(ctx, start, kind, addr, 1, nil)
		close(ch)
		return stream, nil
	}
	if err = adminStreamLineError(trimmed); err != nil {
		releaseDedicated(cn)
		c.observeOp(ctx, start, kind, addr, 1, err)
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream.cancel = cancel
	stream.wg.Add(1)
	go func() {
		var streamErr error
		defer stream.wg.Done()
		defer func() {
			c.observeOp(streamCtx, start, kind, addr, 1, streamErr)
		}()
		defer close(ch)
		defer releaseDedicated(cn)
		stop := context.AfterFunc(streamCtx, func() { _ = cn.Close() }) // best-effort: cancel closes stream
		defer stop()
		emit := func(e DumpEntry) {
			select {
			case ch <- e:
			case <-streamCtx.Done():
			}
		}
		emit(parse(s))
		for {
			if err := streamCtx.Err(); err != nil {
				streamErr = err
				return
			}
			_ = cn.setReadDeadline(time.Time{}) // best-effort: clear before ctx deadline
			if dl, ok := streamCtx.Deadline(); ok {
				_ = cn.setReadDeadline(dl) // best-effort: stream read bound
			}
			line, err := readAdminStreamLine(cn)
			if err != nil {
				if streamCtx.Err() != nil {
					streamErr = streamCtx.Err()
					return
				}
				streamErr = wrapRecvError(err)
				emit(DumpEntry{Err: streamErr})
				return
			}
			trimmed := trimCRLF(line)
			next := string(trimmed)
			if isDumpEnd(next) {
				return
			}
			if err := adminStreamLineError(trimmed); err != nil {
				streamErr = err
				emit(DumpEntry{Err: err})
				return
			}
			emit(parse(next))
		}
	}()
	return stream, nil
}
