package memcached

import (
	"bytes"
	"context"
	"fmt"
	"time"
)

func (resp *response) recv(ctx context.Context, rr wireConn, readTimeout time.Duration) error {
	const armDeadline = true
	return resp.doRecv(ctx, rr, readTimeout, armDeadline)
}

// recvReady reads without arming or clearing deadlines. The caller already set them.
func (resp *response) recvReady(ctx context.Context, rr wireConn) error {
	const armDeadline = false
	return resp.doRecv(ctx, rr, 0, armDeadline)
}

func (resp *response) doRecv(ctx context.Context, rr wireConn, readTimeout time.Duration, armDeadline bool) error {
	if armDeadline {
		const isRead = true
		if has := selectProximateDeadline(ctx, rr, readTimeout, time.Now, isRead); has {
			defer clearReadDeadline(rr)
		}
	}
	if armDeadline && ctx != nil && ctx.Done() != nil {
		stop := watchContextCancel(ctx, rr)
		defer stop()
	}

	if resp.udpEnabled {
		box, payload, err := assembleUDPPayload(
			rr,
			resp.udpReqID,
			udpFragmentCap(resp.maxValueBytes),
			udpAssemblyBudget(resp.maxValueBytes),
			resp.udpMustOwnPayload(),
		)
		if err != nil {
			return err
		}
		resp.udpBox = box
		resp.udpFrame = byteFrame{buf: payload}
		if err = resp.recvFrom(&resp.udpFrame); err != nil {
			return err
		}
		if err = ensureNoTrailingBytes(&resp.udpFrame); err != nil {
			return err
		}
		if resp.udpBox == nil {
			ownRawLines(resp)
		}
		return nil
	}
	if c, ok := rr.(*conn); ok {
		return resp.recvFrom(c)
	}
	return resp.recvFrom(wireProtocolReader{c: rr})
}

// udpMustOwnPayload reports whether assembled UDP bytes must outlive the conn.
// Get and dest Meta.Get copy bodies during recv, so a tot=1 view is enough.
func (resp *response) udpMustOwnPayload() bool {
	if resp.endIndicator == _endIndicatorRetrieval {
		return false
	}
	if resp.metaItem != nil || resp.metaPipeDst != nil {
		switch resp.endIndicator {
		case _endIndicatorLimitedLines, _endIndicatorSpecificEndLine, _endIndicatorQuietBarrier:
			return false
		}
	}
	return true
}

func (resp *response) recvFrom(reader protocolReader) error {
	switch resp.endIndicator {
	case _endIndicatorNoReply:
		return nil
	case _endIndicatorLimitedLines:
		return resp.read1(reader)
	case _endIndicatorSpecificEndLine:
		return resp.read2(reader, false /* collectFaults */)
	case _endIndicatorQuietBarrier:
		return resp.read2(reader, true /* collectFaults */)
	case _endIndicatorRetrieval:
		return resp.readRetrieval(reader)
	default:
	}

	return fmt.Errorf("unknown end indicator: %w", ErrMalformedResponse)
}

func ownRawLines(resp *response) {
	for i, line := range resp.rawLines {
		resp.rawLines[i] = bytes.Clone(line)
	}
}

// selectProximateDeadline applies the sooner of ctx and timeout to the conn.
func selectProximateDeadline(
	ctx context.Context, rr wireConn, timeout time.Duration, now clockFunc, isRead bool,
) (ok bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout < 0 {
		timeout = 0
	}

	var (
		deadline time.Time
		has      bool
	)
	if timeout > 0 {
		deadline = now().Add(timeout)
		has = true
	}

	if ctxDeadline, ok := ctx.Deadline(); ok {
		if !has || ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
			has = true
		}
	}

	if !has {
		return false
	}
	if isRead {
		_ = rr.setReadDeadline(deadline) // best-effort: deadline on closed conn
		return true
	}
	_ = rr.setWriteDeadline(deadline) // best-effort: deadline on closed conn
	return true
}

// prepareOpIO arms one deadline for the op and watches ctx cancel.
func prepareOpIO(ctx context.Context, rr wireConn, readTimeout, writeTimeout time.Duration) func() {
	applyOpDeadlines(ctx, rr, readTimeout, writeTimeout)
	if ctx != nil && ctx.Done() != nil {
		return watchContextCancel(ctx, rr)
	}
	return nopStop
}

// applyOpDeadlines sets one combined deadline when read and write timeouts match.
func applyOpDeadlines(ctx context.Context, rr wireConn, readTimeout, writeTimeout time.Duration) {
	applyOpDeadlinesAt(ctx, rr, readTimeout, writeTimeout, time.Now)
}

func applyOpDeadlinesAt(ctx context.Context, rr wireConn, readTimeout, writeTimeout time.Duration, now clockFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if readTimeout < 0 {
		readTimeout = 0
	}
	if writeTimeout < 0 {
		writeTimeout = 0
	}
	var ctxDL time.Time
	hasCtx := false
	if d, ok := ctx.Deadline(); ok {
		ctxDL, hasCtx = d, true
	}
	if readTimeout == writeTimeout {
		dl, _ := combineDeadline(readTimeout, ctxDL, hasCtx, now)
		_ = rr.setDeadline(dl) // best-effort: deadline on closed conn
		return
	}
	dl, _ := combineDeadline(writeTimeout, ctxDL, hasCtx, now)
	_ = rr.setWriteDeadline(dl) // best-effort: deadline on closed conn
	dl, _ = combineDeadline(readTimeout, ctxDL, hasCtx, now)
	_ = rr.setReadDeadline(dl) // best-effort: deadline on closed conn
}

func combineDeadline(timeout time.Duration, ctxDL time.Time, hasCtx bool, now clockFunc) (time.Time, bool) {
	var dl time.Time
	has := false
	if timeout > 0 {
		dl = now().Add(timeout)
		has = true
	}
	if hasCtx && (!has || ctxDL.Before(dl)) {
		return ctxDL, true
	}
	return dl, has
}
