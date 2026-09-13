package memcached

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"
)

func validAuthSecret(s string) error {
	if strings.IndexFunc(s, func(r rune) bool {
		return r == '\r' || r == '\n' || r == 0 || r == ' ' || r == '\t' || unicode.IsSpace(r)
	}) >= 0 {
		return fmt.Errorf("auth credential invalid: %w", ErrInvalidArgument)
	}
	return nil
}

func buildAuthCommand(username, password string) (*request, *response) {
	payload := []byte(username + " " + password)
	return buildStorageCommand("set", []byte("_"), payload, 0, ExptimeImmediate, false)
}

func authHandshake(ctx context.Context, cn memcachedConn, user, pass string, writeTimeout, readTimeout time.Duration) error {
	req, resp := buildAuthCommand(user, pass)
	defer releaseReqAndResp(req, resp)
	resp.afterRecv = mapStorageReply
	if err := req.send(ctx, cn, writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("auth send failed: %w", err))
	}
	if err := resp.recv(ctx, cn, readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("auth recv failed: %w", err))
	}
	if resp.afterRecv != nil {
		if err := wrapRecvError(resp.afterRecv(resp)); err != nil {
			return err
		}
	}

	greq, gresp := buildGetsCommand("get", []byte("_"))
	defer releaseReqAndResp(greq, gresp)
	var leftover []*Item
	gresp.afterRecv = func(r *response) error {
		items, err := parseValueItems(r.rawLines, false, false)
		if err != nil {
			return fmt.Errorf("parse values failed: %w", err)
		}
		leftover = items
		return nil
	}
	if err := greq.send(ctx, cn, writeTimeout); err != nil {
		return wrapTransport(fmt.Errorf("auth verify send failed: %w", err))
	}
	if err := gresp.recv(ctx, cn, readTimeout); err != nil {
		return wrapRecvError(fmt.Errorf("auth verify recv failed: %w", err))
	}
	if gresp.afterRecv != nil {
		if err := wrapRecvError(gresp.afterRecv(gresp)); err != nil {
			return err
		}
	}
	if len(leftover) == 0 {
		return nil
	}

	dreq, dresp := buildDeleteCommand([]byte("_"), false)
	defer releaseReqAndResp(dreq, dresp)
	dresp.afterRecv = expectLine(_DeletedCRLFBytes)
	_ = dreq.send(ctx, cn, writeTimeout)
	_ = dresp.recv(ctx, cn, readTimeout)
	return fmt.Errorf("auth left credential item: %w", ErrInvalidArgument)
}
