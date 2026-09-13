package memcached

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
)

const authProbeKeyPrefix = "__mc_auth_"

var (
	readAuthRand = crand.Read
	authProbeSeq atomic.Uint64
)

// newAuthProbeKey returns a unique key used only for the token-auth handshake probe.
func newAuthProbeKey() []byte {
	var b [8]byte
	if _, err := readAuthRand(b[:]); err == nil {
		return []byte(authProbeKeyPrefix + hex.EncodeToString(b[:]))
	}
	// Fallback when CSPRNG fails: nanosecond clock plus a monotonic counter.
	n := authProbeSeq.Add(1)
	t := uint64(clock().UnixNano()) //nolint:gosec // uniqueness only
	return []byte(fmt.Sprintf("%s%016x%08x", authProbeKeyPrefix, t, n))
}

// isAuthProbeKey reports whether key was minted by newAuthProbeKey.
func isAuthProbeKey(key string) bool {
	return strings.HasPrefix(key, authProbeKeyPrefix)
}

// validAuthSecret rejects credentials that contain whitespace or ASCII controls.
func validAuthSecret(s string) error {
	if strings.IndexFunc(s, func(r rune) bool {
		return r == '\r' || r == '\n' || r == 0 || r == ' ' || r == '\t' || unicode.IsSpace(r)
	}) >= 0 {
		return fmt.Errorf("auth credential invalid: %w", ErrInvalidArgument)
	}
	return nil
}

// buildAuthCommand encodes the set used to plant the -Y token-auth probe item.
func buildAuthCommand(username, password string, key []byte) (*request, *response) {
	payload := []byte(username + " " + password)
	return buildStorageCommand("set", key, payload, 0, ExptimeNever, false)
}

// authHandshake runs -Y token auth on a new connection and fails if the probe item remains.
func authHandshake(ctx context.Context, cn wireConn, user, pass string, writeTimeout, readTimeout time.Duration) error {
	payload := []byte(user + " " + pass)
	probeKey := newAuthProbeKey()
	req, resp := buildAuthCommand(user, pass, probeKey)
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

	greq, gresp := buildGetsCommand("get", probeKey)
	defer releaseReqAndResp(greq, gresp)
	var leftover []Item
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
	// A different body means the key already existed; -Y ate our set.
	if !bytes.Equal(leftover[0].Value, payload) {
		return nil
	}

	dreq, dresp := buildDeleteCommand(probeKey, false)
	defer releaseReqAndResp(dreq, dresp)
	dresp.afterRecv = mapDeletedReply
	_ = dreq.send(ctx, cn, writeTimeout)
	_ = dresp.recv(ctx, cn, readTimeout)
	return fmt.Errorf("auth left credential item: %w", ErrInvalidArgument)
}
