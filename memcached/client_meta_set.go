package memcached

import (
	"context"
	"fmt"
	"math"
)

// MetaSet stores a key with meta flags.
func (c *Client) MetaSet(ctx context.Context, key, value []byte, msOptions ...MetaSetOption) (*MetaItem, error) {
	msFlags := &metaSetFlags{}
	for _, applyFn := range msOptions {
		applyFn(msFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, value, msFlags.b); err != nil {
		return nil, err
	}
	if !validMetaSetMode(msFlags.M) {
		return nil, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if msFlags.OtokenSet {
		if err := validOpaqueToken(msFlags.Otoken); err != nil {
			return nil, err
		}
	}
	if msFlags.T > math.MaxInt64 {
		return nil, fmt.Errorf("TTL exceeds int64: %w", ErrInvalidArgument)
	}
	clientFlags := msFlags.F

	req, resp := buildMetaSetCommand(key, value, msFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key:   key,
		TTL:   int64(msFlags.T),
		Flags: clientFlags,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, msFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}
