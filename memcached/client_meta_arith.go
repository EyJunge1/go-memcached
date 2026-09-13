package memcached

import (
	"context"
	"fmt"
)

// MetaArithmetic increments or decrements a key with meta flags.
func (c *Client) MetaArithmetic(ctx context.Context, key []byte, delta uint64, f MetaArithmetic) (MetaResult, error) {
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	maFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, maFlags.b); err != nil {
		return MetaResult{}, err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		return MetaResult{}, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			return MetaResult{}, err
		}
	}

	req, resp := buildMetaArithmeticCommand(key, delta, maFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{Key: key}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, maFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return metaResultFrom(item, item.Status), fmt.Errorf("request failed: %w", err)
	}
	return metaResultFrom(item, item.Status), nil
}
