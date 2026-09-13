package memcached

import (
	"context"
	"fmt"
)

// MetaArithmetic increments or decrements a key with meta flags.
func (c *Client) MetaArithmetic(ctx context.Context, key []byte, delta uint64, options ...MetaArithmeticOption) (*MetaItem, error) {
	maFlags := &metaArithmeticFlags{}
	for _, applyFn := range options {
		applyFn(maFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, maFlags.b); err != nil {
		return nil, err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		return nil, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			return nil, err
		}
	}

	req, resp := buildMetaArithmeticCommand(key, delta, maFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, maFlags.q, maFlags.v)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}
