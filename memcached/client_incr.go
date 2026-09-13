package memcached

import (
	"context"
	"fmt"
)

// Incr increments the key. With no-reply it returns 0.
func (c *Client) Incr(ctx context.Context, key string, delta uint64, opts ...ItemWriteOption) (uint64, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("incr", key, delta, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

// Decr decrements the key. With no-reply it returns 0.
func (c *Client) Decr(ctx context.Context, key string, delta uint64, opts ...ItemWriteOption) (uint64, error) {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("decr", key, delta, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

func parseArithmeticAfterRecv(dst *uint64) func(*response) error {
	return func(resp *response) error {
		if resp.endIndicator == endIndicatorNoReply || len(resp.rawLines) == 0 {
			*dst = 0
			return nil
		}
		value, err := parseArithmetic(resp.rawLines[0])
		if err != nil {
			return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
		}
		*dst = value
		return nil
	}
}
