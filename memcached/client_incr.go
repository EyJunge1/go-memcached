package memcached

import (
	"context"
	"fmt"
)

// Incr increments the key and returns the new value.
func (c *Client) Incr(ctx context.Context, key []byte, delta uint64) (uint64, error) {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("incr", key, delta, false)
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

// Decr decrements the key and returns the new value.
func (c *Client) Decr(ctx context.Context, key []byte, delta uint64) (uint64, error) {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand("decr", key, delta, false)
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
		if len(resp.rawLines) == 0 {
			return fmt.Errorf("empty arithmetic reply: %w", ErrMalformedResponse)
		}
		value, err := parseArithmetic(resp.rawLines[0])
		if err != nil {
			return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
		}
		*dst = value
		return nil
	}
}
