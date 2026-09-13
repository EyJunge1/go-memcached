package memcached

import (
	"context"
	"fmt"
)

// Incr adds delta to the key and returns the updated counter.
func (c *Client) Incr(ctx context.Context, key []byte, delta uint64) (uint64, error) {
	return c.arithmetic(ctx, "incr", key, delta)
}

// Decr subtracts delta from the key and returns the updated counter.
func (c *Client) Decr(ctx context.Context, key []byte, delta uint64) (uint64, error) {
	return c.arithmetic(ctx, "decr", key, delta)
}

// arithmetic sends incr or decr and returns the updated counter.
func (c *Client) arithmetic(ctx context.Context, cmd string, key []byte, delta uint64) (uint64, error) {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand(cmd, key, delta, false)
	defer releaseReqAndResp(req, resp)

	var value uint64
	resp.afterRecv = parseArithmeticAfterRecv(&value)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return value, nil
}

// parseArithmeticAfterRecv maps a numeric reply into dst after recv.
func parseArithmeticAfterRecv(dst *uint64) func(*response) error {
	return func(resp *response) error {
		if len(resp.rawLines) == 0 {
			return fmt.Errorf("empty arithmetic reply: %w", ErrMalformedResponse)
		}
		if err := forecastCommonFaultLine(resp.rawLines[0]); err != nil {
			return err
		}
		value, err := parseArithmetic(resp.rawLines[0])
		if err != nil {
			return fmt.Errorf("%s: %w", err.Error(), ErrMalformedResponse)
		}
		*dst = value
		return nil
	}
}
