package memcached

import (
	"context"
	"fmt"
)

// Incr increments the key and returns the new value.
// Quiet sends noreply and returns (0, nil) without inventing a sum.
func (c *Client) Incr(ctx context.Context, key []byte, delta uint64, opts MutateOpts) (uint64, error) {
	return c.arithmetic(ctx, "incr", key, delta, opts)
}

// Decr decrements the key and returns the new value.
// Quiet sends noreply and returns (0, nil) without inventing a difference.
func (c *Client) Decr(ctx context.Context, key []byte, delta uint64, opts MutateOpts) (uint64, error) {
	return c.arithmetic(ctx, "decr", key, delta, opts)
}

func (c *Client) arithmetic(ctx context.Context, cmd string, key []byte, delta uint64, opts MutateOpts) (uint64, error) {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return 0, err
	}

	req, resp := buildArithmeticCommand(cmd, key, delta, opts.Quiet)
	defer releaseReqAndResp(req, resp)

	if opts.Quiet {
		if err := c.dispatchQuiet(ctx, req, resp); err != nil {
			return 0, err
		}
		return 0, nil
	}

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
