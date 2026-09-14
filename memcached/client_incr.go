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

	resp.afterRecv = mapArithmeticReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}

	return resp.arithValue, nil
}

// mapArithmeticReply maps a numeric reply into resp.arithValue.
func mapArithmeticReply(resp *response) error {
	if len(resp.rawLines) == 0 {
		return fmt.Errorf("empty arithmetic reply: %w", ErrMalformedResponse)
	}
	line := resp.rawLines[0]
	if value, err := parseArithmetic(line); err == nil {
		resp.arithValue = value
		return nil
	}
	if err := forecastCommonFaultLine(line); err != nil {
		return err
	}
	return fmt.Errorf("invalid arithmetic reply: %w", ErrMalformedResponse)
}
