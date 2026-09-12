package memcached

import (
	"context"
	"fmt"
)

// ArithmeticOpts adds raw Extra tokens on text incr/decr.
// Text Incr/Decr have no Quiet; use Meta().Arithmetic with q.
type ArithmeticOpts struct {
	// Extra appends raw wire tokens before the line end.
	Extra []string
}

// Incr adds delta to the key and writes the updated counter into dst.
// dst must be non-nil. Quiet arithmetic is Meta().Arithmetic with q.
func (c *Client) Incr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	return c.arithmetic(ctx, nil, "incr", key, delta, dst, opts)
}

// Decr subtracts delta from the key and writes the updated counter into dst.
// dst must be non-nil. Quiet arithmetic is Meta().Arithmetic with q.
func (c *Client) Decr(ctx context.Context, key []byte, delta uint64, dst *uint64, opts ArithmeticOpts) error {
	return c.arithmetic(ctx, nil, "decr", key, delta, dst, opts)
}

// arithmetic sends incr or decr. node nil routes by key; non-nil pins the node.
func (c *Client) arithmetic(
	ctx context.Context,
	node *addr,
	cmd string,
	key []byte,
	delta uint64,
	dst *uint64,
	opts ArithmeticOpts,
) error {
	if dst == nil {
		return fmt.Errorf("arithmetic dst: %w", ErrInvalidArgument)
	}
	*dst = 0
	key, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	req, resp, err := buildArithmeticCommand(cmd, key, delta, opts.Extra, false /* noReply */)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapArithmeticReply
	if node != nil {
		err = c.dispatchToAddr(ctx, node, req, resp)
	} else {
		err = c.dispatchRequest(ctx, req, resp)
	}
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	*dst = resp.arithValue
	return nil
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
