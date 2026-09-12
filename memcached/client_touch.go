package memcached

import (
	"context"
	"fmt"
)

// Touch changes expiry without reading the value. expiry 0 never expires.
func (c *Client) Touch(ctx context.Context, key []byte, expiry Exptime, opts MutateOpts) error {
	return c.touchKey(ctx, nil, key, expiry, opts)
}

// touchKey sends touch. node nil routes by key; non-nil pins the node.
func (c *Client) touchKey(ctx context.Context, node *addr, key []byte, expiry Exptime, opts MutateOpts) error {
	key, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	req, resp, err := buildTouchCommand(key, expiry, opts.Extra, opts.Quiet)
	if err != nil {
		putBuilderBox(keyBox)
		return err
	}
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	if opts.Quiet {
		if node != nil {
			return c.sendQuietToAddr(ctx, node, req, resp, true /* probeFault */)
		}
		return c.dispatchQuiet(ctx, req, resp)
	}

	resp.afterRecv = mapTouchedReply
	if node != nil {
		err = c.dispatchToAddr(ctx, node, req, resp)
	} else {
		err = c.dispatchRequest(ctx, req, resp)
	}
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	return nil
}
