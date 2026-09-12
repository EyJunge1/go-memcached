package memcached

import (
	"context"
	"fmt"
)

// MutateOpts adds optional noreply on Delete and Touch.
// Text Incr and Decr have no Quiet; use Meta().Arithmetic with q.
// Quiet adds noreply: the connection is discarded after send; CLIENT_ERROR and
// SERVER_ERROR are still returned when the server already replied.
type MutateOpts struct {
	Quiet bool
	// Extra appends raw wire tokens before noreply.
	Extra []string
}

// Delete drops the key. A miss is ErrNotFound.
func (c *Client) Delete(ctx context.Context, key []byte, opts MutateOpts) error {
	return c.deleteKey(ctx, nil, key, opts)
}

// deleteKey sends delete. node nil routes by key; non-nil pins the node.
func (c *Client) deleteKey(ctx context.Context, node *addr, key []byte, opts MutateOpts) error {
	key, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	req, resp, err := buildDeleteCommand(key, opts.Extra, opts.Quiet)
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

	resp.afterRecv = mapDeletedReply
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
