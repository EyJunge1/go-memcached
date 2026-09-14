package memcached

import (
	"context"
	"fmt"
)

// MutateOpts adds optional noreply on Delete and Touch.
// Text Incr and Decr have no Quiet; use Meta().Arithmetic with q.
type MutateOpts struct {
	Quiet bool
}

// Delete drops the key.
func (c *Client) Delete(ctx context.Context, key []byte, opts MutateOpts) error {
	key, keyBox := c.nsKey(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	req, resp := buildDeleteCommand(key, opts.Quiet)
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	if opts.Quiet {
		if err := c.dispatchQuiet(ctx, req, resp); err != nil {
			return err
		}
		return nil
	}

	resp.afterRecv = mapDeletedReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
