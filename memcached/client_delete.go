package memcached

import (
	"context"
	"fmt"
)

// MutateOpts selects optional noreply for delete, touch, incr, and decr.
type MutateOpts struct {
	Quiet bool
}

// Delete removes the key.
func (c *Client) Delete(ctx context.Context, key []byte, opts MutateOpts) error {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return err
	}

	req, resp := buildDeleteCommand(key, opts.Quiet)
	defer releaseReqAndResp(req, resp)

	if opts.Quiet {
		if err := c.dispatchQuiet(ctx, req, resp); err != nil {
			return err
		}
		return nil
	}

	resp.afterRecv = expectLine(_DeletedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
