package memcached

import (
	"context"
	"fmt"
)

// Touch changes expiry without reading the value.
func (c *Client) Touch(ctx context.Context, key []byte, expiry Exptime, opts MutateOpts) error {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return err
	}

	req, resp := buildTouchCommand(key, expiry, opts.Quiet)
	defer releaseReqAndResp(req, resp)

	if opts.Quiet {
		if err := c.dispatchQuiet(ctx, req, resp); err != nil {
			return err
		}
		return nil
	}

	resp.afterRecv = expectLine(_TouchedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
