package memcached

import (
	"context"
	"fmt"
)

// Delete removes the key.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return err
	}

	req, resp := buildDeleteCommand(key, false)
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_DeletedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
