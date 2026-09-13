package memcached

import (
	"context"
	"fmt"
)

// Delete removes the key.
func (c *Client) Delete(ctx context.Context, key string, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildDeleteCommand(key, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_DeletedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
