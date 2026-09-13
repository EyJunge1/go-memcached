package memcached

import (
	"context"
	"fmt"
)

// MetaNoop sends a meta no-op and expects MN.
func (c *Client) MetaNoop(ctx context.Context) error {
	req, resp := buildMetaNoopCommand()
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_MetaMNCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
