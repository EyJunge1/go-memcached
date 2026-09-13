package memcached

import (
	"context"
	"fmt"
)

// Touch updates expiration without fetching the item.
func (c *Client) Touch(ctx context.Context, key []byte, expiry Exptime) error {
	key = c.nsBytes(key)
	if err := validateKeyAndValue(key, nil); err != nil {
		return err
	}

	req, resp := buildTouchCommand(key, expiry, false)
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_TouchedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
