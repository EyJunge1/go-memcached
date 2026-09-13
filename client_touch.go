package memcached

import (
	"context"
	"fmt"
	"time"
)

// Touch updates expiration without fetching the item.
func (c *Client) Touch(ctx context.Context, key string, expiry time.Duration, opts ...ItemWriteOption) error {
	key = c.nsString(key)
	if err := validateKeyAndValue([]byte(key), nil); err != nil {
		return err
	}

	req, resp := buildTouchCommand(key, expiry, c.resolveNoReply(opts))
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = expectLine(_TouchedCRLFBytes)
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return fmt.Errorf("request failed: %w", err)
	}

	return nil
}
