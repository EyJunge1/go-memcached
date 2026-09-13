package memcached

import (
	"context"
	"fmt"
)

// MetaDebug returns debug metadata for a key.
func (c *Client) MetaDebug(ctx context.Context, key []byte, f MetaDebug) (*MetaItemDebug, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	mdFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaDebugCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItemDebug{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItemDebug(r.rawLines, item, mdFlags.b)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		if item.Status != MetaStatusUnknown {
			return item, fmt.Errorf("request failed: %w", err)
		}
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}
