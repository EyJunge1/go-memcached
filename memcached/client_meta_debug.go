package memcached

import (
	"context"
	"fmt"
)

// MetaDebug returns debug metadata for a key.
func (c *Client) MetaDebug(ctx context.Context, key []byte, options ...MetaDebugOption) (*MetaItemDebug, error) {
	mdFlags := &metaDebugFlags{}
	for _, applyFn := range options {
		applyFn(mdFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}

	req, resp := buildMetaDebugCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItemDebug{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItemDebug(r.rawLines, item)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}
