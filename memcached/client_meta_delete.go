package memcached

import (
	"context"
	"fmt"
)

// MetaDelete deletes a key with meta flags.
func (c *Client) MetaDelete(ctx context.Context, key []byte, options ...MetaDeleteOption) (*MetaItem, error) {
	mdFlags := &metaDeleteFlags{}
	for _, applyFn := range options {
		applyFn(mdFlags)
	}
	key = c.nsBytes(key)
	if err := validateKeyAndValueFlags(key, nil, mdFlags.b); err != nil {
		return nil, err
	}
	if mdFlags.OtokenSet {
		if err := validOpaqueToken(mdFlags.Otoken); err != nil {
			return nil, err
		}
	}

	req, resp := buildMetaDeleteCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key: key,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, mdFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return item, nil
}
