package memcached

import (
	"context"
	"fmt"
)

// MetaDelete deletes a key with meta flags.
func (c *Client) MetaDelete(ctx context.Context, key []byte, f MetaDelete) (MetaResult, error) {
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	mdFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		return MetaResult{}, err
	}
	if mdFlags.OtokenSet {
		if err := validOpaqueToken(mdFlags.Otoken); err != nil {
			return MetaResult{}, err
		}
	}

	req, resp := buildMetaDeleteCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{Key: key}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, mdFlags.q, false)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return metaResultWithErr(item, item.Status, err), fmt.Errorf("request failed: %w", err)
	}
	return metaResultFrom(item, item.Status), nil
}
