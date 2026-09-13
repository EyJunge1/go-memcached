package memcached

import (
	"context"
	"fmt"
)

// Delete issues md with the given flags.
func (m *MetaClient) Delete(ctx context.Context, key []byte, f MetaDelete) (MetaResult, error) {
	c := m.c
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
	res, err := finishMeta(item, c.dispatchRequest(ctx, req, resp))
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	return res, nil
}
