package memcached

import (
	"context"
	"fmt"
)

// Debug sends me. A protocol miss is MetaItemDebug.Err, not the returned error.
func (m *MetaClient) Debug(ctx context.Context, key []byte, f MetaDebug) (MetaItemDebug, error) {
	c := m.c
	if err := f.validate(); err != nil {
		return MetaItemDebug{}, err
	}
	mdFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		return MetaItemDebug{}, err
	}

	req, resp := buildMetaDebugCommand(key, mdFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItemDebug{Key: key}
	resp.afterRecv = func(r *response) error {
		return parseMetaItemDebug(r.rawLines, item, mdFlags.b)
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		if item.Status.Err() != nil {
			return *item, nil
		}
		return MetaItemDebug{}, fmt.Errorf("request failed: %w", err)
	}
	return *item, nil
}
