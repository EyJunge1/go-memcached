package memcached

import (
	"context"
	"fmt"
)

// Debug sends me and writes the outcome into dst. dst must be non-nil.
// A protocol miss is dst.Err / Status, not the returned error.
func (m *MetaClient) Debug(ctx context.Context, key []byte, f MetaDebug, dst *MetaItemDebug) error {
	if dst == nil {
		return fmt.Errorf("meta debug dst: %w", ErrInvalidArgument)
	}
	c := m.c
	if err := f.validate(); err != nil {
		return err
	}
	mdFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		return err
	}

	*dst = MetaItemDebug{Key: key}
	req, resp := buildMetaDebugCommand(key, &mdFlags)
	defer releaseReqAndResp(req, resp)

	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		*dst = MetaItemDebug{}
		return fmt.Errorf("request failed: %w", err)
	}
	if err := parseMetaItemDebug(resp.rawLines, dst, mdFlags.b); err != nil {
		if dst.Status.Err() != nil {
			return nil
		}
		*dst = MetaItemDebug{}
		return fmt.Errorf("request failed: %w", err)
	}
	return nil
}
