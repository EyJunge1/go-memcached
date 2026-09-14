package memcached

import (
	"context"
	"fmt"
)

// Debug sends me and writes the outcome into dst. dst must be non-nil.
// Transport and parse failures are the method error. A protocol miss lives
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Debug(ctx context.Context, key []byte, dst *MetaItemDebug, f MetaDebug) error {
	if dst == nil {
		return fmt.Errorf("meta debug dst: %w", ErrInvalidArgument)
	}
	c := m.c
	if err := f.validate(); err != nil {
		return err
	}
	mdFlags := f.toFlags()
	key, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	keepKey := dst.WireKey
	keepUser := dst.Key
	keepFields := dst.Fields
	*dst = MetaItemDebug{WireKey: keepKey[:0], Key: keepUser[:0], Fields: keepFields[:0]}
	req, resp := buildMetaDebugCommand(key, &mdFlags)
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	resp.debugDst = dst
	resp.debugBinary = mdFlags.b
	resp.afterRecv = mapDebugReply
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		*dst = MetaItemDebug{WireKey: keepKey[:0], Key: keepUser[:0], Fields: keepFields[:0]}
		return fmt.Errorf("request failed: %w", err)
	}
	if dst.KeyBase64 {
		dst.Key = append(keepUser[:0], c.stripNS(dst.Key)...)
	} else {
		dst.Key = append(keepUser[:0], c.stripNS(dst.WireKey)...)
	}
	return nil
}
