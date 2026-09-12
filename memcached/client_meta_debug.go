package memcached

import (
	"context"
	"fmt"
)

// Debug sends me and writes the outcome into dst. dst must be non-nil.
// Transport and parse failures are the method error. A protocol miss lives
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Debug(
	ctx context.Context,
	key []byte,
	dst *MetaItemDebug,
	f MetaDebug,
) error {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	return c.metaDebug(ctx, nil, key, dst, f)
}

// metaDebug sends me. node nil routes by key; non-nil pins the node.
func (c *Client) metaDebug(
	ctx context.Context,
	node *addr,
	key []byte,
	dst *MetaItemDebug,
	f MetaDebug,
) error {
	if dst == nil {
		return fmt.Errorf("meta debug dst: %w", ErrInvalidArgument)
	}
	if err := f.validate(); err != nil {
		return err
	}
	mdFlags := f.toFlags()
	wired, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(wired, nil, mdFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}

	keepKey := dst.WireKey
	keepUser := dst.Key
	keepFields := dst.Fields
	*dst = MetaItemDebug{WireKey: keepKey[:0], Key: keepUser[:0], Fields: keepFields[:0]}
	req, resp := buildMetaDebugCommand(wired, &mdFlags)
	req.adoptKeyBox(keyBox)
	defer releaseReqAndResp(req, resp)

	resp.debugDst = dst
	resp.debugBinary = mdFlags.b
	resp.afterRecv = mapDebugReply
	var err error
	if node != nil {
		err = c.dispatchToAddr(ctx, node, req, resp)
	} else {
		err = c.dispatchRequest(ctx, req, resp)
	}
	if err != nil {
		*dst = MetaItemDebug{WireKey: keepKey[:0], Key: keepUser[:0], Fields: keepFields[:0]}
		return fmt.Errorf("request: %w", err)
	}
	src := dst.WireKey
	if dst.KeyBase64 {
		src = dst.Key
	}
	dst.Key = append(keepUser[:0], c.stripNS(src)...)
	return nil
}
