package memcached

import (
	"context"
)

// Delete sends md for key using flags f and writes the outcome into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path.
func (m *MetaClient) Delete(ctx context.Context, key []byte, f MetaDelete, dst *MetaResult) error {
	c := m.c
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	mdFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		return err
	}
	if mdFlags.OtokenSet {
		if err := validOpaqueToken(mdFlags.Otoken); err != nil {
			return err
		}
	}
	dst.Item.Key = key

	req, resp := buildMetaDeleteCommand(key, &mdFlags)
	return m.finishMetaDest(ctx, req, resp, dst, keep, mdFlags.q, false)
}
