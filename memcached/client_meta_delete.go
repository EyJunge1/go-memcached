package memcached

import (
	"context"
)

// Delete sends md for key using flags f and writes the outcome into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path.
// Key is copied only when f.Return.Key is set.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Delete(ctx context.Context, key []byte, dst *MetaResult, f MetaDelete) error {
	c := m.c
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	mdFlags := f.toFlags()
	userKey := key
	key, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(key, nil, mdFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	if mdFlags.OtokenSet {
		if err := validOpaqueToken(mdFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return err
		}
	}

	req, resp := buildMetaDeleteCommand(key, &mdFlags)
	req.adoptKeyBox(keyBox)
	return m.finishMetaDest(ctx, req, resp, dst, keep, mdFlags.q, false, mdFlags.k, userKey)
}
