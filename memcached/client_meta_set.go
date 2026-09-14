package memcached

import (
	"context"
	"fmt"
)

// Set sends ms for key using flags f and writes the outcome into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path.
// Key is copied only when f.Return.Key is set.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Set(ctx context.Context, key, value []byte, dst *MetaResult, f MetaSet) error {
	c := m.c
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	msFlags := f.toFlags()
	userKey := key
	key, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(key, value, msFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	if !validMetaSetMode(msFlags.M) {
		putBuilderBox(keyBox)
		return fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if msFlags.OtokenSet {
		if err := validOpaqueToken(msFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return err
		}
	}
	dst.Item.TTL = msFlags.T
	dst.Item.Flags = msFlags.F

	req, resp := buildMetaSetCommand(key, value, &msFlags)
	req.adoptKeyBox(keyBox)
	return m.finishMetaDest(ctx, req, resp, dst, keep, msFlags.q, false, msFlags.k, userKey)
}
