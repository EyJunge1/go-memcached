package memcached

import (
	"context"
	"fmt"
)

// Arithmetic sends ma for key using flags f and writes the outcome into dst.
// dst must be non-nil. If dst.Item.Value has enough cap, a VA body is written
// there. Reusing dst is the zero-allocation path.
// Key is copied only when f.Return.Key is set.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Arithmetic(ctx context.Context, key []byte, delta uint64, dst *MetaResult, f MetaArithmetic) error {
	c := m.c
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	maFlags := f.toFlags()
	userKey := key
	key, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(key, nil, maFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		putBuilderBox(keyBox)
		return fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return err
		}
	}

	req, resp := buildMetaArithmeticCommand(key, delta, &maFlags)
	req.adoptKeyBox(keyBox)
	return m.finishMetaDest(ctx, req, resp, dst, keep, maFlags.q, false, maFlags.k, userKey)
}
