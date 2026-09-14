package memcached

import (
	"context"
	"fmt"
)

// Arithmetic sends ma for key using flags f and writes the outcome into dst.
// dst must be non-nil. If dst.Item.Value has enough cap, a VA body is written
// there. Reusing dst is the zero-allocation path.
func (m *MetaClient) Arithmetic(ctx context.Context, key []byte, delta uint64, f MetaArithmetic, dst *MetaResult) error {
	c := m.c
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	maFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, maFlags.b); err != nil {
		return err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		return fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			return err
		}
	}
	dst.Item.Key = key

	req, resp := buildMetaArithmeticCommand(key, delta, &maFlags)
	return m.finishMetaDest(ctx, req, resp, dst, keep, maFlags.q, false)
}
