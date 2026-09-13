package memcached

import (
	"context"
	"fmt"
)

// Arithmetic sends ma for key using flags f.
func (m *MetaClient) Arithmetic(ctx context.Context, key []byte, delta uint64, f MetaArithmetic) (MetaResult, error) {
	c := m.c
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	maFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, nil, maFlags.b); err != nil {
		return MetaResult{}, err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		return MetaResult{}, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err := validOpaqueToken(maFlags.Otoken); err != nil {
			return MetaResult{}, err
		}
	}

	req, resp := buildMetaArithmeticCommand(key, delta, &maFlags)
	defer releaseReqAndResp(req, resp)

	item := MetaItem{Key: key}
	return finishMetaParse(&item, c.dispatchRequest(ctx, req, resp), parseMetaItem(resp.rawLines, &item, maFlags.q, false))
}
