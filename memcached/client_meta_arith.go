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
func (m *MetaClient) Arithmetic(
	ctx context.Context,
	key []byte,
	delta uint64,
	dst *MetaResult,
	f MetaArithmetic,
) error {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaArithmetic(c, key, delta, dst, f)
	if err != nil {
		return err
	}
	return m.finishMetaDest(
		ctx, req, resp, dst, keep,
		quiet, false /* quietMiss */, returnKey, userKey,
	)
}

// prepareMetaArithmetic validates flags and builds the ma request.
// On success the caller owns req/resp (via finishMetaDest / finishMetaDestOnAddr).
func prepareMetaArithmetic(
	c *Client,
	key []byte,
	delta uint64,
	dst *MetaResult,
	f MetaArithmetic,
) (req *request, resp *response, keep metaKeep, userKey []byte, quiet, returnKey bool, err error) {
	keep, err = prepareMetaDest(dst)
	if err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if err = f.validate(); err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	maFlags := f.toFlags()
	userKey = key
	wired, keyBox := c.nsKey(key)
	if err = c.checkKeyValueFlags(wired, nil, maFlags.b); err != nil {
		putBuilderBox(keyBox)
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if !validMetaArithmeticMode(maFlags.M) {
		putBuilderBox(keyBox)
		return nil, nil, metaKeep{}, nil, false, false, fmt.Errorf("invalid meta arithmetic mode: %w", ErrInvalidArgument)
	}
	if maFlags.OtokenSet {
		if err = validOpaqueToken(maFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return nil, nil, metaKeep{}, nil, false, false, err
		}
	}
	req, resp = buildMetaArithmeticCommand(wired, delta, &maFlags)
	req.adoptKeyBox(keyBox)
	return req, resp, keep, userKey, maFlags.q, maFlags.k, nil
}
