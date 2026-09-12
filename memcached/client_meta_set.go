package memcached

import (
	"context"
	"fmt"
	"time"
)

// Set sends ms for key using flags f and writes the outcome into dst.
// dst must be non-nil. Reusing dst is the zero-allocation path.
// Key is copied only when f.Return.Key is set.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil.
func (m *MetaClient) Set(ctx context.Context, key, value []byte, dst *MetaResult, f MetaSet) error {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaSet(c, key, value, dst, f)
	if err != nil {
		return err
	}
	return m.finishMetaDest(ctx, req, resp, dst, keep, quiet, false /* quietMiss */, returnKey, userKey)
}

// Store maps item onto Meta Set (parity with Client.Store / StoreMulti).
// Item.Quiet maps to MetaSet.Quiet. Protocol outcomes land on dst.
func (m *MetaClient) Store(ctx context.Context, item Item, dst *MetaResult) error {
	f, err := itemToMetaSet(item)
	if err != nil {
		return err
	}
	return m.Set(ctx, item.Key, item.Value, dst, f)
}

// prepareMetaSet validates flags and builds the ms request.
// On success the caller owns req/resp (via finishMetaDest / finishMetaDestOnAddr).
func prepareMetaSet(
	c *Client,
	key, value []byte,
	dst *MetaResult,
	f MetaSet,
) (req *request, resp *response, keep metaKeep, userKey []byte, quiet, returnKey bool, err error) {
	keep, err = prepareMetaDest(dst)
	if err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if err = f.validate(); err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	msFlags := f.toFlags()
	userKey = key
	wired, keyBox := c.nsKey(key)
	if err = c.checkKeyValueFlags(wired, value, msFlags.b); err != nil {
		putBuilderBox(keyBox)
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if !validMetaSetMode(msFlags.M) {
		putBuilderBox(keyBox)
		return nil, nil, metaKeep{}, nil, false, false, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if msFlags.OtokenSet {
		if err = validOpaqueToken(msFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return nil, nil, metaKeep{}, nil, false, false, err
		}
	}
	dst.Item.TTL = time.Duration(msFlags.T) * time.Second
	dst.Item.Flags = msFlags.F
	req, resp = buildMetaSetCommand(wired, value, &msFlags)
	req.adoptKeyBox(keyBox)
	return req, resp, keep, userKey, msFlags.q, msFlags.k, nil
}
