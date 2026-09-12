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
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	req, resp, keep, userKey, quiet, returnKey, err := prepareMetaDelete(c, key, dst, f)
	if err != nil {
		return err
	}
	return m.finishMetaDest(ctx, req, resp, dst, keep, quiet, false /* quietMiss */, returnKey, userKey)
}

// prepareMetaDelete validates flags and builds the md request.
// On success the caller owns req/resp (via finishMetaDest / finishMetaDestOnAddr).
func prepareMetaDelete(
	c *Client,
	key []byte,
	dst *MetaResult,
	f MetaDelete,
) (req *request, resp *response, keep metaKeep, userKey []byte, quiet, returnKey bool, err error) {
	keep, err = prepareMetaDest(dst)
	if err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if err = f.validate(); err != nil {
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	mdFlags := f.toFlags()
	userKey = key
	wired, keyBox := c.nsKey(key)
	if err = c.checkKeyValueFlags(wired, nil, mdFlags.b); err != nil {
		putBuilderBox(keyBox)
		return nil, nil, metaKeep{}, nil, false, false, err
	}
	if mdFlags.OtokenSet {
		if err = validOpaqueToken(mdFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return nil, nil, metaKeep{}, nil, false, false, err
		}
	}
	req, resp = buildMetaDeleteCommand(wired, &mdFlags)
	req.adoptKeyBox(keyBox)
	return req, resp, keep, userKey, mdFlags.q, mdFlags.k, nil
}
