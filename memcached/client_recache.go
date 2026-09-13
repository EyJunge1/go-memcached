package memcached

import "context"

// GetOrVivify is mg with N. won is true when the server returned W.
func (c *Client) GetOrVivify(ctx context.Context, key []byte, ttl Exptime) (MetaResult, bool, error) {
	res, err := c.MetaGet(ctx, key, MetaGet{
		Vivify: &ttl,
		Return: MetaGetReturn{Value: true},
	})
	return res, res.Item.Won, err
}

// GetEarlyRecache is mg with R. won is true when the server returned W.
func (c *Client) GetEarlyRecache(ctx context.Context, key []byte, window Exptime) (MetaResult, bool, error) {
	res, err := c.MetaGet(ctx, key, MetaGet{
		Recache: &window,
		Return:  MetaGetReturn{Value: true},
	})
	return res, res.Item.Won, err
}

// Invalidate is md with I and T (stale-for TTL).
func (c *Client) Invalidate(ctx context.Context, key []byte, staleFor Exptime) error {
	_, err := c.MetaDelete(ctx, key, MetaDelete{
		Invalidate: true,
		TTL:        &staleFor,
	})
	return err
}
