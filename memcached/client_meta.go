package memcached

import "errors"

// MetaClient is the meta-protocol command surface. Obtain it via Client.Meta.
type MetaClient struct {
	c *Client
}

// Meta returns the meta-protocol handle. It is always available.
func (c *Client) Meta() *MetaClient {
	return &MetaClient{c: c}
}

func finishMeta(item *MetaItem, err error) (MetaResult, error) {
	if err != nil {
		if item != nil {
			if cd := item.status.Err(); cd != nil && errors.Is(err, cd) {
				return metaResultFrom(item, item.status), nil
			}
		}
		return MetaResult{}, err
	}
	st := MetaStatusUnknown
	if item != nil {
		st = item.status
	}
	return metaResultFrom(item, st), nil
}
