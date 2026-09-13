package memcached

import "errors"

// MetaClient is the mg/ms/md/mn/ma/me API. Get it from Client.Meta.
type MetaClient struct {
	c *Client
}

// Meta returns the meta-protocol handle. No Config flag is required.
func (c *Client) Meta() *MetaClient {
	return &MetaClient{c: c}
}

// finishMeta returns a MetaResult from item, treating protocol status errors as success.
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
