package memcached

import (
	"errors"
	"fmt"
)

// MetaClient is the mg/ms/md/mn/ma/me API. Get it from Client.Meta.
type MetaClient struct {
	c *Client
}

// Meta returns the meta-protocol handle. No Config flag is required.
func (c *Client) Meta() *MetaClient {
	if c == nil {
		return nil
	}
	return &c.meta
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

func finishMetaParse(item *MetaItem, dispatchErr, parseErr error) (MetaResult, error) {
	if dispatchErr != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", dispatchErr)
	}
	res, err := finishMeta(item, parseErr)
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	return res, nil
}
