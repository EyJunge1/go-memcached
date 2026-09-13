package memcached

import (
	"context"
	"fmt"
)

// MetaNoop sends a meta no-op and returns MN.
func (c *Client) MetaNoop(ctx context.Context, f MetaNoop) (MetaResult, error) {
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	req, resp := buildMetaNoopCommand(f)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{}
	resp.afterRecv = func(r *response) error {
		if err := expectLine(_MetaMNCRLFBytes)(r); err != nil {
			return err
		}
		item.Status = MetaStatusMN
		return nil
	}
	if err := c.dispatchRequest(ctx, req, resp); err != nil {
		return metaResultWithErr(item, item.Status, err), fmt.Errorf("request failed: %w", err)
	}
	return metaResultFrom(item, MetaStatusMN), nil
}
