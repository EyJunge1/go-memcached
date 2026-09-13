package memcached

import (
	"context"
	"fmt"
)

// Noop issues mn.
func (m *MetaClient) Noop(ctx context.Context, f MetaNoop) (MetaResult, error) {
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
		item.status = MetaStatusMN
		return nil
	}
	res, err := finishMeta(item, m.c.dispatchRequest(ctx, req, resp))
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	if res.Status == MetaStatusUnknown {
		res.Status = MetaStatusMN
	}
	return res, nil
}
