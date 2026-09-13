package memcached

import (
	"context"
	"fmt"
)

// Noop sends mn.
func (m *MetaClient) Noop(ctx context.Context, f MetaNoop) (MetaResult, error) {
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	req, resp := buildMetaNoopCommand(f)
	defer releaseReqAndResp(req, resp)

	item := MetaItem{}
	err := m.c.dispatchRequest(ctx, req, resp)
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	if err := mapStatusLine(resp, _MetaMNCRLFBytes); err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	item.status = MetaStatusMN
	res, err := finishMeta(&item, nil)
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	if res.Status == MetaStatusUnknown {
		res.Status = MetaStatusMN
	}
	return res, nil
}
