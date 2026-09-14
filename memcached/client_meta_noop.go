package memcached

import (
	"context"
	"fmt"
)

// Noop sends mn and writes the outcome into dst. dst must be non-nil.
// Transport and parse failures are the method error.
func (m *MetaClient) Noop(ctx context.Context, dst *MetaResult, f MetaNoop) error {
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	req, resp := buildMetaNoopCommand(f)
	defer releaseReqAndResp(req, resp)

	resp.afterRecv = mapMetaNoopReply
	if err := m.c.dispatchRequest(ctx, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	dst.Status = MetaStatusMN
	return nil
}

func mapMetaNoopReply(resp *response) error {
	return mapStatusLine(resp, _MetaMNCRLFBytes)
}
