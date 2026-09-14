package memcached

import (
	"context"
	"fmt"
)

// Noop sends mn and writes the outcome into dst. dst must be non-nil.
func (m *MetaClient) Noop(ctx context.Context, f MetaNoop, dst *MetaResult) error {
	keep, err := prepareMetaDest(dst)
	if err != nil {
		return err
	}
	if err := f.validate(); err != nil {
		return err
	}
	req, resp := buildMetaNoopCommand(f)
	defer releaseReqAndResp(req, resp)

	if err := m.c.dispatchRequest(ctx, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	if err := mapStatusLine(resp, _MetaMNCRLFBytes); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	dst.Status = MetaStatusMN
	return nil
}
