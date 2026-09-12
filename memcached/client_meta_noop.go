package memcached

import (
	"context"
	"fmt"
)

// Noop sends mn and writes the outcome into dst. dst must be non-nil.
// Transport and parse failures are the method error.
func (m *MetaClient) Noop(ctx context.Context, dst *MetaResult, f MetaNoop) error {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	return c.metaNoop(ctx, nil, dst, f)
}

// metaNoop sends mn. node nil uses the first live node; non-nil pins the node.
func (c *Client) metaNoop(ctx context.Context, node *addr, dst *MetaResult, f MetaNoop) error {
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
	if node != nil {
		err = c.dispatchToAddr(ctx, node, req, resp)
	} else {
		err = c.dispatchRequest(ctx, req, resp)
	}
	if err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request: %w", err)
	}
	dst.Status = MetaStatusMN
	return nil
}

func mapMetaNoopReply(resp *response) error {
	return mapStatusLine(resp, _MetaMNCRLFBytes)
}
