package memcached

import (
	"context"
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

type metaKeep struct {
	value []byte
	key   []byte
}

func takeMetaKeep(dst *MetaResult) metaKeep {
	keep := metaKeep{value: dst.Item.Value, key: dst.Item.WireKey}
	*dst = MetaResult{}
	restoreMetaKeep(dst, keep)
	return keep
}

func restoreMetaKeep(dst *MetaResult, keep metaKeep) {
	if keep.value != nil {
		dst.Item.Value = keep.value[:0]
	}
	if keep.key != nil {
		dst.Item.WireKey = keep.key[:0]
	}
}

func prepareMetaDest(dst *MetaResult) (metaKeep, error) {
	if dst == nil {
		return metaKeep{}, fmt.Errorf("meta dst: %w", ErrInvalidArgument)
	}
	return takeMetaKeep(dst), nil
}

func (m *MetaClient) finishMetaDest(ctx context.Context, req *request, resp *response, dst *MetaResult, keep metaKeep, quiet, quietMiss bool) error {
	resp.metaItem = &dst.Item
	defer releaseReqAndResp(req, resp)
	if err := m.c.dispatchRequest(ctx, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	if err := resp.metaStatusErr(quiet, quietMiss); err != nil {
		if cd := dst.Item.status.Err(); cd != nil && errors.Is(err, cd) {
			applyMetaDestStatus(dst, keep)
			return nil
		}
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	applyMetaDestStatus(dst, keep)
	return nil
}

func applyMetaDestStatus(dst *MetaResult, keep metaKeep) {
	dst.Status = dst.Item.status
	dst.Item.status = 0
	if dst.Status != MetaStatusVA {
		if keep.value != nil {
			dst.Item.Value = keep.value[:0]
		} else {
			dst.Item.Value = nil
		}
	}
}

func (resp *response) metaStatusErr(noReply, quietMissAsNotFound bool) error {
	item := resp.metaItem
	if item == nil {
		item = &resp.metaSlot
	}
	if resp.metaFilled {
		return metaItemStatusErr(item, noReply, quietMissAsNotFound)
	}
	return parseMetaItem(resp.rawLines, item, noReply, quietMissAsNotFound)
}
