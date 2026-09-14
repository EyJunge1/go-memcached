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
	value   []byte
	key     []byte
	userKey []byte
	opaque  []byte
}

func takeMetaKeep(dst *MetaResult) metaKeep {
	keep := metaKeep{value: dst.Item.Value, key: dst.Item.WireKey, userKey: dst.Item.Key, opaque: dst.Item.Opaque}
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
	if keep.userKey != nil {
		dst.Item.Key = keep.userKey[:0]
	}
	if keep.opaque != nil {
		dst.Item.Opaque = keep.opaque[:0]
	}
}

// copyMetaUserKey writes the user-facing key into keepUser. It never aliases WireKey.
func copyMetaUserKey(item *MetaItem, keepUser []byte, c *Client, fallback []byte, returnKey bool) {
	if item == nil {
		return
	}
	if !returnKey {
		item.Key = nil
		return
	}
	if item.KeyBase64 && len(item.WireKey) > 0 {
		dec, err := base64DecodeInto(keepUser, item.WireKey)
		if err != nil {
			item.Key = append(keepUser[:0], fallback...)
			return
		}
		item.Key = append(keepUser[:0], c.stripNS(dec)...)
		return
	}
	src := fallback
	switch {
	case len(item.WireKey) > 0:
		src = c.stripNS(item.WireKey)
	case len(item.Key) > 0:
		src = c.stripNS(item.Key)
	}
	item.Key = append(keepUser[:0], src...)
}

func prepareMetaDest(dst *MetaResult) (metaKeep, error) {
	if dst == nil {
		return metaKeep{}, fmt.Errorf("meta dst: %w", ErrInvalidArgument)
	}
	return takeMetaKeep(dst), nil
}

func (m *MetaClient) finishMetaDest(ctx context.Context, req *request, resp *response, dst *MetaResult, keep metaKeep, quiet, quietMiss, returnKey bool, userKey []byte) error {
	resp.metaItem = &dst.Item
	defer releaseReqAndResp(req, resp)
	if err := m.c.dispatchRequest(ctx, req, resp); err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	if err := resp.metaStatusErr(quiet, quietMiss); err != nil {
		if cd := dst.Item.status.Err(); cd != nil && errors.Is(err, cd) {
			applyMetaMutateDest(dst, keep, m.c, userKey, returnKey)
			return nil
		}
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request failed: %w", err)
	}
	applyMetaMutateDest(dst, keep, m.c, userKey, returnKey)
	return nil
}

// applyMetaMutateDest publishes status and copies Key only when Return.Key is set.
// Key never shares keep.key unless we synthesize both Key and WireKey together.
func applyMetaMutateDest(dst *MetaResult, keep metaKeep, c *Client, userKey []byte, returnKey bool) {
	applyMetaDestStatus(dst, keep)
	item := &dst.Item
	if !returnKey {
		item.Key = nil
		if keep.key != nil {
			item.WireKey = keep.key[:0]
		} else {
			item.WireKey = nil
		}
		return
	}
	copyMetaUserKey(item, keep.userKey, c, userKey, true)
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
