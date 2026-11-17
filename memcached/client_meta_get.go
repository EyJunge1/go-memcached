package memcached

import (
	"context"
	"errors"
	"fmt"
)

// Get sends mg for key using flags f and writes the outcome into dst.
// dst must be non-nil. If dst.Item.Value has enough cap, a VA body is written
// there. Key is copied only when f.Return.Key is set.
// Transport and parse failures are the method error. EN, NF, NS, and EX live
// on dst.Status / dst.Err(); the method error stays nil (no ErrNotFound shim).
// Prefer MetaGetItem when a following CAS write must re-apply TTL via MetaSet.TTL.
func (m *MetaClient) Get(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error {
	c, err := m.boundClient()
	if err != nil {
		return err
	}
	return c.metaGet(ctx, nil, key, dst, f)
}

// GetOne is an alias for Get (parity with Client.GetOne).
func (m *MetaClient) GetOne(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error {
	return m.Get(ctx, key, dst, f)
}

// metaGet sends mg. node nil routes by key; non-nil pins the node.
func (c *Client) metaGet(ctx context.Context, node *addr, key []byte, dst *MetaResult, f MetaGet) error {
	if dst == nil {
		return fmt.Errorf("meta get dst: %w", ErrInvalidArgument)
	}
	if err := f.validate(); err != nil {
		return err
	}
	mgFlags := f.toFlags()
	wired, keyBox := c.nsKey(key)
	if err := c.checkKeyValueFlags(wired, nil, mgFlags.b); err != nil {
		putBuilderBox(keyBox)
		return err
	}
	if mgFlags.OtokenSet {
		if err := validOpaqueToken(mgFlags.Otoken); err != nil {
			putBuilderBox(keyBox)
			return err
		}
	}

	keep := takeMetaKeep(dst)
	req, resp := buildMetaGetCommand(wired, &mgFlags)
	req.adoptKeyBox(keyBox)
	resp.metaItem = &dst.Item
	defer releaseReqAndResp(req, resp)

	var err error
	if node != nil {
		err = c.dispatchToAddr(ctx, node, req, resp)
	} else {
		err = c.dispatchRequest(ctx, req, resp)
	}
	if err != nil {
		resetMetaGetDest(dst, keep)
		return fmt.Errorf("request: %w", err)
	}
	if err := resp.metaStatusErr(mgFlags.q, true /* quietMissAsNotFound */); err != nil {
		if cd := dst.Item.status.Err(); cd == nil || !errors.Is(err, cd) {
			resetMetaGetDest(dst, keep)
			return fmt.Errorf("request: %w", err)
		}
	}
	applyMetaGetDest(dst, keep, c, wired, f, &mgFlags)
	return nil
}

func resetMetaGetDest(dst *MetaResult, keep metaKeep) {
	*dst = MetaResult{}
	restoreMetaKeep(dst, keep)
}

func applyMetaGetDest(dst *MetaResult, keep metaKeep, c *Client, key []byte, f MetaGet, flags *metaGetFlags) {
	item := &dst.Item
	applyCheckCAS(item, flags)
	if item.status != MetaStatusVA {
		item.Value = nil
		if keep.value != nil {
			item.Value = keep.value[:0]
		}
	}
	dst.Status = item.status
	item.status = 0
	if !f.Return.Key {
		item.Key = nil
		item.WireKey = nil
		if keep.key != nil {
			item.WireKey = keep.key[:0]
		}
		return
	}
	copyMetaUserKey(item, keep.userKey, c, c.stripNS(key), true /* returnKey */)
}

func metaItemStatusErr(item *MetaItem, noReply, quietMissAsNotFound bool) error {
	if item == nil {
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	}
	switch item.status {
	case MetaStatusNF, MetaStatusEN:
		return ErrNotFound
	case MetaStatusNS:
		return ErrNotStored
	case MetaStatusEX:
		return ErrExists
	case MetaStatusHD:
		item.gotHD = true
		return nil
	case MetaStatusVA, MetaStatusMN, MetaStatusME:
		return nil
	case MetaStatusUnknown, 0:
		// 0 is unset; MetaStatusUnknown is explicit. Quiet synthesis treats both alike.
		if noReply && len(item.Value) == 0 && !item.gotHD {
			if quietMissAsNotFound {
				item.status = MetaStatusEN
				return ErrNotFound
			}
			item.status = MetaStatusHD
			item.gotHD = true
			return nil
		}
		return fmt.Errorf("missing response: %w", ErrMalformedResponse)
	default:
		return nil
	}
}

// applyCheckCAS sets CASMatched when mg used C+v and the server returned HD.
// A reused dest buffer must not hide the match: HD has no VA body.
func applyCheckCAS(item *MetaItem, flags *metaGetFlags) {
	if item == nil || flags == nil {
		return
	}
	item.CASMatched = flags.Cset && flags.v && item.gotHD && item.status != MetaStatusVA
}
