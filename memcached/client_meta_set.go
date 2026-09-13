package memcached

import (
	"context"
	"fmt"
)

// Set issues ms with the given flags.
func (m *MetaClient) Set(ctx context.Context, key, value []byte, f MetaSet) (MetaResult, error) {
	c := m.c
	if err := f.validate(); err != nil {
		return MetaResult{}, err
	}
	msFlags := f.toFlags()
	key = c.nsBytes(key)
	if err := c.checkKeyValueFlags(key, value, msFlags.b); err != nil {
		return MetaResult{}, err
	}
	if !validMetaSetMode(msFlags.M) {
		return MetaResult{}, fmt.Errorf("invalid meta set mode: %w", ErrInvalidArgument)
	}
	if msFlags.OtokenSet {
		if err := validOpaqueToken(msFlags.Otoken); err != nil {
			return MetaResult{}, err
		}
	}
	clientFlags := msFlags.F

	req, resp := buildMetaSetCommand(key, value, msFlags)
	defer releaseReqAndResp(req, resp)

	item := &MetaItem{
		Key:   key,
		TTL:   msFlags.T,
		Flags: clientFlags,
	}
	resp.afterRecv = func(r *response) error {
		return parseMetaItem(r.rawLines, item, msFlags.q, false)
	}
	res, err := finishMeta(item, c.dispatchRequest(ctx, req, resp))
	if err != nil {
		return MetaResult{}, fmt.Errorf("request failed: %w", err)
	}
	return res, nil
}
