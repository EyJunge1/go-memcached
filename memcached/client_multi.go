package memcached

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DeleteMissMode controls whether missing keys fail DeleteMulti.
// The zero value is invalid; callers must pass DeleteMissIgnore or DeleteMissStrict.
type DeleteMissMode int

const (
	// DeleteMissIgnore treats missing keys as success.
	DeleteMissIgnore DeleteMissMode = iota + 1
	// DeleteMissStrict returns an error when any key is missing.
	DeleteMissStrict
)

func validDeleteMissMode(misses DeleteMissMode) error {
	switch misses {
	case DeleteMissIgnore, DeleteMissStrict:
		return nil
	default:
		return fmt.Errorf("delete miss mode: %w", ErrInvalidArgument)
	}
}

// StoreMulti stores items via Meta quiet sets + mn barrier.
// dst must have len(items) and receives per-item outcomes.
// Partial protocol failures are joined with errors.Join; transport failures are
// the Pipeline error. Item.Quiet is ignored (always quiet on the wire).
// Keys and values are borrowed until StoreMulti returns (same as Meta().Pipeline).
func (m *MetaClient) StoreMulti(ctx context.Context, items []Item, dst []MetaResult) error {
	if _, err := m.boundClient(); err != nil {
		return err
	}
	if len(items) != len(dst) {
		return fmt.Errorf("meta store multi dst length %d != items %d: %w", len(dst), len(items), ErrInvalidArgument)
	}
	if len(items) == 0 {
		return nil
	}
	ops, err := metaStoreMultiOps(items)
	if err != nil {
		return err
	}
	err = m.Pipeline(ctx, ops, dst)
	return joinMetaResults(err, dst, false /* ignoreNotFound */)
}

// DeleteMulti deletes keys via Meta quiet deletes + mn barrier.
// dst must have len(keys) and receives per-item outcomes.
// Keys are borrowed until DeleteMulti returns (same as Meta().Pipeline).
func (m *MetaClient) DeleteMulti(ctx context.Context, keys [][]byte, misses DeleteMissMode, dst []MetaResult) error {
	if _, err := m.boundClient(); err != nil {
		return err
	}
	if err := validDeleteMissMode(misses); err != nil {
		return err
	}
	if len(keys) != len(dst) {
		return fmt.Errorf("meta delete multi dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	err := m.Pipeline(ctx, metaDeleteMultiOps(keys), dst)
	return joinMetaResults(err, dst, misses != DeleteMissStrict)
}

// GetMulti fetches keys via Meta quiet gets + opaque + mn barrier.
// dst must have len(keys); misses leave Status EN/NF without a method error.
// Keys are borrowed until GetMulti returns (same as Meta().Pipeline).
func (m *MetaClient) GetMulti(ctx context.Context, keys [][]byte, dst []MetaResult, f MetaGet) error {
	if _, err := m.boundClient(); err != nil {
		return err
	}
	if len(keys) != len(dst) {
		return fmt.Errorf("meta get multi dst length %d != keys %d: %w", len(dst), len(keys), ErrInvalidArgument)
	}
	if len(keys) == 0 {
		return nil
	}
	return m.Pipeline(ctx, metaGetMultiOps(keys, f), dst)
}

// FillFunc loads a value when GetOrFill wins the recache/vivify race.
type FillFunc func(ctx context.Context, key []byte) (value []byte, flags uint32, exp Exptime, err error)

// GetOrFillOpts configures GetOrFill.
type GetOrFillOpts struct {
	// Vivify is the create-on-miss TTL (N). Required for miss→win races.
	Vivify Exptime
	// Recache is the remaining-TTL window (R). Zero omits R.
	Recache time.Duration
	// Touch optionally refreshes TTL on hit.
	Touch *Exptime
}

// GetOrFill performs mg with vivify/recache; when Won, calls fill and ms (CAS when available).
// On miss without win, dst reflects the meta miss. fill errors are returned to the caller.
func (m *MetaClient) GetOrFill(
	ctx context.Context, key []byte, dst *MetaResult, opts GetOrFillOpts, fill FillFunc,
) error {
	return metaGetOrFill(ctx, m, key, dst, opts, fill)
}

type metaGetSetter interface {
	Get(ctx context.Context, key []byte, dst *MetaResult, f MetaGet) error
	Set(ctx context.Context, key, value []byte, dst *MetaResult, f MetaSet) error
}

var (
	_ metaGetSetter = (*MetaClient)(nil)
	_ metaGetSetter = (*NodeMetaClient)(nil)
)

func metaGetOrFill(
	ctx context.Context,
	m metaGetSetter,
	key []byte,
	dst *MetaResult,
	opts GetOrFillOpts,
	fill FillFunc,
) error {
	if dst == nil {
		return fmt.Errorf("get-or-fill dst: %w", ErrInvalidArgument)
	}
	if fill == nil {
		return fmt.Errorf("get-or-fill fill: %w", ErrInvalidArgument)
	}
	vivify := opts.Vivify
	g := MetaGet{
		Vivify:  &vivify,
		Recache: opts.Recache,
		Touch:   opts.Touch,
		Return:  MetaGetReturn{Value: true, CAS: true, Flags: true, TTL: true},
	}
	if err := m.Get(ctx, key, dst, g); err != nil {
		return err
	}
	if !dst.Item.Won {
		return nil
	}
	value, flags, exp, err := fill(ctx, key)
	if err != nil {
		return err
	}
	ms := MetaSet{Flags: flags, TTL: &exp, Return: MetaSetReturn{CAS: true}}
	if dst.Item.CAS != 0 {
		cas := dst.Item.CAS
		ms.CompareCAS = &cas
	}
	return m.Set(ctx, key, value, dst, ms)
}

func metaGetMultiOps(keys [][]byte, f MetaGet) []MetaOp {
	f.Quiet = true
	if !f.Return.Value && !f.Return.CAS && !f.Return.Flags && !f.Return.TTL &&
		!f.Return.HitBefore && !f.Return.Key && !f.Return.LastAccess && !f.Return.Size {
		f.Return.Value = true
	}
	ops := make([]MetaOp, len(keys))
	for i := range keys {
		gf := f
		ops[i] = MetaOp{Key: keys[i], Get: &gf}
	}
	return ops
}

// itemToMetaSet maps a text Item onto MetaSet flags (shared by Store / StoreMulti).
func itemToMetaSet(it Item) (MetaSet, error) {
	mode, err := storeModeToMeta(it.Mode)
	if err != nil {
		return MetaSet{}, err
	}
	ttl := it.Exp
	ms := MetaSet{
		Quiet: it.Quiet,
		Flags: it.Flags,
		Mode:  mode,
		TTL:   &ttl,
	}
	if it.Mode == StoreCAS {
		cas := it.CAS
		ms.CompareCAS = &cas
	}
	return ms, nil
}

func metaStoreMultiOps(items []Item) ([]MetaOp, error) {
	ops := make([]MetaOp, len(items))
	for i := range items {
		it := items[i]
		ms, err := itemToMetaSet(it)
		if err != nil {
			return nil, err
		}
		ms.Quiet = true // StoreMulti is always quiet on the wire.
		ops[i] = MetaOp{Key: it.Key, Body: it.Value, Set: &ms}
	}
	return ops, nil
}

func metaDeleteMultiOps(keys [][]byte) []MetaOp {
	ops := make([]MetaOp, len(keys))
	for i := range keys {
		ops[i] = MetaOp{Key: keys[i], Delete: &MetaDelete{Quiet: true}}
	}
	return ops
}

func storeModeToMeta(mode StoreMode) (MetaSetMode, error) {
	switch mode {
	case 0, StoreSet:
		return MetaSetModeSet, nil
	case StoreAdd:
		return MetaSetModeAdd, nil
	case StoreReplace:
		return MetaSetModeReplace, nil
	case StoreAppend:
		return MetaSetModeAppend, nil
	case StorePrepend:
		return MetaSetModePrepend, nil
	case StoreCAS:
		return MetaSetModeSet, nil
	default:
		return 0, fmt.Errorf("store mode: %w", ErrInvalidArgument)
	}
}

func joinMetaResults(pipeErr error, dst []MetaResult, ignoreNotFound bool) error {
	errs := make([]error, 0, 1+len(dst))
	if pipeErr != nil {
		errs = append(errs, pipeErr)
	}
	for i := range dst {
		err := dst[i].Err()
		if err == nil {
			continue
		}
		if ignoreNotFound && errors.Is(err, ErrNotFound) {
			continue
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
