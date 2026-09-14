package memcached

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

// MetaStatus is a two-letter meta reply (HD, VA, EN, NF, NS, EX, MN, ME).
type MetaStatus uint8

const (
	MetaStatusUnknown MetaStatus = iota
	MetaStatusHD
	MetaStatusVA
	MetaStatusEN
	MetaStatusNF
	MetaStatusNS
	MetaStatusEX
	MetaStatusMN
	MetaStatusME
)

// Err maps EN and NF to ErrNotFound, NS to ErrNotStored, and EX to ErrExists.
func (s MetaStatus) Err() error {
	switch s {
	case MetaStatusEN, MetaStatusNF:
		return ErrNotFound
	case MetaStatusNS:
		return ErrNotStored
	case MetaStatusEX:
		return ErrExists
	default:
		return nil
	}
}

// Err returns a transport or parse error, otherwise Status.Err.
func (r MetaResult) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Status.Err()
}

// String is the two-letter wire token, or "??" when the status is unknown.
func (s MetaStatus) String() string {
	switch s {
	case MetaStatusHD:
		return "HD"
	case MetaStatusVA:
		return "VA"
	case MetaStatusEN:
		return "EN"
	case MetaStatusNF:
		return "NF"
	case MetaStatusNS:
		return "NS"
	case MetaStatusEX:
		return "EX"
	case MetaStatusMN:
		return "MN"
	case MetaStatusME:
		return "ME"
	default:
		return "??"
	}
}

// metaStatusFromCD maps a two-letter meta status token to MetaStatus.
func metaStatusFromCD(cd []byte) MetaStatus {
	switch {
	case bytes.Equal(cd, _HDBytes):
		return MetaStatusHD
	case bytes.Equal(cd, _VABytes):
		return MetaStatusVA
	case bytes.Equal(cd, _ENBytes):
		return MetaStatusEN
	case bytes.Equal(cd, _NFBytes):
		return MetaStatusNF
	case bytes.Equal(cd, _NSBytes):
		return MetaStatusNS
	case bytes.Equal(cd, _EXBytes):
		return MetaStatusEX
	case bytes.Equal(cd, _MNBytes):
		return MetaStatusMN
	case bytes.Equal(cd, _MEBytes):
		return MetaStatusME
	default:
		return MetaStatusUnknown
	}
}

// MetaResult is the outcome of a single meta command.
type MetaResult struct {
	Status MetaStatus
	Item   MetaItem
	err    error
}

// metaResultFrom builds a MetaResult from item and status.
func metaResultFrom(item *MetaItem, status MetaStatus) MetaResult {
	if item == nil {
		return MetaResult{Status: status}
	}
	if status == MetaStatusUnknown {
		status = item.status
	}
	cp := *item
	cp.status = 0
	return MetaResult{
		Status: status,
		Item:   cp,
	}
}

// metaResultWithErr builds a MetaResult that also carries err.
func metaResultWithErr(item *MetaItem, status MetaStatus, err error) MetaResult {
	res := metaResultFrom(item, status)
	res.err = err
	return res
}

// MetaGetReturn chooses which tokens mg should echo.
type MetaGetReturn struct {
	CAS, Flags, Hit, Key, LastAccess, Size, TTL, Value bool
}

// MetaGet is the flag set for mg.
// CompareCAS, Vivify (create-if-missing TTL), and Touch follow Config rule C: nil drops the flag.
// Recache follows rule D: a zero duration drops R.
type MetaGet struct {
	BinaryKey  bool
	Return     MetaGetReturn
	CompareCAS *uint64
	Opaque     string
	Quiet      bool
	NoLRUBump  bool
	NewCAS     *uint64
	Vivify     *Exptime
	// Recache is the remaining-TTL window sent as R (seconds).
	// Zero drops the flag. It is not an item Exptime and has no 30-day split.
	Recache time.Duration
	Touch   *Exptime
	P       string // proxy hint, ignored by memcached
	L       string // proxy path, ignored by memcached
}

// MetaNoop is the flag set for mn. P and L are proxy hints the daemon ignores.
type MetaNoop struct {
	P string
	L string
}

// MetaSet is the flag set for ms.
type MetaSet struct {
	BinaryKey  bool
	ReturnCAS  bool
	CompareCAS *uint64
	NewCAS     *uint64
	Flags      uint32
	Invalidate bool
	ReturnKey  bool
	Opaque     string
	Quiet      bool
	ReturnSize bool
	TTL        *Exptime
	Mode       MetaSetMode
	AutoVivify *Exptime
	P          string
	L          string
}

// MetaDelete is the flag set for md.
type MetaDelete struct {
	BinaryKey       bool
	CompareCAS      *uint64
	NewCAS          *uint64
	Invalidate      bool
	ReturnKey       bool
	Opaque          string
	Quiet           bool
	TTL             *Exptime
	RemoveValueOnly bool
	P               string
	L               string
}

// MetaDebug is the flag set for me.
type MetaDebug struct {
	BinaryKey bool
	P         string
	L         string
}

// MetaArithmetic is the flag set for ma.
type MetaArithmetic struct {
	BinaryKey   bool
	CompareCAS  *uint64
	NewCAS      *uint64
	AutoCreate  *Exptime
	Initial     *uint64
	TTL         *Exptime
	Mode        MetaArithmeticMode
	Opaque      string
	Quiet       bool
	ReturnTTL   bool
	ReturnCAS   bool
	ReturnValue bool
	ReturnKey   bool
	P           string
	L           string
}

// MetaOp is one step in Meta().Pipeline. Pipeline writes into a caller dst
// of the same length; Key is copied only when the op asks to return it.
// Exactly one of Get, Set, Del, Arith, or Noop must be set.
// Debug (me) cannot be pipelined.
type MetaOp struct {
	Key   []byte
	Body  []byte
	Delta uint64
	Get   *MetaGet
	Set   *MetaSet
	Del   *MetaDelete
	Arith *MetaArithmetic
	Noop  *MetaNoop
}

// applyOpaque stores token as a numeric O flag or a string opaque.
func applyOpaque(token string, flagsO *uint64, flagsOset *bool, flagsTok *string, flagsTokSet *bool) {
	if token == "" {
		return
	}
	if v, err := strconv.ParseUint(token, 10, 64); err == nil && strconv.FormatUint(v, 10) == token {
		*flagsO = v
		*flagsOset = true
		return
	}
	*flagsTok = token
	*flagsTokSet = true
}

// toFlags maps MetaGet onto the mg wire flags.
func (f MetaGet) toFlags() metaGetFlags {
	out := metaGetFlags{
		b: f.BinaryKey,
		c: f.Return.CAS,
		f: f.Return.Flags,
		h: f.Return.Hit,
		k: f.Return.Key,
		l: f.Return.LastAccess,
		q: f.Quiet,
		s: f.Return.Size,
		t: f.Return.TTL,
		u: f.NoLRUBump,
		v: f.Return.Value,
	}
	applyOpaque(f.Opaque, &out.O, &out.Oset, &out.Otoken, &out.OtokenSet)
	if f.CompareCAS != nil {
		out.C = *f.CompareCAS
		out.Cset = true
	}
	if f.NewCAS != nil {
		out.E = *f.NewCAS
		out.Eset = true
	}
	if f.Vivify != nil {
		out.N = expiryToken(*f.Vivify)
		out.Nset = true
	}
	if tok, ok := recacheToken(f.Recache); ok {
		out.R = tok
		out.Rset = true
	}
	if f.Touch != nil {
		out.T = expiryToken(*f.Touch)
		out.Tset = true
	}
	out.P = f.P
	out.L = f.L
	return out
}

// toFlags maps MetaSet onto the ms wire flags.
func (f MetaSet) toFlags() metaSetFlags {
	out := metaSetFlags{
		b: f.BinaryKey,
		c: f.ReturnCAS,
		F: f.Flags,
		I: f.Invalidate,
		k: f.ReturnKey,
		q: f.Quiet,
		s: f.ReturnSize,
		M: f.Mode,
	}
	applyOpaque(f.Opaque, &out.O, &out.Oset, &out.Otoken, &out.OtokenSet)
	if f.CompareCAS != nil {
		out.C = *f.CompareCAS
		out.Cset = true
	}
	if f.NewCAS != nil {
		out.E = *f.NewCAS
		out.Eset = true
	}
	if f.TTL != nil {
		out.T = expiryToken(*f.TTL)
		out.Tset = true
	}
	if f.AutoVivify != nil {
		out.N = expiryToken(*f.AutoVivify)
		out.Nset = true
	}
	out.P = f.P
	out.L = f.L
	return out
}

// toFlags maps MetaDelete onto the md wire flags.
func (f MetaDelete) toFlags() metaDeleteFlags {
	out := metaDeleteFlags{
		b: f.BinaryKey,
		I: f.Invalidate,
		k: f.ReturnKey,
		q: f.Quiet,
		x: f.RemoveValueOnly,
	}
	applyOpaque(f.Opaque, &out.O, &out.Oset, &out.Otoken, &out.OtokenSet)
	if f.CompareCAS != nil {
		out.C = *f.CompareCAS
		out.Cset = true
	}
	if f.NewCAS != nil {
		out.E = *f.NewCAS
		out.Eset = true
	}
	if f.TTL != nil {
		out.T = expiryToken(*f.TTL)
		out.Tset = true
	}
	out.P = f.P
	out.L = f.L
	return out
}

// toFlags maps MetaArithmetic onto the ma wire flags.
func (f MetaArithmetic) toFlags() metaArithmeticFlags {
	out := metaArithmeticFlags{
		b: f.BinaryKey,
		q: f.Quiet,
		t: f.ReturnTTL,
		c: f.ReturnCAS,
		v: f.ReturnValue,
		k: f.ReturnKey,
		M: f.Mode,
	}
	applyOpaque(f.Opaque, &out.O, &out.Oset, &out.Otoken, &out.OtokenSet)
	if f.CompareCAS != nil {
		out.C = *f.CompareCAS
		out.Cset = true
	}
	if f.NewCAS != nil {
		out.E = *f.NewCAS
		out.Eset = true
	}
	if f.AutoCreate != nil {
		out.N = expiryToken(*f.AutoCreate)
		out.Nset = true
	}
	if f.Initial != nil {
		out.J = *f.Initial
		out.Jset = true
	}
	if f.TTL != nil {
		out.T = expiryToken(*f.TTL)
		out.Tset = true
	}
	out.P = f.P
	out.L = f.L
	return out
}

// toFlags maps MetaDebug onto the me wire flags.
func (f MetaDebug) toFlags() metaDebugFlags {
	return metaDebugFlags{b: f.BinaryKey, P: f.P, L: f.L}
}

// validate checks MetaGet proxy hints and the recache window.
func (f MetaGet) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.Recache == 0 {
		return nil
	}
	if _, ok := recacheToken(f.Recache); !ok {
		return fmt.Errorf("recache window: %w", ErrInvalidArgument)
	}
	return nil
}

// validate checks MetaSet proxy hints and that N is only set for append or prepend.
func (f MetaSet) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.AutoVivify != nil && f.Mode != MetaSetModeAppend && f.Mode != MetaSetModePrepend {
		return fmt.Errorf("ms N requires append or prepend: %w", ErrInvalidArgument)
	}
	return nil
}

// validate checks MetaDelete proxy hints and that T requires invalidate.
func (f MetaDelete) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.TTL != nil && !f.Invalidate {
		return fmt.Errorf("md T requires invalidate: %w", ErrInvalidArgument)
	}
	return nil
}

// validate checks MetaArithmetic proxy hints, rejects Quiet with ReturnValue, and requires AutoCreate when Initial is set.
func (f MetaArithmetic) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.Quiet && f.ReturnValue {
		return fmt.Errorf("ma q+v: %w", ErrInvalidArgument)
	}
	if f.Initial != nil && f.AutoCreate == nil {
		return fmt.Errorf("ma J requires auto-create: %w", ErrInvalidArgument)
	}
	return nil
}

// validate checks MetaDebug proxy hints.
func (f MetaDebug) validate() error {
	return validMetaHints(f.P, f.L)
}

// validate checks MetaNoop proxy hints.
func (f MetaNoop) validate() error {
	return validMetaHints(f.P, f.L)
}
