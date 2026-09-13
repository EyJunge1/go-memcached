package memcached

import (
	"fmt"
	"strconv"
	"time"
)

// MetaStatus is a meta response code (HD, VA, EN, NF, NS, EX, MN, ME).
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

func (r MetaResult) Err() error {
	return r.Status.Err()
}

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

func metaStatusFromCD(cd []byte) MetaStatus {
	switch string(cd) {
	case "HD":
		return MetaStatusHD
	case "VA":
		return MetaStatusVA
	case "EN":
		return MetaStatusEN
	case "NF":
		return MetaStatusNF
	case "NS":
		return MetaStatusNS
	case "EX":
		return MetaStatusEX
	case "MN":
		return MetaStatusMN
	case "ME":
		return MetaStatusME
	default:
		return MetaStatusUnknown
	}
}

// MetaResult is one meta command outcome.
type MetaResult struct {
	Status    MetaStatus
	Item      MetaItem
	WireKey   []byte
	KeyBase64 bool
}

func metaResultFrom(item *MetaItem, status MetaStatus) MetaResult {
	if item == nil {
		return MetaResult{Status: status}
	}
	if status == MetaStatusUnknown {
		status = item.Status
	}
	return MetaResult{
		Status:    status,
		Item:      *item,
		WireKey:   item.WireKey,
		KeyBase64: item.KeyBase64,
	}
}

// MetaGetReturn selects response tokens for mg.
type MetaGetReturn struct {
	CAS, Flags, Hit, Key, LastAccess, Size, TTL, Value bool
}

// MetaGet is the mg flags struct.
type MetaGet struct {
	BinaryKey bool
	Return    MetaGetReturn
	CheckCAS  *uint64
	Opaque    string
	Quiet     bool
	NoLRUBump bool
	NewCAS    *uint64
	Vivify    *Exptime
	// Recache is the remaining-TTL window for R (seconds on the wire).
	// Zero omits the flag. This is not an item Exptime and has no 30-day cap.
	Recache time.Duration
	Touch   *Exptime
	P       string // proxy hint (server ignores)
	L       string // proxy path (server ignores)
}

// MetaNoop is the mn flags struct. P/L are proxy hints; the daemon ignores them.
type MetaNoop struct {
	P string
	L string
}

// MetaSet is the ms flags struct.
type MetaSet struct {
	BinaryKey   bool
	ReturnCAS   bool
	CompareCAS  *uint64
	NewCAS      *uint64
	ClientFlags uint32
	Invalidate  bool
	ReturnKey   bool
	Opaque      string
	Quiet       bool
	ReturnSize  bool
	TTL         *Exptime
	Mode        metaSetMode
	AutoVivify  *Exptime
	P           string
	L           string
}

// MetaDelete is the md flags struct.
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

// MetaDebug is the me flags struct.
type MetaDebug struct {
	BinaryKey bool
	P         string
	L         string
}

// MetaArithmetic is the ma flags struct.
type MetaArithmetic struct {
	BinaryKey   bool
	CompareCAS  *uint64
	NewCAS      *uint64
	AutoCreate  *Exptime
	Initial     *uint64
	TTL         *Exptime
	Mode        metaArithmeticMode
	Opaque      string
	Quiet       bool
	ReturnTTL   bool
	ReturnCAS   bool
	ReturnValue bool
	ReturnKey   bool
	P           string
	L           string
}

// MetaOpKind selects a pipeline command.
type MetaOpKind uint8

const (
	MetaOpGet MetaOpKind = iota
	MetaOpSet
	MetaOpDelete
	MetaOpArith
	MetaOpNoop
)

// MetaOp is one entry in MetaPipeline.
type MetaOp struct {
	Op    MetaOpKind
	Key   []byte
	Body  []byte
	Delta uint64
	Flags any // MetaGet, MetaSet, MetaDelete, or MetaArithmetic
}

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

func (f MetaGet) toFlags() *metaGetFlags {
	out := &metaGetFlags{
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
	if f.CheckCAS != nil {
		out.C = *f.CheckCAS
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

func (f MetaSet) toFlags() *metaSetFlags {
	out := &metaSetFlags{
		b: f.BinaryKey,
		c: f.ReturnCAS,
		F: f.ClientFlags,
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

func (f MetaDelete) toFlags() *metaDeleteFlags {
	out := &metaDeleteFlags{
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

func (f MetaArithmetic) toFlags() *metaArithmeticFlags {
	out := &metaArithmeticFlags{
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

func (f MetaDebug) toFlags() *metaDebugFlags {
	return &metaDebugFlags{b: f.BinaryKey, P: f.P, L: f.L}
}

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

func (f MetaSet) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.AutoVivify != nil && f.Mode != MetaSetModeAppend && f.Mode != MetaSetModePrepend {
		return fmt.Errorf("ms N requires append or prepend: %w", ErrInvalidArgument)
	}
	return nil
}

func (f MetaDelete) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.TTL != nil && !f.Invalidate {
		return fmt.Errorf("md T requires invalidate: %w", ErrInvalidArgument)
	}
	return nil
}

func (f MetaArithmetic) validate() error {
	if err := validMetaHints(f.P, f.L); err != nil {
		return err
	}
	if f.Initial != nil && f.AutoCreate == nil {
		return fmt.Errorf("ma J requires auto-create: %w", ErrInvalidArgument)
	}
	return nil
}

func (f MetaDebug) validate() error {
	return validMetaHints(f.P, f.L)
}

func (f MetaNoop) validate() error {
	return validMetaHints(f.P, f.L)
}
