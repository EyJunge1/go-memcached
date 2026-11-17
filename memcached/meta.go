package memcached

import (
	"bytes"
	"time"
)

// MetaStatus is a two-letter meta reply (HD, VA, EN, NF, NS, EX, MN, ME).
type MetaStatus uint8

// MetaStatusUnknown is an explicit unknown/malformed status. Zero MetaStatus is
// unset/invalid (Uber: enums start at one). Other constants are two-letter meta replies.
const (
	MetaStatusUnknown MetaStatus = iota + 1
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

// MetaResult is the outcome of a single meta command.
type MetaResult struct {
	Status MetaStatus
	Item   MetaItem
	err    error
}

// Err returns a transport or parse error, otherwise Status.Err.
func (r MetaResult) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.Status.Err()
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

// metaResultFrom builds a MetaResult from item and status.
func metaResultFrom(item *MetaItem, status MetaStatus) MetaResult {
	if item == nil {
		return MetaResult{Status: status}
	}
	if status == 0 || status == MetaStatusUnknown {
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

type (
	// MetaGetReturn chooses which tokens mg should echo.
	MetaGetReturn struct {
		CAS, Flags, HitBefore, Key, LastAccess, Size, TTL, Value bool
	}

	// MetaGet is the flag set for mg.
	// ProxyHint/ProxyPath fields on meta options are wire P/L tokens ignored by
	// memcached and limited to 4 KiB each.
	// CompareCAS, Vivify (create-if-missing TTL), and Touch are pointers: nil drops the flag.
	// Recache is a duration: zero omits R.
	// Extra appends raw wire tokens after typed flags (escape hatch for new server flags).
	MetaGet struct {
		BinaryKey  bool
		Return     MetaGetReturn
		CompareCAS *uint64
		Opaque     []byte
		Quiet      bool
		NoLRUBump  bool
		NewCAS     *uint64
		Vivify     *Exptime
		// Recache is the remaining-TTL window sent as R (seconds).
		// Zero drops the flag. It is not an item Exptime and has no 30-day split.
		Recache   time.Duration
		Touch     *Exptime
		ProxyHint string // wire P; ignored by memcached
		ProxyPath string // wire L; ignored by memcached
		Extra     []string
	}

	// MetaNoop is the flag set for mn. ProxyHint and ProxyPath are ignored by the daemon.
	// In Meta().Pipeline a Noop sends no line; it only sets P/L on the batch barrier.
	MetaNoop struct {
		ProxyHint string
		ProxyPath string
		Extra     []string
	}

	// MetaSetReturn chooses which tokens ms should echo.
	MetaSetReturn struct {
		CAS, Key, Size bool
	}

	// MetaSet is the flag set for ms.
	// Extra appends raw wire tokens after typed flags.
	// TTL (wire T): omit on update to leave the existing item TTL unchanged;
	// set explicitly when re-applying a TTL read via MetaGetItem (text cas
	// always sends exptime and does not preserve TTL by omission).
	MetaSet struct {
		BinaryKey  bool
		Return     MetaSetReturn
		CompareCAS *uint64
		NewCAS     *uint64
		Flags      uint32
		Invalidate bool
		Opaque     []byte
		Quiet      bool
		TTL        *Exptime
		Mode       MetaSetMode
		AutoVivify *Exptime
		ProxyHint  string
		ProxyPath  string
		Extra      []string
	}

	// MetaDeleteReturn chooses which tokens md should echo.
	MetaDeleteReturn struct {
		Key bool
	}

	// MetaDelete is the flag set for md.
	// Extra appends raw wire tokens after typed flags.
	MetaDelete struct {
		BinaryKey       bool
		CompareCAS      *uint64
		NewCAS          *uint64
		Invalidate      bool
		Return          MetaDeleteReturn
		Opaque          []byte
		Quiet           bool
		TTL             *Exptime
		RemoveValueOnly bool
		ProxyHint       string
		ProxyPath       string
		Extra           []string
	}

	// MetaDebug is the flag set for me.
	// Extra appends raw wire tokens after typed flags.
	MetaDebug struct {
		BinaryKey bool
		ProxyHint string
		ProxyPath string
		Extra     []string
	}

	// MetaArithmeticReturn chooses which tokens ma should echo.
	MetaArithmeticReturn struct {
		TTL, CAS, Value, Key bool
	}

	// MetaArithmetic is the flag set for ma.
	// Extra appends raw wire tokens after typed flags.
	MetaArithmetic struct {
		BinaryKey  bool
		CompareCAS *uint64
		NewCAS     *uint64
		AutoCreate *Exptime
		Initial    *uint64
		TTL        *Exptime
		Mode       MetaArithmeticMode
		Opaque     []byte
		Quiet      bool
		Return     MetaArithmeticReturn
		ProxyHint  string
		ProxyPath  string
		Extra      []string
	}

	// MetaOp is one step in Meta().Pipeline. Pipeline writes into a caller dst
	// of the same length; Key is copied only when the op asks to return it.
	// Body is borrowed until Pipeline returns (PreparedMetaBatch.RunValues borrows
	// per-run set bodies). Multi helpers pass Key/Body through to Pipeline without
	// retaining them after the call. DeferredMeta copies Key/Body/Opaque at enqueue.
	// Exactly one of Get, Set, Delete, Arithmetic, or Noop must be set. Debug (me)
	// cannot be pipelined. Pipeline Noop writes no mn line; it only puts
	// ProxyHint/ProxyPath on the batch barrier.
	MetaOp struct {
		Key        []byte
		Body       []byte
		Delta      uint64
		Get        *MetaGet
		Set        *MetaSet
		Delete     *MetaDelete
		Arithmetic *MetaArithmetic
		Noop       *MetaNoop
	}
)
