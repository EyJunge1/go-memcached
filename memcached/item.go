package memcached

// GetResult is one index-aligned Get slot.
// Found distinguishes a hit from a miss; an empty stored value is Found with
// len(Value)==0. Get does not copy the key — use keys[i].
//
// Reusing the same slot is intended. Value stays valid until this slot is
// filled again. If Value has enough cap, Get writes into that buffer and the
// previous slice header that shared it becomes invalid.
type GetResult struct {
	Found bool
	Value []byte
	Flags uint32
	CAS   uint64
}

// Item is the Store / Pipeline.Store payload.
// Key and Value are borrowed for the duration of the call (and until Pipeline.Run
// returns for queued ops). Do not mutate them concurrently with the call.
// PreparedBatch copies default values at Prepare; RunValues borrows per-run bodies.
type Item struct {
	Mode  StoreMode
	Key   []byte
	Value []byte
	Flags uint32
	Exp   Exptime
	// CAS is sent only when Mode is StoreCAS.
	CAS uint64
	// Quiet adds noreply. Pipeline.Store rejects Quiet.
	Quiet bool
}

// MetaItem is a parsed mg, ms, md, ma, or mn reply.
type MetaItem struct {
	Key   []byte
	Value []byte

	// CAS is the c token. Ask for it with MetaGet.Return.CAS or MetaSet.Return.CAS.
	CAS uint64
	// Flags is the f token on a value reply.
	Flags uint32
	// TTL is the t token in seconds. -1 means the item never expires.
	TTL int64
	// LastAccess is the l token (seconds since last use).
	LastAccess int64
	// Size is the s token on success.
	Size uint64
	// Opaque is the O token (at most 32 bytes). Reusing dst keeps cap.
	Opaque []byte
	// HitBefore is the h token.
	HitBefore bool
	// Won is set when this client won the Recache race (flag W).
	Won bool
	// Stale is flag X.
	Stale bool
	// RecacheBusy is flag Z (another client already won).
	RecacheBusy bool
	// CASMatched is set when MetaGet.CompareCAS was sent and the reply is HD.
	CASMatched bool
	// WireKey is the k token as the server sent it (may be Base64).
	WireKey []byte
	// KeyBase64 is set when the server sent the key in Base64 (b).
	KeyBase64    bool
	status       MetaStatus
	gotHD        bool
	opaqueNum    uint64
	hasOpaqueNum bool
}

// MetaItemDebug is a parsed me reply.
type MetaItemDebug struct {
	Status      MetaStatus
	Key         []byte // decoded when the request set BinaryKey
	WireKey     []byte // ME key token as received
	KeyBase64   bool
	TTL         int64 // exp; -1 never expires
	LastAccess  int64 // la (seconds since last use)
	CAS         uint64
	HitBefore   bool   // fetch (item was accessed before)
	SlabClassID uint64 // cls
	Size        uint64
	// Fields holds me pairs that are not one of the six typed keys.
	// Reusing dst keeps Key/Value caps.
	Fields []DebugField
}

// DebugField is one unknown me key=value pair.
type DebugField struct {
	Key   []byte
	Value []byte
}

// Stat is one STAT name/value pair. Reusing dst keeps Name/Value caps.
type Stat struct {
	Name  []byte
	Value []byte
}

// Err is the error for Status, if any.
func (d MetaItemDebug) Err() error {
	return d.Status.Err()
}
