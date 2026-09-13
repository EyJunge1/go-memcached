package memcached

// Item is a Get hit and the Store / Pipeline.Store payload.
// Get fills Key, Value, Flags, and CAS; Mode and Quiet stay zero.
// Get strips Config.Cluster.Namespace from Key.
type Item struct {
	Mode  StoreMode
	Key   []byte
	Value []byte
	Flags uint32
	Exp   Exptime
	CAS   uint64
	Quiet bool
}

// MetaItem is a parsed mg, ms, md, ma, or mn reply.
type MetaItem struct {
	Key   []byte
	Value []byte

	// CAS is the c token. Ask for it with MetaGet.Return.CAS or MetaSet.ReturnCAS.
	CAS uint64
	// Flags is the f token on a value reply.
	Flags uint32
	// TTL is the t token in seconds. -1 means the item never expires.
	TTL int64
	// LastAccess is the l token (seconds since last use).
	LastAccess int64
	// Size is the s token on success.
	Size uint64
	// Opaque is the O token (at most 32 bytes).
	Opaque string
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
	KeyBase64 bool
	status    MetaStatus
	gotHD     bool
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
	Fields map[string]string
}

// Err is the error for Status, if any.
func (d MetaItemDebug) Err() error {
	return d.Status.Err()
}
