package memcached

// Item is a get result and the input for Store / Pipeline.Store.
// Get fills Key, Value, Flags, and CAS; Mode and Quiet stay zero.
// Key is the caller key; Get strips Config.Cluster.Namespace.
type Item struct {
	Mode  StoreMode
	Key   []byte
	Value []byte
	Flags uint32
	Exp   Exptime
	CAS   uint64
	Quiet bool
}

// MetaItem is a decoded mg/ms/md/ma/mn line (tokens c, f, t, l, s, O, h, W, X, Z, k, b).
type MetaItem struct {
	Key   []byte
	Value []byte

	// CAS is token c. Request it with MetaGet.Return.CAS or MetaSet.ReturnCAS.
	CAS uint64
	// Flags is token f when a value is returned.
	Flags uint32
	// TTL is token t in seconds. -1 means never expire.
	TTL int64
	// LastAccess is token l (seconds since last access).
	LastAccess int64
	// Size is token s on success.
	Size uint64
	// Opaque is token O (up to 32 bytes).
	Opaque string
	// HitBefore is token h.
	HitBefore bool
	// Won is recache flag W.
	Won bool
	// Stale is flag X.
	Stale bool
	// RecacheBusy is flag Z (winner already sent).
	RecacheBusy bool
	// CASMatched is true when MetaGet.CompareCAS was set and the server returned HD.
	CASMatched bool
	// WireKey is token k as received (possibly Base64).
	WireKey []byte
	// KeyBase64 is set when the server echoed flag b.
	KeyBase64 bool
	status    MetaStatus
	gotHD     bool
}

// MetaItemDebug is a decoded me line (exp, la, cas, fetch, cls, size).
type MetaItemDebug struct {
	Status      MetaStatus
	Key         []byte // decoded when the request used b
	WireKey     []byte // ME key token as received
	KeyBase64   bool
	TTL         int64  // exp; -1 never expires
	LastAccess  int64  // la
	CAS         uint64 // cas
	HitBefore   bool   // fetch
	SlabClassID uint64 // cls
	Size        uint64 // size
	// Fields holds unknown me key=value tokens. The six known keys stay typed.
	Fields map[string]string
}

func (d MetaItemDebug) Err() error {
	return d.Status.Err()
}
