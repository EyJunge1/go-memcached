package memcached

// Item is a get result and the input for Store / Pipeline.Store.
// Get fills Key, Value, Flags, and CAS; Mode and Quiet stay zero.
type Item struct {
	Mode  StoreMode
	Key   []byte
	Value []byte
	Flags uint32
	Exp   Exptime
	CAS   uint64
	Quiet bool
}

// MetaItem represents a key-value pair with meta information.
type MetaItem struct {
	Key   []byte
	Value []byte

	/* Meta */
	// CAS is a unique value that is used to check-and-set operation.
	// Request it with MetaGet.Return.CAS or MetaSet.ReturnCAS.
	CAS uint64
	// Flags is the client flags stored with the item when a value is returned.
	// Request it with MetaGet.Return.Flags.
	Flags uint32
	// TTL is the time-to-live of the item. -1 means never expire.
	// Request it with MetaGet.Return.TTL.
	TTL int64
	// LastAccessedTime is the time since item was last accessed in seconds.
	// Request it with MetaGet.Return.LastAccess.
	LastAccessedTime int64
	// Size is the size of the stored item on success.
	// Request it with MetaGet.Return.Size or MetaSet.ReturnSize.
	Size uint64
	// Opaque is the opaque value when the token is decimal.
	Opaque uint64
	// OpaqueToken is the raw opaque token (up to 32 bytes).
	OpaqueToken string
	// HitBefore is whether the item has been hit before.
	// Request it with MetaGet.Return.Hit.
	HitBefore bool
	// Won is set when the server returned recache flag W.
	Won bool
	// Stale is set when the server returned stale flag X.
	Stale bool
	// RecacheBusy is set when the server returned winning-already-sent flag Z.
	RecacheBusy bool
	// CASMatched is true when MetaGet.CheckCAS was used with a value
	// request and the server returned HD (CAS still matches).
	CASMatched bool
	// WireKey is the k token as received (possibly Base64).
	WireKey []byte
	// KeyBase64 is set when the server echoed response flag b.
	KeyBase64 bool
	Status    MetaStatus
	gotHD     bool
}

// MetaItemDebug represents a key-value pair with meta information for debug.
//
//	exp   = expiration time
//	la    = time in seconds since last access
//	cas   = CAS ID
//	fetch = whether an item has been fetched before
//	cls   = slab class id
//	size  = total size in bytes
type MetaItemDebug struct {
	Status         MetaStatus
	Key            []byte // decoded when the request used b
	WireKey        []byte // ME key token as received (possibly Base64)
	KeyBase64      bool   // set when the request used BinaryKey
	TTL            int64  // exp, expiration time in seconds, -1 means never expire
	LastAccessTime int64  // la, time in seconds since last access
	CAS            uint64 // cas
	HitBefore      bool   // fetch
	SlabClassID    uint64 // cls
	Size           uint64 // size
	// Fields holds unknown me key=value tokens. The six known keys stay
	// on the typed fields above and are not copied here.
	Fields map[string]string
}
