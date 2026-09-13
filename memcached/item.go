package memcached

import "strconv"

// Item represents a key-value pair to be got or stored.
type Item struct {
	Key   []byte
	Value []byte

	// Flags is the client flags stored with the item.
	Flags uint32
	// CAS is a unique value that is used to check-and-set operation.
	// It is set when GetOpts.CAS is true (gets/gats).
	CAS uint64
	// Exp is the expiration used by Pipeline.Set/Add/Replace/Cas.
	// Retrieval leaves it 0.
	Exp Exptime
}

func (i *Item) String() string {
	return "Item{" +
		"Key:" + string(i.Key) +
		" Value:" + string(i.Value) +
		" Flags:" + strconv.FormatUint(uint64(i.Flags), 10) +
		" CAS:" + strconv.FormatUint(i.CAS, 10) +
		"}"
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

func (m *MetaItem) String() string {
	return "MetaItem{" +
		"Key:" + string(m.Key) +
		" Value:" + string(m.Value) +
		" CAS:" + strconv.FormatUint(m.CAS, 10) +
		" Flags:" + strconv.FormatUint(uint64(m.Flags), 10) +
		" TTL:" + strconv.FormatInt(m.TTL, 10) +
		" LastAccessedTime:" + strconv.FormatInt(m.LastAccessedTime, 10) +
		" Size:" + strconv.FormatUint(m.Size, 10) +
		" Opaque:" + strconv.FormatUint(m.Opaque, 10) +
		" OpaqueToken:" + m.OpaqueToken +
		" HitBefore:" + strconv.FormatBool(m.HitBefore) +
		" Won:" + strconv.FormatBool(m.Won) +
		" Stale:" + strconv.FormatBool(m.Stale) +
		" RecacheBusy:" + strconv.FormatBool(m.RecacheBusy) +
		" CASMatched:" + strconv.FormatBool(m.CASMatched) +
		"}"
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
	Key            []byte // key
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
