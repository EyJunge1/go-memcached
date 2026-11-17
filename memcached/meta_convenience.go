package memcached

import "time"

// MetaGetValue returns MetaGet flags that request the value body (wire v).
// Prefer chaining With* for additional flags, or MetaGetItem for CAS-ready reads.
func MetaGetValue() MetaGet {
	return MetaGet{Return: MetaGetReturn{Value: true}}
}

// MetaGetItem returns Value+CAS+Flags+TTL (wire v, c, f, t).
// Use before CAS writes so the caller can re-apply remaining TTL via MetaSet.TTL.
// Protocol misses/conflicts stay on dst (method error nil); see MetaClient.Get.
// Meta ms without T leaves the existing TTL unchanged; text cas always sends exptime.
func MetaGetItem() MetaGet {
	return MetaGet{Return: MetaGetReturn{Value: true, CAS: true, Flags: true, TTL: true}}
}

// WithBinaryKey sets the b flag (base64 key on the wire).
func (f MetaGet) WithBinaryKey() MetaGet { f.BinaryKey = true; return f }

// WithQuiet sets the q flag.
func (f MetaGet) WithQuiet() MetaGet { f.Quiet = true; return f }

// WithNoLRUBump sets the u flag.
func (f MetaGet) WithNoLRUBump() MetaGet { f.NoLRUBump = true; return f }

// WithValue requests the value body (v).
func (f MetaGet) WithValue() MetaGet { f.Return.Value = true; return f }

// WithCAS requests the CAS token (c).
func (f MetaGet) WithCAS() MetaGet { f.Return.CAS = true; return f }

// WithFlags requests client flags (f).
func (f MetaGet) WithFlags() MetaGet { f.Return.Flags = true; return f }

// WithHitBefore requests the hit-before token (h).
func (f MetaGet) WithHitBefore() MetaGet { f.Return.HitBefore = true; return f }

// WithKey requests the key echo (k).
func (f MetaGet) WithKey() MetaGet { f.Return.Key = true; return f }

// WithLastAccess requests seconds since last access (l).
func (f MetaGet) WithLastAccess() MetaGet { f.Return.LastAccess = true; return f }

// WithSize requests the size token (s).
func (f MetaGet) WithSize() MetaGet { f.Return.Size = true; return f }

// WithTTL requests remaining TTL (t).
func (f MetaGet) WithTTL() MetaGet { f.Return.TTL = true; return f }

// WithOpaque sets the O token (numeric string or opaque bytes).
func (f MetaGet) WithOpaque(tok []byte) MetaGet { f.Opaque = tok; return f }

// WithCompareCAS sets the C token.
func (f MetaGet) WithCompareCAS(cas uint64) MetaGet {
	f.CompareCAS = uint64Ptr(cas)
	return f
}

// WithNewCAS sets the E token.
func (f MetaGet) WithNewCAS(cas uint64) MetaGet {
	f.NewCAS = uint64Ptr(cas)
	return f
}

// WithVivify sets N (create-on-miss TTL).
func (f MetaGet) WithVivify(exp Exptime) MetaGet {
	f.Vivify = exptimePtr(exp)
	return f
}

// WithRecache sets R (remaining-TTL recache window).
func (f MetaGet) WithRecache(d time.Duration) MetaGet { f.Recache = d; return f }

// WithTouch sets T (update remaining TTL).
func (f MetaGet) WithTouch(exp Exptime) MetaGet {
	f.Touch = exptimePtr(exp)
	return f
}

// WithProxyHint sets wire P.
func (f MetaGet) WithProxyHint(s string) MetaGet { f.ProxyHint = s; return f }

// WithProxyPath sets wire L.
func (f MetaGet) WithProxyPath(s string) MetaGet { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens after typed flags.
func (f MetaGet) WithExtra(tokens ...string) MetaGet {
	f.Extra = append(f.Extra, tokens...)
	return f
}

// WithBinaryKey sets the b flag.
func (f MetaSet) WithBinaryKey() MetaSet { f.BinaryKey = true; return f }

// WithQuiet sets the q flag.
func (f MetaSet) WithQuiet() MetaSet { f.Quiet = true; return f }

// WithInvalidate sets the I flag.
func (f MetaSet) WithInvalidate() MetaSet { f.Invalidate = true; return f }

// WithCAS requests the CAS echo (c).
func (f MetaSet) WithCAS() MetaSet { f.Return.CAS = true; return f }

// WithKey requests the key echo (k).
func (f MetaSet) WithKey() MetaSet { f.Return.Key = true; return f }

// WithSize requests the size echo (s).
func (f MetaSet) WithSize() MetaSet { f.Return.Size = true; return f }

// WithFlags sets client flags (F).
func (f MetaSet) WithFlags(flags uint32) MetaSet { f.Flags = flags; return f }

// WithMode sets the M mode token.
func (f MetaSet) WithMode(mode MetaSetMode) MetaSet { f.Mode = mode; return f }

// WithOpaque sets the O token.
func (f MetaSet) WithOpaque(tok []byte) MetaSet { f.Opaque = tok; return f }

// WithCompareCAS sets the C token.
func (f MetaSet) WithCompareCAS(cas uint64) MetaSet {
	f.CompareCAS = uint64Ptr(cas)
	return f
}

// WithNewCAS sets the E token.
func (f MetaSet) WithNewCAS(cas uint64) MetaSet {
	f.NewCAS = uint64Ptr(cas)
	return f
}

// WithTTL sets T.
func (f MetaSet) WithTTL(exp Exptime) MetaSet {
	f.TTL = exptimePtr(exp)
	return f
}

// WithAutoVivify sets N.
func (f MetaSet) WithAutoVivify(exp Exptime) MetaSet {
	f.AutoVivify = exptimePtr(exp)
	return f
}

// WithProxyHint sets wire P.
func (f MetaSet) WithProxyHint(s string) MetaSet { f.ProxyHint = s; return f }

// WithProxyPath sets wire L.
func (f MetaSet) WithProxyPath(s string) MetaSet { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens after typed flags.
func (f MetaSet) WithExtra(tokens ...string) MetaSet {
	f.Extra = append(f.Extra, tokens...)
	return f
}

// WithBinaryKey sets the b flag.
func (f MetaDelete) WithBinaryKey() MetaDelete { f.BinaryKey = true; return f }

// WithQuiet sets the q flag.
func (f MetaDelete) WithQuiet() MetaDelete { f.Quiet = true; return f }

// WithInvalidate sets the I flag.
func (f MetaDelete) WithInvalidate() MetaDelete { f.Invalidate = true; return f }

// WithRemoveValueOnly sets the x flag.
func (f MetaDelete) WithRemoveValueOnly() MetaDelete { f.RemoveValueOnly = true; return f }

// WithKey requests the key echo (k).
func (f MetaDelete) WithKey() MetaDelete { f.Return.Key = true; return f }

// WithOpaque sets the O token.
func (f MetaDelete) WithOpaque(tok []byte) MetaDelete { f.Opaque = tok; return f }

// WithCompareCAS sets the C token.
func (f MetaDelete) WithCompareCAS(cas uint64) MetaDelete {
	f.CompareCAS = uint64Ptr(cas)
	return f
}

// WithNewCAS sets the E token.
func (f MetaDelete) WithNewCAS(cas uint64) MetaDelete {
	f.NewCAS = uint64Ptr(cas)
	return f
}

// WithTTL sets T.
func (f MetaDelete) WithTTL(exp Exptime) MetaDelete {
	f.TTL = exptimePtr(exp)
	return f
}

// WithProxyHint sets wire P.
func (f MetaDelete) WithProxyHint(s string) MetaDelete { f.ProxyHint = s; return f }

// WithProxyPath sets wire L.
func (f MetaDelete) WithProxyPath(s string) MetaDelete { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens after typed flags.
func (f MetaDelete) WithExtra(tokens ...string) MetaDelete {
	f.Extra = append(f.Extra, tokens...)
	return f
}

// WithBinaryKey sets the b flag.
func (f MetaArithmetic) WithBinaryKey() MetaArithmetic { f.BinaryKey = true; return f }

// WithQuiet sets the q flag.
func (f MetaArithmetic) WithQuiet() MetaArithmetic { f.Quiet = true; return f }

// WithMode sets incr/decr.
func (f MetaArithmetic) WithMode(mode MetaArithmeticMode) MetaArithmetic { f.Mode = mode; return f }

// WithCAS requests the CAS echo (c).
func (f MetaArithmetic) WithCAS() MetaArithmetic { f.Return.CAS = true; return f }

// WithValue requests the value echo (v).
func (f MetaArithmetic) WithValue() MetaArithmetic { f.Return.Value = true; return f }

// WithKey requests the key echo (k).
func (f MetaArithmetic) WithKey() MetaArithmetic { f.Return.Key = true; return f }

// WithTTLEcho requests remaining TTL (t).
func (f MetaArithmetic) WithTTLEcho() MetaArithmetic { f.Return.TTL = true; return f }

// WithOpaque sets the O token.
func (f MetaArithmetic) WithOpaque(tok []byte) MetaArithmetic { f.Opaque = tok; return f }

// WithCompareCAS sets the C token.
func (f MetaArithmetic) WithCompareCAS(cas uint64) MetaArithmetic {
	f.CompareCAS = uint64Ptr(cas)
	return f
}

// WithNewCAS sets the E token.
func (f MetaArithmetic) WithNewCAS(cas uint64) MetaArithmetic {
	f.NewCAS = uint64Ptr(cas)
	return f
}

// WithInitial sets the J token.
func (f MetaArithmetic) WithInitial(v uint64) MetaArithmetic {
	f.Initial = uint64Ptr(v)
	return f
}

// WithTTL sets T.
func (f MetaArithmetic) WithTTL(exp Exptime) MetaArithmetic {
	f.TTL = exptimePtr(exp)
	return f
}

// WithAutoCreate sets N.
func (f MetaArithmetic) WithAutoCreate(exp Exptime) MetaArithmetic {
	f.AutoCreate = exptimePtr(exp)
	return f
}

// WithProxyHint sets wire P.
func (f MetaArithmetic) WithProxyHint(s string) MetaArithmetic { f.ProxyHint = s; return f }

// WithProxyPath sets wire L.
func (f MetaArithmetic) WithProxyPath(s string) MetaArithmetic { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens after typed flags.
func (f MetaArithmetic) WithExtra(tokens ...string) MetaArithmetic {
	f.Extra = append(f.Extra, tokens...)
	return f
}

// WithBinaryKey sets the b flag.
func (f MetaDebug) WithBinaryKey() MetaDebug { f.BinaryKey = true; return f }

// WithProxyHint sets wire P.
func (f MetaDebug) WithProxyHint(s string) MetaDebug { f.ProxyHint = s; return f }

// WithProxyPath sets wire L.
func (f MetaDebug) WithProxyPath(s string) MetaDebug { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens after typed flags.
func (f MetaDebug) WithExtra(tokens ...string) MetaDebug {
	f.Extra = append(f.Extra, tokens...)
	return f
}

// WithProxyHint sets wire P on the pipeline mn barrier.
func (f MetaNoop) WithProxyHint(s string) MetaNoop { f.ProxyHint = s; return f }

// WithProxyPath sets wire L on the pipeline mn barrier.
func (f MetaNoop) WithProxyPath(s string) MetaNoop { f.ProxyPath = s; return f }

// WithExtra appends raw wire tokens on the pipeline mn barrier.
func (f MetaNoop) WithExtra(tokens ...string) MetaNoop {
	f.Extra = append(f.Extra, tokens...)
	return f
}

func uint64Ptr(v uint64) *uint64 { return &v }

func exptimePtr(v Exptime) *Exptime { return &v }
