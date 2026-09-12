package memcached

import (
	"fmt"
)

// toFlags maps MetaGet onto the mg wire flags.
func (f MetaGet) toFlags() metaGetFlags {
	out := metaGetFlags{
		b: f.BinaryKey,
		c: f.Return.CAS,
		f: f.Return.Flags,
		h: f.Return.HitBefore,
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
	out.P = f.ProxyHint
	out.L = f.ProxyPath
	out.extra = f.Extra
	return out
}

// validate checks MetaGet proxy hints, Extra tokens, and the recache window.
func (f MetaGet) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	if err := validMetaExtras(f.Extra); err != nil {
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

// toFlags maps MetaSet onto the ms wire flags.
func (f MetaSet) toFlags() metaSetFlags {
	out := metaSetFlags{
		b: f.BinaryKey,
		c: f.Return.CAS,
		F: f.Flags,
		I: f.Invalidate,
		k: f.Return.Key,
		q: f.Quiet,
		s: f.Return.Size,
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
	out.P = f.ProxyHint
	out.L = f.ProxyPath
	out.extra = f.Extra
	return out
}

// validate checks MetaSet proxy hints, Extra tokens, and that N is only set for append or prepend.
func (f MetaSet) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	if err := validMetaExtras(f.Extra); err != nil {
		return err
	}
	if f.AutoVivify != nil && f.Mode != MetaSetModeAppend && f.Mode != MetaSetModePrepend {
		return fmt.Errorf("ms N requires append or prepend: %w", ErrInvalidArgument)
	}
	return nil
}

// toFlags maps MetaDelete onto the md wire flags.
func (f MetaDelete) toFlags() metaDeleteFlags {
	out := metaDeleteFlags{
		b: f.BinaryKey,
		I: f.Invalidate,
		k: f.Return.Key,
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
	out.P = f.ProxyHint
	out.L = f.ProxyPath
	out.extra = f.Extra
	return out
}

// validate checks MetaDelete proxy hints, Extra tokens, and that T requires invalidate.
func (f MetaDelete) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	if err := validMetaExtras(f.Extra); err != nil {
		return err
	}
	if f.TTL != nil && !f.Invalidate {
		return fmt.Errorf("md T requires invalidate: %w", ErrInvalidArgument)
	}
	return nil
}

// toFlags maps MetaArithmetic onto the ma wire flags.
func (f MetaArithmetic) toFlags() metaArithmeticFlags {
	out := metaArithmeticFlags{
		b: f.BinaryKey,
		q: f.Quiet,
		t: f.Return.TTL,
		c: f.Return.CAS,
		v: f.Return.Value,
		k: f.Return.Key,
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
	out.P = f.ProxyHint
	out.L = f.ProxyPath
	out.extra = f.Extra
	return out
}

// validate checks MetaArithmetic proxy hints, Extra tokens, rejects Quiet with
// Return.Value, and requires AutoCreate when Initial is set.
func (f MetaArithmetic) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	if err := validMetaExtras(f.Extra); err != nil {
		return err
	}
	if f.Quiet && f.Return.Value {
		return fmt.Errorf("ma q+v: %w", ErrInvalidArgument)
	}
	if f.Initial != nil && f.AutoCreate == nil {
		return fmt.Errorf("ma J requires auto-create: %w", ErrInvalidArgument)
	}
	return nil
}

// toFlags maps MetaDebug onto the me wire flags.
func (f MetaDebug) toFlags() metaDebugFlags {
	return metaDebugFlags{b: f.BinaryKey, P: f.ProxyHint, L: f.ProxyPath, extra: f.Extra}
}

// validate checks MetaDebug proxy hints and Extra tokens.
func (f MetaDebug) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	return validMetaExtras(f.Extra)
}

// validate checks MetaNoop proxy hints and Extra tokens.
func (f MetaNoop) validate() error {
	if err := validMetaHints(f.ProxyHint, f.ProxyPath); err != nil {
		return err
	}
	return validMetaExtras(f.Extra)
}

func applyOpaque(token []byte, flagsO *uint64, flagsOset *bool, flagsTok *string, flagsTokSet *bool) {
	if len(token) == 0 {
		return
	}
	// Numeric opaques stay on []byte to avoid a string conversion.
	if v, err := parseUintFromBytes(token); err == nil && opaqueIsCanonical(token, v) {
		*flagsO = v
		*flagsOset = true
		return
	}
	*flagsTok = string(token)
	*flagsTokSet = true
}
