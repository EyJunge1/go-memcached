package memcached

import (
	"testing"
	"time"
)

func TestMetaGetItemConvenience(t *testing.T) {
	w := MetaGetItem().toFlags()
	if !w.v || !w.c || !w.f || !w.t {
		t.Fatalf("MetaGetItem flags incomplete: %+v", w)
	}
}

func TestMetaGetConvenienceMapsToFlags(t *testing.T) {
	f := MetaGetValue().
		WithCAS().
		WithFlags().
		WithHitBefore().
		WithKey().
		WithLastAccess().
		WithSize().
		WithTTL().
		WithQuiet().
		WithNoLRUBump().
		WithBinaryKey().
		WithOpaque([]byte("7")).
		WithCompareCAS(9).
		WithNewCAS(11).
		WithVivify(Rel(time.Minute)).
		WithRecache(5 * time.Second).
		WithTouch(Rel(time.Hour)).
		WithProxyHint("h").
		WithProxyPath("p").
		WithExtra("Z1")

	w := f.toFlags()
	if !w.v || !w.c || !w.f || !w.h || !w.k || !w.l || !w.s || !w.t || !w.q || !w.u || !w.b {
		t.Fatalf("bool flags incomplete: %+v", w)
	}
	if !w.Cset || w.C != 9 || !w.Eset || w.E != 11 {
		t.Fatalf("CAS flags: Cset=%v C=%d Eset=%v E=%d", w.Cset, w.C, w.Eset, w.E)
	}
	if !w.Nset || !w.Rset || !w.Tset {
		t.Fatalf("expiry flags: Nset=%v Rset=%v Tset=%v", w.Nset, w.Rset, w.Tset)
	}
	if w.P != "h" || w.L != "p" {
		t.Fatalf("proxy P=%q L=%q", w.P, w.L)
	}
	if len(w.extra) != 1 || w.extra[0] != "Z1" {
		t.Fatalf("extra=%v", w.extra)
	}
}

func TestMetaSetDeleteArithConvenience(t *testing.T) {
	set := MetaSet{}.
		WithQuiet().
		WithInvalidate().
		WithBinaryKey().
		WithFlags(3).
		WithMode(MetaSetModeAdd).
		WithTTL(Rel(time.Second)).
		WithCAS().
		WithKey().
		WithSize().
		WithOpaque([]byte("9")).
		WithCompareCAS(2).
		WithNewCAS(4).
		WithAutoVivify(Rel(time.Minute)).
		WithProxyHint("hp").
		WithProxyPath("lp").
		WithExtra("Z1")
	sf := set.toFlags()
	if !sf.q || !sf.I || !sf.b || sf.F != 3 || sf.M != MetaSetModeAdd || !sf.Tset || !sf.c || !sf.k || !sf.s {
		t.Fatalf("set flags=%+v", sf)
	}
	if !sf.Cset || sf.C != 2 || !sf.Eset || sf.E != 4 || !sf.Nset {
		t.Fatalf("set CAS/N flags=%+v", sf)
	}
	if sf.P != "hp" || sf.L != "lp" || len(sf.extra) != 1 || sf.extra[0] != "Z1" {
		t.Fatalf("set proxy/extra=%+v", sf)
	}

	del := MetaDelete{}.
		WithInvalidate().
		WithRemoveValueOnly().
		WithKey().
		WithQuiet().
		WithBinaryKey().
		WithOpaque([]byte("3")).
		WithCompareCAS(7).
		WithNewCAS(8).
		WithTTL(Rel(time.Second)).
		WithProxyHint("hd").
		WithProxyPath("ld").
		WithExtra("Zd")
	df := del.toFlags()
	if !df.I || !df.x || !df.k || !df.q || !df.b || !df.Tset || !df.Cset || df.C != 7 || !df.Eset || df.E != 8 {
		t.Fatalf("delete flags=%+v", df)
	}
	if df.P != "hd" || df.L != "ld" || len(df.extra) != 1 || df.extra[0] != "Zd" {
		t.Fatalf("delete proxy/extra=%+v", df)
	}

	ar := MetaArithmetic{}.
		WithMode(MetaArithmeticModeDecr).
		WithValue().
		WithInitial(1).
		WithQuiet().
		WithBinaryKey().
		WithCAS().
		WithKey().
		WithTTLEcho().
		WithOpaque([]byte("5")).
		WithCompareCAS(1).
		WithNewCAS(2).
		WithTTL(Rel(time.Second)).
		WithAutoCreate(Rel(time.Minute)).
		WithProxyHint("ha").
		WithProxyPath("la").
		WithExtra("Za")
	af := ar.toFlags()
	if af.M != MetaArithmeticModeDecr || !af.v || !af.Jset || af.J != 1 || !af.q || !af.b || !af.c || !af.k || !af.t {
		t.Fatalf("arith flags=%+v", af)
	}
	if !af.Cset || af.C != 1 || !af.Eset || af.E != 2 || !af.Tset || !af.Nset {
		t.Fatalf("arith CAS/exp flags=%+v", af)
	}
	if af.P != "ha" || af.L != "la" || len(af.extra) != 1 || af.extra[0] != "Za" {
		t.Fatalf("arith proxy/extra=%+v", af)
	}
}

func TestMetaDebugNoopConvenience(t *testing.T) {
	d := MetaDebug{}.WithBinaryKey().WithProxyHint("h").WithProxyPath("p").WithExtra("Ze")
	df := d.toFlags()
	if !df.b || df.P != "h" || df.L != "p" || len(df.extra) != 1 || df.extra[0] != "Ze" {
		t.Fatalf("debug flags=%+v", df)
	}

	n := MetaNoop{}.WithProxyHint("hn").WithProxyPath("ln").WithExtra("Zn")
	if n.ProxyHint != "hn" || n.ProxyPath != "ln" || len(n.Extra) != 1 || n.Extra[0] != "Zn" {
		t.Fatalf("noop=%+v", n)
	}
	noError(t, n.validate())
}
