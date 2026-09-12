package memcached

import (
	"testing"
	"time"
)

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
	set := MetaSet{}.WithQuiet().WithFlags(3).WithMode(MetaSetModeAdd).WithTTL(Rel(time.Second)).WithCAS()
	sf := set.toFlags()
	if !sf.q || sf.F != 3 || sf.M != MetaSetModeAdd || !sf.Tset || !sf.c {
		t.Fatalf("set flags=%+v", sf)
	}

	del := MetaDelete{}.WithInvalidate().WithRemoveValueOnly().WithKey()
	df := del.toFlags()
	if !df.I || !df.x || !df.k {
		t.Fatalf("delete flags=%+v", df)
	}

	ar := MetaArithmetic{}.WithMode(MetaArithmeticModeDecr).WithValue().WithInitial(1).WithQuiet()
	af := ar.toFlags()
	if af.M != MetaArithmeticModeDecr || !af.v || !af.Jset || af.J != 1 || !af.q {
		t.Fatalf("arith flags=%+v", af)
	}
}
