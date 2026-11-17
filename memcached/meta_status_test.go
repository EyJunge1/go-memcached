package memcached

import "testing"

func TestMetaStatusStringAndErr(t *testing.T) {
	cases := []struct {
		s    MetaStatus
		want string
		err  error
	}{
		{MetaStatusHD, "HD", nil},
		{MetaStatusVA, "VA", nil},
		{MetaStatusEN, "EN", ErrNotFound},
		{MetaStatusNF, "NF", ErrNotFound},
		{MetaStatusNS, "NS", ErrNotStored},
		{MetaStatusEX, "EX", ErrExists},
		{MetaStatusMN, "MN", nil},
		{MetaStatusME, "ME", nil},
		{MetaStatusUnknown, "??", nil},
		{0, "??", nil},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Fatalf("%v.String()=%q want %q", tc.s, got, tc.want)
		}
		if tc.err == nil {
			noError(t, tc.s.Err())
		} else {
			isErrorIs(t, tc.s.Err(), tc.err)
		}
	}

	r := MetaResult{Status: MetaStatusNF}
	isErrorIs(t, r.Err(), ErrNotFound)
	r.err = ErrClosed
	isErrorIs(t, r.Err(), ErrClosed)
}
