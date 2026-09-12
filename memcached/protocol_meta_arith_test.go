package memcached

import (
	"testing"
)

func TestBuildMetaArithmeticCommand(t *testing.T) {
	key := []byte("foo")
	delta := uint64(64)

	tests := []struct {
		name              string
		flags             *metaArithmeticFlags
		wantRequestRaw    []byte
		wantRespIndicator responseEndIndicator
	}{
		{
			name: "normal1:all set decrement",
			flags: &metaArithmeticFlags{
				b: true,
				C: 1,
				E: 2,
				N: 3,
				J: 4,
				D: 5,
				T: 6,
				M: MetaArithmeticModeDecr,
				O: 7,
				q: true,
				t: true,
				c: true,
				v: true,
				k: true,
			},
			wantRequestRaw:    []byte("ma Zm9v b C1 E2 N3 J4 D64 T6 MD O7 q t c v k\r\nmn\r\n"),
			wantRespIndicator: _endIndicatorQuietBarrier,
		},
		{
			name: "normal:not binary no quite increment",
			flags: &metaArithmeticFlags{
				b: false,
				C: 1,
				E: 2,
				N: 3,
				J: 4,
				D: 5,
				T: 6,
				M: MetaArithmeticModeIncr,
				O: 7,
				q: false,
				t: true,
				c: true,
				v: true,
				k: true,
			},
			wantRequestRaw:    []byte("ma foo C1 E2 N3 J4 D64 T6 O7 t c v k\r\n"),
			wantRespIndicator: _endIndicatorLimitedLines,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp := buildMetaArithmeticCommand(key, delta, tt.flags)
			equal(t, string(tt.wantRequestRaw), string(req.raw))
			equal(t, tt.wantRespIndicator, resp.endIndicator)
		})
	}
}

func TestBuildMetaArithmeticCommandDeltaZero(t *testing.T) {
	req, resp := buildMetaArithmeticCommand([]byte("foo"), 0, &metaArithmeticFlags{})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("ma foo D0\r\n"), req.raw)
}

func TestBuildMetaArithmeticCommandOmitsInjectedMode(t *testing.T) {
	req, resp := buildMetaArithmeticCommand([]byte("foo"), 1, &metaArithmeticFlags{
		M: MetaArithmeticMode(255),
	})
	defer releaseReqAndResp(req, resp)
	equal(t, []byte("ma foo D1\r\n"), req.raw)
}
