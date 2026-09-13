package memcached

import (
	"testing"
)

func Test_buildMetaSetCommand(t *testing.T) {
	key := []byte("foo")
	value := []byte("bar")

	tests := []struct {
		name              string
		flags             *metaSetFlags
		wantRequestRaw    []byte
		wantRespIndicator responseEndIndicator
	}{
		{
			name: "normal1:all set to set",
			flags: &metaSetFlags{
				b: true,
				c: true,
				C: 1,
				E: 2,
				F: 3,
				I: true,
				k: true,
				O: 4,
				q: true,
				s: true,
				T: 5,
				M: MetaSetModeSet,
				N: 6,
			},
			wantRequestRaw:    []byte("ms Zm9v 3 b c C1 E2 F3 I k O4 q s T5 MS N6\r\nbar\r\nmn\r\n"),
			wantRespIndicator: endIndicatorQuietBarrier,
		},
		{
			name: "normal2:not binary no quite replace",
			flags: &metaSetFlags{
				b: false,
				c: true,
				C: 1,
				E: 2,
				F: 3,
				I: true,
				k: true,
				O: 4,
				q: false,
				s: true,
				T: 5,
				M: MetaSetModeReplace,
				N: 6,
			},
			wantRequestRaw:    []byte("ms foo 3 c C1 E2 F3 I k O4 s T5 MR N6\r\nbar\r\n"),
			wantRespIndicator: endIndicatorLimitedLines,
		},
		{
			name:              "omits F0",
			flags:             &metaSetFlags{},
			wantRequestRaw:    []byte("ms foo 3\r\nbar\r\n"),
			wantRespIndicator: endIndicatorLimitedLines,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp := buildMetaSetCommand(key, value, tt.flags)
			equal(t, string(tt.wantRequestRaw), string(req.raw))
			equal(t, tt.wantRespIndicator, resp.endIndicator)
		})
	}
}
