package memcached

import (
	"testing"
)

func Test_parseFlags(t *testing.T) {
	type args struct {
		parts    [][]byte
		startPos int
		item     *MetaItem
	}
	tests := []struct {
		name string
		args args
		want *MetaItem
	}{
		{
			name: "normal",
			args: args{
				parts:    constructParts([]byte("HD c26 kZm9v b O456 s3\r\n")),
				startPos: 1,
				item:     &MetaItem{},
			},
			want: &MetaItem{
				Key:              []byte("foo"),
				Value:            nil,
				CAS:              26,
				Flags:            0,
				TTL:              0,
				LastAccess: 0,
				Size:             3,
				Opaque:      "456",
				HitBefore:        false,
				WireKey:          []byte("Zm9v"),
				KeyBase64:        true,
			},
		},
		{
			name: "with flags",
			args: args{
				parts:    constructParts([]byte("VA 3 c29 f123 h1 kZm9v b l0 O789 s3 t200\r\n")),
				startPos: 2,
				item:     &MetaItem{},
			},
			want: &MetaItem{
				Key:              []byte("foo"),
				Value:            nil,
				CAS:              29,
				Flags:            123,
				TTL:              200,
				LastAccess: 0,
				Size:             3,
				Opaque:      "789",
				HitBefore:        true,
				WireKey:          []byte("Zm9v"),
				KeyBase64:        true,
			},
		},
		{
			name: "recache WXZ",
			args: args{
				parts:    constructParts([]byte("HD W X Z Oopaque\r\n")),
				startPos: 1,
				item:     &MetaItem{},
			},
			want: &MetaItem{
				Won:         true,
				Stale:       true,
				RecacheBusy: true,
				Opaque: "opaque",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			noError(t, parseFlags(tt.args.parts, tt.args.startPos, tt.args.item))
			equal(t, tt.want, tt.args.item)
		})
	}
}

func Test_parseMetaItem(t *testing.T) {
	type args struct {
		lines   [][]byte
		item    *MetaItem
		noReply bool
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantItem *MetaItem
	}{
		{
			name: "normal1: not found error and noReply false",
			args: args{
				lines: [][]byte{
					// ERROR
					[]byte("NF\r\n"),
				},
				item:    &MetaItem{},
				noReply: false,
			},
			wantErr:  true,
			wantItem: nil,
		},
		{
			name: "normal2: miss error and noReply true",
			args: args{
				lines: [][]byte{
					// ERROR
					[]byte("EN\r\n"),
				},
				item:    &MetaItem{},
				noReply: true,
			},
			wantErr:  true,
			wantItem: nil,
		},
		{
			name: "normal3: not found error flags",
			args: args{
				lines: [][]byte{
					// ERROR
					[]byte("EN kfoo\r\n"),
				},
				item:    &MetaItem{},
				noReply: false,
			},
			wantErr:  true,
			wantItem: nil,
		},
		{
			name: "normal3: HD",
			args: args{
				lines: [][]byte{
					// HD c26 kZm9v b O456 s3
					[]byte("HD c26 kZm9v b O456 s3\r\n"),
				},
				item:    &MetaItem{},
				noReply: false,
			},
			wantErr: false,
			wantItem: &MetaItem{
				Key:              []byte("foo"),
				Value:            nil,
				CAS:              26,
				Flags:            0,
				TTL:              0,
				LastAccess: 0,
				Size:             3,
				Opaque:      "456",
				HitBefore:        false,
				WireKey:          []byte("Zm9v"),
				KeyBase64:        true,
				status:           MetaStatusHD,
				gotHD:            true,
			},
		},
		{
			name: "normal4: VA",
			args: args{
				lines: [][]byte{
					// VA 3 c29 f123 h1 kZm9v b l0 O789 s3 t200
					// bar
					[]byte("VA 3 c29 f123 h1 kZm9v b l0 O789 s3 t200\r\n"),
					[]byte("bar\r\n"),
				},
				item:    &MetaItem{},
				noReply: false,
			},
			wantErr: false,
			wantItem: &MetaItem{
				Key:              []byte("foo"),
				Value:            []byte("bar"),
				CAS:              29,
				Flags:            123,
				TTL:              200,
				LastAccess: 0,
				Size:             3,
				Opaque:      "789",
				HitBefore:        true,
				WireKey:          []byte("Zm9v"),
				KeyBase64:        true,
				status:           MetaStatusVA,
			},
		},
		{
			name: "malformed1: missing data block",
			args: args{
				lines: [][]byte{
					// VA 3 c29 f123 h1 kZm9v b l0 O789 s3 t200
					[]byte("VA 3 c29 f123 h1 kZm9v b l0 O789 s3 t200\r\n"),
				},
				item:    &MetaItem{},
				noReply: false,
			},
			wantErr:  true,
			wantItem: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseMetaItem(tt.args.lines, tt.args.item, tt.args.noReply, false)
			if tt.wantErr {
				isError(t, err)
				return
			}

			noError(t, err)
			equal(t, tt.wantItem, tt.args.item)
		})
	}
}

func Test_parseMetaItemPreservesOpaqueValue(t *testing.T) {
	value := []byte{0x1f, 0x8b, 0x08, 0x00}
	item := &MetaItem{}
	err := parseMetaItem(
		[][]byte{
			[]byte("VA 4 f18\r\n"),
			append(append([]byte{}, value...), []byte("\r\n")...),
		},
		item,
		false,
		false,
	)
	noError(t, err)
	equal(t, value, item.Value)
	equal(t, uint32(18), item.Flags)
}

func Test_buildMetaGetCommand(t *testing.T) {
	key := []byte("foo")

	tests := []struct {
		name              string
		flags             *metaGetFlags
		wantRequestRaw    []byte
		wantRespIndicator responseEndIndicator
	}{
		{
			name: "normal1:all set",
			flags: &metaGetFlags{
				b: true,
				c: true,
				f: true,
				h: true,
				k: true,
				l: true,
				O: 1,
				q: true,
				s: true,
				t: true,
				u: true,
				v: true,
				E: 2,
				N: 3,
				R: 4,
				T: 5,
			},
			wantRequestRaw:    []byte("mg Zm9v b c f h k l O1 q s t u v E2 N3 R4 T5\r\nmn\r\n"),
			wantRespIndicator: endIndicatorQuietBarrier,
		},
		{
			name: "normal2:not binary no quite",
			flags: &metaGetFlags{
				b: false,
				c: true,
				f: true,
				h: true,
				k: true,
				l: true,
				O: 1,
				q: false,
				s: true,
				t: true,
				u: true,
				v: true,
				E: 2,
				N: 3,
				R: 4,
				T: 5,
			},
			wantRequestRaw:    []byte("mg foo c f h k l O1 s t u v E2 N3 R4 T5\r\n"),
			wantRespIndicator: endIndicatorLimitedLines,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, resp := buildMetaGetCommand(key, tt.flags)
			equal(t, string(tt.wantRequestRaw), string(req.raw))
			equal(t, tt.wantRespIndicator, resp.endIndicator)
		})
	}
}

func Test_buildMetaGetCommandKeepsKeyUnchanged(t *testing.T) {
	flags := &metaGetFlags{v: true, f: true}
	req, _ := buildMetaGetCommand([]byte("foo"), flags)

	equal(t, []byte("foo"), req.key)
	contains(t, string(req.raw), "mg foo")
}

func Test_buildMetaGetCommand_emitsT0(t *testing.T) {
	req, _ := buildMetaGetCommand([]byte("foo"), &metaGetFlags{T: 0, Tset: true})
	equal(t, []byte("mg foo T0\r\n"), req.raw)
}

func Test_parseMetaItem_quietOnlyMN(t *testing.T) {
	item := &MetaItem{}
	noError(t, parseMetaItem([][]byte{_MetaMNCRLFBytes}, item, true, false))
	equal(t, MetaStatusHD, item.status)
}

func Test_parseMetaItem_quietWantValueMiss(t *testing.T) {
	item := &MetaItem{}
	isErrorIs(t, parseMetaItem([][]byte{_MetaMNCRLFBytes}, item, true, true), ErrNotFound)
	equal(t, MetaStatusEN, item.status)
}

func Test_buildMetaGetCommand_checkCASAndOpaqueToken(t *testing.T) {
	req, _ := buildMetaGetCommand([]byte("foo"), &metaGetFlags{C: 9, Cset: true, Otoken: "opaque", OtokenSet: true, v: true})
	equal(t, []byte("mg foo Oopaque v C9\r\n"), req.raw)
}

func Test_buildMetaGetCommand_emitsO0(t *testing.T) {
	req, _ := buildMetaGetCommand([]byte("foo"), &metaGetFlags{O: 0, Oset: true})
	equal(t, []byte("mg foo O0\r\n"), req.raw)
}

func Test_buildMetaGetCommand_PL(t *testing.T) {
	req, _ := buildMetaGetCommand([]byte("foo"), &metaGetFlags{P: "h", L: "/p"})
	equal(t, []byte("mg foo Ph L/p\r\n"), req.raw)
}

func Test_parseMetaPipeline_flagError(t *testing.T) {
	_, err := parseMetaPipeline([][]byte{[]byte("HD cxyz\r\n"), _MetaMNCRLFBytes})
	isErrorIs(t, err, ErrMalformedResponse)
}

func Test_parseMetaPipeline_chargeKeepsSibling(t *testing.T) {
	items, err := parseMetaPipeline([][]byte{
		[]byte("HD O0\r\n"),
		[]byte("CLIENT_ERROR bad meta\r\n"),
		_MetaMNCRLFBytes,
	})
	isErrorIs(t, err, ErrClientError)
	hasLen(t, items, 1)
	equal(t, MetaStatusHD, items[0].status)
	equal(t, "0", items[0].Opaque)
}

func Test_parseMetaItem_quietOnlyMN_LF(t *testing.T) {
	item := &MetaItem{}
	noError(t, parseMetaItem([][]byte{[]byte("MN\n")}, item, true, false))
	equal(t, MetaStatusHD, item.status)
}

func Test_parseMetaItem_valueSizeMismatch(t *testing.T) {
	item := &MetaItem{}
	err := parseMetaItem([][]byte{
		[]byte("VA 5\r\n"),
		[]byte("ab\r\n"),
	}, item, false, false)
	isErrorIs(t, err, ErrMalformedResponse)
}
