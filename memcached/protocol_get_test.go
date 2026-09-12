package memcached

import (
	"testing"
)

func TestParseValueItems(t *testing.T) {
	type args struct {
		lines   [][]byte
		withCAS bool
	}
	tests := []struct {
		name    string
		args    args
		want    []Item
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 0 5\r\n"),
					[]byte("value\r\n"),
					[]byte("VALUE key2 0 6\r\n"),
					[]byte("value2\r\n"),
					[]byte("END\r\n"),
				},
				withCAS: false,
			},
			want: []Item{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				},
			},
			wantErr: false,
		},
		{
			name: "with flags/cas-unique",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key 123 5 1\r\n"),
					[]byte("value\r\n"),
					[]byte("VALUE key2 123 6 2\r\n"),
					[]byte("value2\r\n"),
					[]byte("END\r\n"),
				},
				withCAS: true,
			},
			want: []Item{
				{
					Key:   []byte("key"),
					Value: []byte("value"),
					Flags: 123,
					CAS:   1,
				},
				{
					Key:   []byte("key2"),
					Value: []byte("value2"),
					Flags: 123,
					CAS:   2,
				},
			},
			wantErr: false,
		},
		{
			name: "malformed-1",
			args: args{
				lines: [][]byte{
					[]byte("VALUE key -1 5\r\n"),
					[]byte("value\r\n"),
					[]byte("END\r\n"),
				},
				withCAS: false,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "rejects non-VALUE protocol line",
			args: args{
				lines: [][]byte{
					[]byte("HD\r\n"),
					[]byte("ignored\r\n"),
					[]byte("END\r\n"),
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseValueItems(tt.args.lines, tt.args.withCAS)
			if tt.wantErr {
				isError(t, err)
				return
			}

			noError(t, err)
			equal(t, tt.want, got)
		})
	}
}

func TestParseValueItemsPreservesOpaqueValue(t *testing.T) {
	value := []byte{0x1f, 0x8b, 0x08, 0x00}

	lines := [][]byte{
		[]byte("VALUE key 18 4\r\n"),
		append(append([]byte(nil), value...), []byte("\r\n")...),
		[]byte("END\r\n"),
	}

	items, err := parseValueItems(lines, false)
	noError(t, err)
	items = requireSlice(t, items)
	hasLen(t, items, 1)
	equal(t, value, items[0].Value)
	equal(t, uint32(18), items[0].Flags)
}

func TestTakeValueBufferKeepsCap(t *testing.T) {
	buf := make([]byte, 0, 32)
	got := takeValueBuffer(buf, 1)
	got = requireSlice(t, got)
	equal(t, 1, len(got))
	equal(t, 32, cap(got))
	same(t, buf[:1], got[:1])

	owned := ownValue(buf[:0], got)
	owned = requireSlice(t, owned)
	same(t, got[:1], owned[:1])
	equal(t, 32, cap(owned))
}

func TestDataBlockValueUsesDeclaredLength(t *testing.T) {
	raw := []byte("ab\r\n")
	got, err := dataBlockValue(raw, 2)
	noError(t, err)
	equal(t, []byte("ab"), got)

	valueCRLF := []byte("\r\n\r\n")
	got, err = dataBlockValue(valueCRLF, 2)
	noError(t, err)
	equal(t, []byte("\r\n"), got)

	_, err = dataBlockValue([]byte("ab"), 2)
	isErrorIs(t, err, ErrMalformedResponse)
}
