package memcached

import (
	"bytes"
	"testing"
)

func TestProtocolBuilder(t *testing.T) {
	builder := newProtocolBuilder().
		AddString("set").AddString("key").AddInt(0).AddInt(0).AddInt(5).
		AddCRLF().
		AddString("value")
	expected := "set key 0 0 5\r\nvalue\r\n"
	equal(t, expected, string(builder.build()))
	builder.release()
}

func TestProtocolBuilderReleaseDiscardsOversizedBuffer(t *testing.T) {
	large := make([]byte, 0, _maxPooledBufferCap+1024)
	large = append(large, make([]byte, _maxPooledBufferCap+1)...)
	b := &protocolBuilder{box: &large}
	b.release()
	isNil(t, b.box)

	for range 32 {
		pb := newProtocolBuilder()
		capSize := cap(pb.buf())
		pb.release()
		lessOrEqual(t, capSize, _maxPooledBufferCap)
	}
}

func TestProtocolBuilderShortCommandsFitDefaultCap(t *testing.T) {
	tests := []struct {
		name  string
		build func() *protocolBuilder
	}{
		{
			name: "get",
			build: func() *protocolBuilder {
				return newProtocolBuilder().AddString("get").AddString("test_key").AddCRLF()
			},
		},
		{
			name: "delete",
			build: func() *protocolBuilder {
				return newProtocolBuilder().AddString("delete").AddString("test_key").AddCRLF()
			},
		},
		{
			name: "set small",
			build: func() *protocolBuilder {
				value := []byte("test_value")
				return newProtocolBuilder().
					AddString("set").AddString("test_key").AddInt(0).AddInt(0).AddInt(len(value)).
					AddCRLF().
					AddBytes(value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pb := tt.build()
			defer pb.release()
			lessOrEqual(t, len(pb.buf()), _defaultBufferSize)
			lessOrEqual(t, cap(pb.buf()), _maxPooledBufferCap)
		})
	}
}

func BenchmarkProtocolBuilder_GetShortKey(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		pb := newProtocolBuilder().AddString("get").AddString("test_key").AddCRLF()
		_, box := pb.takeBytes()
		putBuilderBox(box)
		pb.release()
	}
}

func BenchmarkProtocolBuilder_DeleteShortKey(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		pb := newProtocolBuilder().AddString("delete").AddString("test_key").AddCRLF()
		_, box := pb.takeBytes()
		putBuilderBox(box)
		pb.release()
	}
}

func BenchmarkProtocolBuilder_SetSmallValue(b *testing.B) {
	b.ReportAllocs()
	value := []byte("test_value")
	for b.Loop() {
		pb := newProtocolBuilder().
			AddString("set").AddString("test_key").AddInt(0).AddInt(0).AddInt(len(value)).
			AddCRLF().
			AddBytes(value)
		_, box := pb.takeBytes()
		putBuilderBox(box)
		pb.release()
	}
}

func BenchmarkProtocolBuilder_MetaSetManyFlags(b *testing.B) {
	b.ReportAllocs()
	value := []byte("test_value")
	for b.Loop() {
		pb := newProtocolBuilder().
			AddString("ms").AddBytes([]byte("test_key")).
			AddString("c").AddString("C").AddString("F").AddUint(1).
			AddString("T").AddUint(60).AddString("S").AddInt(len(value)).
			AddCRLF().
			AddBytes(value)
		_, box := pb.takeBytes()
		putBuilderBox(box)
		pb.release()
	}
}

func BenchmarkProtocolBuilder_SetLargeValue(b *testing.B) {
	b.ReportAllocs()
	value := make([]byte, 4096)
	for b.Loop() {
		pb := newProtocolBuilder().
			AddString("set").AddString("test_key").AddInt(0).AddInt(0).AddInt(len(value)).
			AddCRLF().
			AddBytes(value)
		_, box := pb.takeBytes()
		putBuilderBox(box)
		pb.release()
	}
}

func TestForecastCommonFaultLine(t *testing.T) {
	tests := []struct {
		give string
		want error
	}{
		{give: "NOT_FOUND\r\n", want: ErrNotFound},
		{give: "EXISTS\r\n", want: ErrExists},
		{give: "NOT_STORED\r\n", want: ErrNotStored},
		{give: "EN\r\n"},
		{give: "EN kfoo\r\n"},
		{give: "NF\r\n"},
		{give: "NS\r\n"},
		{give: "EX\r\n"},
		{give: "ERROR\r\n", want: ErrNonexistentCommand},
		{give: "ERROR bad args\r\n", want: ErrCommandError},
		{give: "ERROR: shutdown not enabled\r\n", want: ErrCommandError},
		{give: "SERVER_ERROR: shutdown not enabled\r\n", want: ErrServerError},
		{give: "END\r\n"},
		{give: "ENTRY\r\n"},
		{give: "VALUE k 0 3\r\n"},
		{give: "CLIENT_ERROR bad flags\r\n", want: ErrClientError},
		{give: "CLIENT_ERROR", want: ErrClientError},
		{give: "CLIENT_ERROR\r\n", want: ErrClientError},
		{give: "CLIENT_ERROR\n", want: ErrClientError},
		{give: "NOT_STORED\n", want: ErrNotStored},
		{give: "SERVER_ERROR oom", want: ErrServerError},
		{give: "CLIENT_ERRORFOO\r\n"},
		{give: "12\r\n"},
		{give: "12 \r\n"},
		{give: "NF kfoo\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.give, func(t *testing.T) {
			isErrorIs(t, forecastCommonFaultLine([]byte(tt.give)), tt.want)
		})
	}
}

func TestPrefixedProtocolErrorCapsServerText(t *testing.T) {
	line := append([]byte("SERVER_ERROR "), bytes.Repeat([]byte("x"), (4<<10)+100)...)
	line = append(line, '\r', '\n')
	err := prefixedProtocolError(line, _ServerErrorBytes, ErrServerError)
	isErrorIs(t, err, ErrServerError)
	if len(errText(t, err)) > (4<<10)+len(ErrServerError.Error())+3 {
		t.Fatalf("server error text was not capped: %d bytes", len(errText(t, err)))
	}
}
