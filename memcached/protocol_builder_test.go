package memcached

import (
	"context"
	"testing"
	"time"
)

func Test_protocolBuilder(t *testing.T) {
	builder := newProtocolBuilder().
		AddString("set").AddString("key").AddInt(0).AddInt(0).AddInt(5).
		AddCRLF().
		AddString("value")
	expected := "set key 0 0 5\r\nvalue\r\n"
	equal(t, expected, string(builder.build()))
	builder.release()
}

func Test_protocolBuilder_releaseDiscardsOversizedBuffer(t *testing.T) {
	large := make([]byte, 0, maxPooledBufferCap+1024)
	large = append(large, make([]byte, maxPooledBufferCap+1)...)
	b := &protocolBuilder{box: &large}
	b.release()
	isNil(t, b.box)

	for range 32 {
		pb := newProtocolBuilder()
		capSize := cap(pb.buf())
		pb.release()
		lessOrEqual(t, capSize, maxPooledBufferCap)
	}
}

func Test_protocolBuilder_shortCommandsFitDefaultCap(t *testing.T) {
	cases := []struct {
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pb := tc.build()
			defer pb.release()
			lessOrEqual(t, len(pb.buf()), defaultBufferSize)
			lessOrEqual(t, cap(pb.buf()), maxPooledBufferCap)
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

func Test_selectProximateDeadline(t *testing.T) {
	mockNowFunc := func() time.Time {
		return time.Date(2021, 7, 5, 0, 0, 0, 0, time.UTC)
	}
	baseTime := mockNowFunc()

	tests := []struct {
		name         string
		ctx          context.Context
		conn         *mockConn
		timeout      time.Duration
		wantDeadline time.Time
		wantHas      bool
	}{
		{
			name:         "nil context and zero timeout",
			ctx:          nil,
			conn:         newMockConn(),
			timeout:      0,
			wantDeadline: time.Time{},
			wantHas:      false,
		},
		{
			name:         "nil context and negative timeout",
			ctx:          nil,
			conn:         newMockConn(),
			timeout:      -1 * time.Second,
			wantDeadline: time.Time{},
			wantHas:      false,
		},
		{
			name:         "background context and positive timeout",
			ctx:          context.Background(),
			conn:         newMockConn(),
			timeout:      time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
		{
			name: "context with deadline and zero timeout",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(2*time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      0,
			wantDeadline: baseTime.Add(2 * time.Second),
			wantHas:      true,
		},
		{
			name: "context with earlier deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      2 * time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
		{
			name: "timeout earlier than context deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), baseTime.Add(2*time.Second))
				t.Cleanup(cancel)
				return ctx
			}(),
			conn:         newMockConn(),
			timeout:      time.Second,
			wantDeadline: baseTime.Add(time.Second),
			wantHas:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHas := selectProximateDeadline(tt.ctx, tt.conn, tt.timeout, mockNowFunc, true)
			equal(t, tt.wantHas, gotHas)
			equal(t, tt.wantDeadline, tt.conn.readDeadline)
		})
	}
}

func Test_readRetrievalValueContainsCRLFAndEND(t *testing.T) {
	payload := []byte("VALUE k 0 11\r\nhello\nworld\r\nEND\r\n")
	dst := make([]GetResult, 1)
	resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
	defer resp.release()
	noError(t, resp.readRetrieval(newPayloadReader(payload)))
	isTrue(t, dst[0].Found)
	equal(t, []byte("hello\nworld"), dst[0].Value)
}

func Test_read2ValueContainsCRLFAndEND(t *testing.T) {
	payload := []byte("VALUE k 0 11\r\nhello\nworld\r\nEND\r\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 3)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	items, err := parseValueItems(resp.rawLines, false)
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("hello\nworld"), items[0].Value)
}

func Test_read2AcceptsLFOnlyEND(t *testing.T) {
	payload := []byte("END\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 1)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	hasLen(t, resp.rawLines, 1)
}

func Test_read2AcceptsLFOnlyMN(t *testing.T) {
	payload := []byte("HD O1\r\nMN\n")
	resp := buildQuietBarrierResponse()
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), true))
	item := &MetaItem{}
	noError(t, parseMetaItem(resp.rawLines, item, true, false))
	equal(t, MetaStatusHD, item.status)
}

func Test_read2ValueLooksLikeEND(t *testing.T) {
	payload := []byte("VALUE k 0 3\r\nEND\r\nEND\r\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 3)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	items, err := parseValueItems(resp.rawLines, false)
	noError(t, err)
	hasLen(t, items, 1)
	equal(t, []byte("END"), items[0].Value)
}

func Test_read1VAContainsCRLF(t *testing.T) {
	payload := []byte("VA 11\r\nhello\nworld\r\n")
	resp := buildLimitedLineResponse(1)
	defer resp.release()
	noError(t, resp.read1(newPayloadReader(payload)))
	item := &MetaItem{}
	noError(t, parseMetaItem(resp.rawLines, item, false, false))
	equal(t, []byte("hello\nworld"), item.Value)
}

func Test_read1HDIsOneProtocolRecord(t *testing.T) {
	resp := buildLimitedLineResponse(1)
	defer resp.release()
	noError(t, resp.read1(newPayloadReader([]byte("HD c1\r\n"))))
	hasLen(t, resp.rawLines, 1)
	equal(t, []byte("HD c1\r\n"), resp.rawLines[0])
}

func Test_forecastCommonFaultLine(t *testing.T) {
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_FOUND\r\n")), ErrNotFound)
	isErrorIs(t, forecastCommonFaultLine([]byte("EXISTS\r\n")), ErrExists)
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_STORED\r\n")), ErrNotStored)
	equal(t, error(nil), forecastCommonFaultLine([]byte("EN\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("EN kfoo\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("NF\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("NS\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("EX\r\n")))
	isErrorIs(t, forecastCommonFaultLine([]byte("ERROR\r\n")), ErrNonexistentCommand)
	isErrorIs(t, forecastCommonFaultLine([]byte("ERROR bad args\r\n")), ErrCommandError)
	isErrorIs(t, forecastCommonFaultLine([]byte("ERROR: shutdown not enabled\r\n")), ErrCommandError)
	isErrorIs(t, forecastCommonFaultLine([]byte("SERVER_ERROR: shutdown not enabled\r\n")), ErrServerError)
	equal(t, error(nil), forecastCommonFaultLine([]byte("END\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("ENTRY\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("VALUE k 0 3\r\n")))
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR bad flags\r\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR\r\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("CLIENT_ERROR\n")), ErrClientError)
	isErrorIs(t, forecastCommonFaultLine([]byte("NOT_STORED\n")), ErrNotStored)
	isErrorIs(t, forecastCommonFaultLine([]byte("SERVER_ERROR oom")), ErrServerError)
	equal(t, error(nil), forecastCommonFaultLine([]byte("CLIENT_ERRORFOO\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("12\r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("12 \r\n")))
	equal(t, error(nil), forecastCommonFaultLine([]byte("NF kfoo\r\n")))
}
