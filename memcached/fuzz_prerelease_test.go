package memcached

import (
	"bufio"
	"bytes"
	"testing"
)

func FuzzReadRetrievalFrame(f *testing.F) {
	f.Add([]byte("VALUE k 0 1\r\nx\r\nEND\r\n"))
	f.Add([]byte("VALUE k 0 1\r\nx\r\nEND\r\nEND\r\n"))
	f.Add([]byte("VALUE k 0 3\r\nab\r\nEND\r\n"))
	f.Add([]byte("VALUE k 0 1\r\na\r\nVALUE k 0 1\r\nb\r\nEND\r\n"))
	f.Add([]byte("END\r\n"))
	f.Add([]byte("SERVER_ERROR out of memory\r\n"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		dst := []GetResult{{}}
		resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
		defer resp.release()
		if err := resp.readRetrieval(newPayloadReader(payload)); err != nil {
			if dst[0].Found || len(dst[0].Value) != 0 {
				t.Fatalf("failed retrieval retained destination: %+v", dst[0])
			}
		}
	})
}

func FuzzMetaPipelineFrames(f *testing.F) {
	f.Add([]byte("HD O0\r\nMN\r\n"))
	f.Add([]byte("VA 1 O0\r\nx\r\nMN\r\n"))
	f.Add([]byte("HD O0\r\nHD O0\r\nMN\r\n"))
	f.Add([]byte("HD O999\r\nMN\r\n"))
	f.Add([]byte("HD O0 Qevil\r\nMN\r\n"))
	f.Add([]byte("EN O0\r\nMN\r\n"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		ops := []preparedMetaOp{
			{idx: 0, hasGet: true, getFlags: metaGetFlags{O: 0, Oset: true}, tok: "0"},
		}
		index, err := buildMetaPipelineIndex(ops)
		if err != nil {
			return
		}
		defer putMetaPipelineIndex(index)
		dst := []MetaResult{{}}
		resp := buildQuietBarrierResponse()
		defer resp.release()
		resp.metaPipeDst = dst
		resp.metaPipeGroup = ops
		resp.metaPipeIndex = index
		resetMetaPipeSeen(resp, len(ops))
		if err := resp.read2(newPayloadReader(payload), true); err != nil {
			invalidateMetaPipelineDest(nil, dst, ops)
			if dst[0].Status != MetaStatusUnknown || len(dst[0].Item.Value) != 0 {
				t.Fatalf("failed meta wave retained destination: %+v", dst[0])
			}
		}
	})
}

func FuzzAssembleUDPPayload(f *testing.F) {
	mk := func(id, seq, tot uint16, payload string) []byte {
		h := make([]byte, udpHeaderSize+len(payload))
		h[0] = byte(id >> 8)
		h[1] = byte(id)
		h[2] = byte(seq >> 8)
		h[3] = byte(seq)
		h[4] = byte(tot >> 8)
		h[5] = byte(tot)
		copy(h[udpHeaderSize:], payload)
		return h
	}
	f.Add(mk(3, 0, 1, "END\r\n"))
	f.Add(mk(3, 0, 2, "A"))
	f.Add([]byte{0, 3})
	f.Fuzz(func(_ *testing.T, dgram []byte) {
		cn := &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{dgram}}
		_, _, _ = assembleUDPPayload(cn, 3, 8, 64, true)

		payload := dgram
		if len(payload) > 128 {
			payload = payload[:128]
		}
		cut := len(payload) / 2
		first := mk(3, 0, 2, string(payload[:cut]))
		second := mk(3, 1, 2, string(payload[cut:]))
		pair := [][]byte{first, second}
		if len(payload) > 0 && payload[0]&1 != 0 {
			pair[0], pair[1] = pair[1], pair[0]
		}
		cn = &recordUDPConn{mockConn: newMockConn(), datagrams: pair}
		box, _, _ := assembleUDPPayload(cn, 3, 8, 64, true)
		putUDPBuf(box)

		second[5] = 3 // conflicting total count
		cn = &recordUDPConn{mockConn: newMockConn(), datagrams: [][]byte{first, second}}
		box, _, _ = assembleUDPPayload(cn, 3, 8, 64, true)
		putUDPBuf(box)
	})
}

func FuzzProtocolLine(f *testing.F) {
	f.Add([]byte("STORED\r\n"))
	f.Add([]byte("STAT k v\r\n"))
	f.Add(append(bytes.Repeat([]byte("A"), 100), '\n'))
	f.Add(append(bytes.Repeat([]byte("A"), protocolLineLimit+1), '\n'))
	f.Fuzz(func(_ *testing.T, line []byte) {
		r := bufio.NewReaderSize(bytes.NewReader(line), 1024)
		_, _ = readLimitedLine(r, '\n', protocolLineLimit)
	})
}
