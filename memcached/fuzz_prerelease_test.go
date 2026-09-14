package memcached

import (
	"bytes"
	"testing"
)

func FuzzReadRetrievalFrame(f *testing.F) {
	f.Add([]byte("VALUE k 0 1\r\nx\r\nEND\r\n"))
	f.Add([]byte("VALUE k 0 1\r\nx\r\nEND\r\nEND\r\n"))
	f.Add([]byte("VALUE k 0 3\r\nab\r\nEND\r\n"))
	f.Add([]byte("END\r\n"))
	f.Add([]byte("SERVER_ERROR out of memory\r\n"))
	f.Fuzz(func(_ *testing.T, payload []byte) {
		dst := []GetResult{{}}
		resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
		defer resp.release()
		_ = resp.readRetrieval(newPayloadReader(payload))
	})
}

func FuzzMetaPipelineFrames(f *testing.F) {
	f.Add([]byte("HD O0\r\nMN\r\n"))
	f.Add([]byte("VA 1 O0\r\nx\r\nMN\r\n"))
	f.Add([]byte("HD O0\r\nHD O0\r\nMN\r\n"))
	f.Add([]byte("EN O0\r\nMN\r\n"))
	f.Fuzz(func(_ *testing.T, payload []byte) {
		ops := []preparedMetaOp{
			{idx: 0, hasGet: true, getFlags: metaGetFlags{O: 0, Oset: true}},
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
		_ = resp.read2(newPayloadReader(payload), true)
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
	})
}

func FuzzProtocolLine(f *testing.F) {
	f.Add([]byte("STORED\r\n"))
	f.Add([]byte("STAT k v\r\n"))
	f.Add(append(bytes.Repeat([]byte("A"), 100), '\n'))
	f.Fuzz(func(_ *testing.T, line []byte) {
		pr := newPayloadReader(line)
		_, _ = pr.readLine('\n')
	})
}
