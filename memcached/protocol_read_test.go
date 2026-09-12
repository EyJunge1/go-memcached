package memcached

import (
	"testing"
)

func TestReadRetrievalValueContainsCRLFAndEND(t *testing.T) {
	payload := []byte("VALUE k 0 11\r\nhello\nworld\r\nEND\r\n")
	dst := make([]GetResult, 1)
	resp := buildRetrievalResponse([][]byte{[]byte("k")}, nil, dst, false)
	defer resp.release()
	noError(t, resp.readRetrieval(newPayloadReader(payload)))
	isTrue(t, dst[0].Found)
	equal(t, []byte("hello\nworld"), dst[0].Value)
}

func TestRead2ValueContainsCRLFAndEND(t *testing.T) {
	payload := []byte("VALUE k 0 11\r\nhello\nworld\r\nEND\r\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 3)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	items, err := parseValueItems(resp.rawLines, false)
	noError(t, err)
	items = requireSlice(t, items)
	hasLen(t, items, 1)
	equal(t, []byte("hello\nworld"), items[0].Value)
}

func TestRead2AcceptsLFOnlyEND(t *testing.T) {
	payload := []byte("END\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 1)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	hasLen(t, resp.rawLines, 1)
}

func TestRead2AcceptsLFOnlyMN(t *testing.T) {
	payload := []byte("HD O1\r\nMN\n")
	resp := buildQuietBarrierResponse()
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), true))
	item := &MetaItem{}
	noError(t, parseMetaItem(resp.rawLines, item, true, false))
	equal(t, MetaStatusHD, item.status)
}

func TestRead2ValueLooksLikeEND(t *testing.T) {
	payload := []byte("VALUE k 0 3\r\nEND\r\nEND\r\n")
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 3)
	defer resp.release()
	noError(t, resp.read2(newPayloadReader(payload), false))
	items, err := parseValueItems(resp.rawLines, false)
	noError(t, err)
	items = requireSlice(t, items)
	hasLen(t, items, 1)
	equal(t, []byte("END"), items[0].Value)
}

func TestRead1VAContainsCRLF(t *testing.T) {
	payload := []byte("VA 11\r\nhello\nworld\r\n")
	resp := buildLimitedLineResponse(1)
	defer resp.release()
	noError(t, resp.read1(newPayloadReader(payload)))
	item := &MetaItem{}
	noError(t, parseMetaItem(resp.rawLines, item, false, false))
	equal(t, []byte("hello\nworld"), item.Value)
}

func TestRead1HDIsOneProtocolRecord(t *testing.T) {
	resp := buildLimitedLineResponse(1)
	defer resp.release()
	noError(t, resp.read1(newPayloadReader([]byte("HD c1\r\n"))))
	hasLen(t, resp.rawLines, 1)
	equal(t, []byte("HD c1\r\n"), resp.rawLines[0])
}
