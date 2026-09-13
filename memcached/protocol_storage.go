package memcached

import (
	"time"
)

// buildStorageCommand constructs storage class command, including:
//
//	set/add/replace/append/prepend, etc.
//
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// <data block>\r\n
const maxRelativeExpiry = 30 * 24 * time.Hour

// expiryToken maps a Go duration onto memcached's exptime integer.
// 0 stays 0 (never expire). Sub-second positives become 1. Negatives become -1
// (immediately expired). Values above 30 days become now+ttl Unix timestamps.
func expiryToken(d time.Duration) int64 {
	switch {
	case d == 0:
		return 0
	case d < 0:
		return -1
	case d < time.Second:
		return 1
	case d > maxRelativeExpiry:
		return nowFunc().Add(d).Unix()
	default:
		return int64(d / time.Second)
	}
}

func buildStorageCommand(
	command, key string,
	value []byte,
	flags uint32,
	exptime time.Duration,
	noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddString(key).                 // key
		AddUint(uint64(flags)).         // flags
		AddInt64(expiryToken(exptime)). // exptime
		AddInt(len(value))              // bytes
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().
		AddBytes(value). // data block
		AddCRLF().
		build()

	req := buildRequest([]byte(command), []byte(key), raw)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// cas <key> <flag> <exptime> <bytes> <cas unique> [noreply]\r\n
func buildCasCommand(
	key string, value []byte, flag uint32, expTime time.Duration, casUnique uint64, noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString("cas").               // command
		AddString(key).                 // key
		AddUint(uint64(flag)).          // flags
		AddInt64(expiryToken(expTime)). // exptime
		AddInt(len(value)).             // bytes
		AddUint(casUnique)              // cas unique
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().
		AddBytes(value). // data block
		AddCRLF().
		build()

	req := buildRequest([]byte("cas"), []byte(key), raw)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// mapStorageReply maps a storage-class reply (set/add/replace/append/prepend/cas).
// STORED is success; NOT_STORED/EXISTS/NOT_FOUND stay protocol sentinels so the
// pooled conn is released. Anything else is malformed.
func mapStorageReply(resp *response) error {
	return mapStatusLine(resp, _StoredCRLFBytes)
}
