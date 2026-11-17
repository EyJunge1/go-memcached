package memcached

import (
	"bufio"
	"fmt"
	"strconv"
	"time"
)

func (f *fakeMemcached) metaDelete(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	mf := parseFakeMetaFlags(fields[2:])
	invalidate := false
	for _, tok := range fields[2:] {
		if tok == "I" {
			invalidate = true
		}
	}
	key := fields[1]
	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.getLiveLocked(key, now)
	if !ok {
		writeFakeMetaStatus(w, "NF", mf.quiet, mf.opaque)
		return
	}
	if invalidate {
		item.stale = true
		f.cas++
		item.cas = f.cas
		if mf.ttlSet {
			item.expiresAt = fakeExpiryFromToken(mf.ttl, now)
		}
	} else {
		delete(f.items, key)
	}
	writeFakeMetaStatus(w, "HD", mf.quiet, mf.opaque)
}

func (f *fakeMemcached) metaArith(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	mf := parseFakeMetaFlags(fields[2:])
	delta := uint64(1)
	initial := uint64(0)
	autoCreate := mf.vivify
	var autoTTL int64
	if mf.vivify {
		autoTTL = mf.vivifyTTL
	}
	for _, tok := range fields[2:] {
		switch {
		case len(tok) > 1 && tok[0] == 'D':
			delta, _ = strconv.ParseUint(tok[1:], 10, 64)
		case len(tok) > 1 && tok[0] == 'J':
			initial, _ = strconv.ParseUint(tok[1:], 10, 64)
		}
	}
	key := fields[1]
	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.getLiveLocked(key, now)
	if mf.casSet && ok && item.cas != mf.cas {
		_, _ = w.WriteString("EX\r\n")
		_ = w.Flush()
		return
	}
	if !ok {
		if !autoCreate {
			_, _ = w.WriteString("NF\r\n")
			_ = w.Flush()
			return
		}
		f.cas++
		item = &fakeItem{
			value:     []byte(strconv.FormatUint(initial, 10)),
			cas:       f.cas,
			expiresAt: fakeExpiryFromToken(autoTTL, now),
		}
		f.items[key] = item
	}
	if mf.ttlSet {
		item.expiresAt = fakeExpiryFromToken(mf.ttl, now)
	}
	cur, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
		_ = w.Flush()
		return
	}
	decr := mf.mode == 'D' || mf.mode == '-'
	switch {
	case decr && delta > cur:
		cur = 0
	case decr:
		cur -= delta
	default:
		cur += delta
	}
	item.value = []byte(strconv.FormatUint(cur, 10))
	f.cas++
	item.cas = f.cas
	if mf.quiet {
		return
	}
	suffix := ""
	if mf.opaque != "" {
		suffix = " O" + mf.opaque
	}
	if mf.value {
		_, _ = fmt.Fprintf(w, "VA %d%s\r\n%s\r\n", len(item.value), suffix, item.value)
		_ = w.Flush()
		return
	}
	_, _ = w.WriteString("HD" + suffix + "\r\n")
	_ = w.Flush()
}

func (f *fakeMemcached) metaDebug(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	key := fields[1]
	binaryKey := false
	for _, tok := range fields[2:] {
		if tok == "b" {
			binaryKey = true
		}
	}
	lookup := key
	if binaryKey {
		if dec, err := base64Decode([]byte(key)); err == nil {
			lookup = string(dec)
		}
	}
	now := time.Now()
	f.mu.Lock()
	item, ok := f.getLiveLocked(lookup, now)
	exp := int64(-1)
	var cas uint64
	var size int
	if ok {
		cas = item.cas
		size = len(item.value)
		exp = fakeRemainingTTLSecs(item, now)
	}
	f.mu.Unlock()
	if !ok {
		_, _ = w.WriteString("EN\r\n")
		_ = w.Flush()
		return
	}
	_, _ = fmt.Fprintf(w, "ME %s exp=%d la=0 cas=%d fetch=no cls=1 size=%d extra=yes\r\n", key, exp, cas, size)
	_ = w.Flush()
}
