package memcached

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"time"
)

type fakeMetaFlags struct {
	quiet     bool
	value     bool
	returnCAS bool
	returnTTL bool
	returnF   bool
	cas       uint64
	casSet    bool
	mode      byte
	opaque    string
	ttl       int64
	ttlSet    bool
	flags     uint32
	flagsSet  bool
	vivifyTTL int64
	vivify    bool
	recache   bool
	returnKey bool
	binaryKey bool
}

func parseFakeMetaFlags(tokens []string) fakeMetaFlags {
	var f fakeMetaFlags
	for _, tok := range tokens {
		switch {
		case tok == "q":
			f.quiet = true
		case tok == "v":
			f.value = true
		case tok == "c":
			f.returnCAS = true
		case tok == "t":
			f.returnTTL = true
		case tok == "f":
			f.returnF = true
		case tok == "k":
			f.returnKey = true
		case tok == "b":
			f.binaryKey = true
		case len(tok) > 1 && tok[0] == 'C':
			f.cas, _ = strconv.ParseUint(tok[1:], 10, 64)
			f.casSet = true
		case len(tok) > 1 && tok[0] == 'O':
			f.opaque = tok[1:]
		case len(tok) == 2 && tok[0] == 'M':
			f.mode = tok[1]
		case len(tok) > 1 && tok[0] == 'T':
			f.ttl, _ = strconv.ParseInt(tok[1:], 10, 64)
			f.ttlSet = true
		case len(tok) > 1 && tok[0] == 'F':
			u, err := strconv.ParseUint(tok[1:], 10, 32)
			if err == nil {
				f.flags = uint32(u) // #nosec G115 -- ParseUint bitSize 32
				f.flagsSet = true
			}
		case len(tok) > 1 && tok[0] == 'N':
			f.vivifyTTL, _ = strconv.ParseInt(tok[1:], 10, 64)
			f.vivify = true
		case len(tok) > 1 && tok[0] == 'R':
			f.recache = true
		}
	}
	return f
}

func (f *fakeMemcached) metaSet(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return true
	}
	key := fields[1]
	nbytes, err := strconv.Atoi(fields[2])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return true
	}
	mf := parseFakeMetaFlags(fields[3:])
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	existing, ok := f.getLiveLocked(key, now)
	if mf.casSet && ok && existing.cas != mf.cas {
		writeFakeMetaStatus(w, "EX", mf.quiet, mf.opaque)
		return true
	}
	switch mf.mode {
	case 'E':
		if ok {
			writeFakeMetaStatus(w, "NS", mf.quiet, mf.opaque)
			return true
		}
	case 'R':
		if !ok {
			writeFakeMetaStatus(w, "NS", mf.quiet, mf.opaque)
			return true
		}
	}
	var flags uint32
	var expiresAt time.Time
	if ok {
		flags = existing.flags
		expiresAt = existing.expiresAt // omit T → keep TTL on update (real meta)
	}
	if mf.flagsSet {
		flags = mf.flags
	}
	if mf.ttlSet {
		expiresAt = fakeExpiryFromToken(mf.ttl, now)
		if !expiresAt.IsZero() && !expiresAt.After(now) {
			delete(f.items, key)
			writeFakeMetaStatus(w, "HD", mf.quiet, mf.opaque)
			return true
		}
	}
	f.cas++
	f.items[key] = &fakeItem{value: value, flags: flags, cas: f.cas, expiresAt: expiresAt}
	writeFakeMetaStatus(w, "HD", mf.quiet, mf.opaque)
	return true
}

// writeFakeMetaStatus emits a meta status line. Quiet suppresses success unless
// opaque is set (pipeline matching requires O replies before mn).
func writeFakeMetaStatus(w *bufio.Writer, status string, quiet bool, opaque string) {
	if quiet && opaque == "" && (status == "HD" || status == "VA") {
		return
	}
	line := status + "\r\n"
	if opaque != "" {
		line = status + " O" + opaque + "\r\n"
	}
	_, _ = w.WriteString(line)
	_ = w.Flush()
}

func fakeMetaSuffix(mf fakeMetaFlags, key string, item *fakeItem, now time.Time, won, stale, busy bool) string {
	suffix := ""
	if mf.opaque != "" {
		suffix = " O" + mf.opaque
	}
	if mf.returnKey {
		suffix += " k" + key
	}
	if mf.binaryKey {
		suffix += " b"
	}
	if item != nil && mf.returnCAS {
		suffix += " c" + strconv.FormatUint(item.cas, 10)
	}
	if item != nil && mf.returnF {
		suffix += " f" + strconv.FormatUint(uint64(item.flags), 10)
	}
	if item != nil && mf.returnTTL {
		suffix += " t" + strconv.FormatInt(fakeRemainingTTLSecs(item, now), 10)
	}
	return suffix + recacheExtra(won, stale, busy)
}

func (f *fakeMemcached) metaGet(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	if fields[1] == "cerr" {
		_, _ = w.WriteString("CLIENT_ERROR bad meta\r\n")
		_ = w.Flush()
		return
	}
	mf := parseFakeMetaFlags(fields[2:])
	now := time.Now()
	f.mu.Lock()
	item, ok := f.getLiveLocked(fields[1], now)
	if !ok && mf.vivify {
		f.cas++
		item = &fakeItem{
			cas:       f.cas,
			expiresAt: fakeExpiryFromToken(mf.vivifyTTL, now),
		}
		f.items[fields[1]] = item
		item.recacheBusy = true
		suffix := fakeMetaSuffix(mf, fields[1], item, now, true, false, false)
		f.mu.Unlock()
		if !mf.quiet || mf.opaque != "" {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if !ok {
		f.mu.Unlock()
		suffix := fakeMetaSuffix(mf, fields[1], nil, now, false, false, false)
		if !mf.quiet || mf.opaque != "" {
			_, _ = w.WriteString("EN" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	won, stale, busy := false, item.stale, false
	if mf.recache {
		if item.recacheBusy {
			busy = true
		} else {
			won = true
			item.recacheBusy = true
		}
	}
	snap := &fakeItem{cas: item.cas, flags: item.flags, expiresAt: item.expiresAt, value: item.value}
	suffix := fakeMetaSuffix(mf, fields[1], snap, now, won, stale, busy)
	f.mu.Unlock()

	if mf.casSet && snap.cas == mf.cas {
		if !mf.quiet {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if mf.value {
		// VA always carries client flags (fN); strip returnF from suffix to avoid "fN fN".
		vaMF := mf
		vaMF.returnF = false
		vaSuffix := fakeMetaSuffix(vaMF, fields[1], snap, now, won, stale, busy)
		_, _ = fmt.Fprintf(w, "VA %d f%d%s\r\n", len(snap.value), snap.flags, vaSuffix)
		_, _ = w.Write(snap.value)
		_, _ = w.WriteString("\r\n")
		_ = w.Flush()
		return
	}
	if !mf.quiet {
		_, _ = w.WriteString("HD" + suffix + "\r\n")
		_ = w.Flush()
	}
}

func recacheExtra(won, stale, busy bool) string {
	s := ""
	if won {
		s += " W"
	}
	if stale {
		s += " X"
	}
	if busy {
		s += " Z"
	}
	return s
}
