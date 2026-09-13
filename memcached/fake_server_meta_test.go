package memcached

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

func parseFakeMetaFlags(tokens []string) (quiet, value, returnCAS bool, cas uint64, casSet bool, mode byte, opaque string) {
	for _, tok := range tokens {
		switch {
		case tok == "q":
			quiet = true
		case tok == "v":
			value = true
		case tok == "c":
			returnCAS = true
		case len(tok) > 1 && tok[0] == 'C':
			cas, _ = strconv.ParseUint(tok[1:], 10, 64)
			casSet = true
		case len(tok) > 1 && tok[0] == 'O':
			opaque = tok[1:]
		case len(tok) == 2 && tok[0] == 'M':
			mode = tok[1]
		}
	}
	return
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
	quiet, _, _, cas, casSet, mode, _ := parseFakeMetaFlags(fields[3:])
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	existing, ok := f.items[key]
	if casSet && ok && existing.cas != cas {
		f.mu.Unlock()
		_, _ = w.WriteString("EX\r\n")
		_ = w.Flush()
		return true
	}
	switch mode {
	case 'E':
		if ok {
			f.mu.Unlock()
			_, _ = w.WriteString("NS\r\n")
			_ = w.Flush()
			return true
		}
	case 'R':
		if !ok {
			f.mu.Unlock()
			_, _ = w.WriteString("NS\r\n")
			_ = w.Flush()
			return true
		}
	}
	f.cas++
	var flags uint32
	if ok {
		flags = existing.flags
	}
	f.items[key] = &fakeItem{value: value, flags: flags, cas: f.cas}
	f.mu.Unlock()
	if !quiet {
		_, _ = w.WriteString("HD\r\n")
		_ = w.Flush()
	}
	return true
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
	quiet, wantValue, returnCAS, cas, casSet, _, opaque := parseFakeMetaFlags(fields[2:])
	returnKey, binaryKey, vivify, recache := false, false, false, false
	for _, tok := range fields[2:] {
		switch {
		case tok == "k":
			returnKey = true
		case tok == "b":
			binaryKey = true
		case len(tok) > 0 && tok[0] == 'N':
			vivify = true
		case len(tok) > 0 && tok[0] == 'R':
			recache = true
		}
	}
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	if !ok && vivify {
		f.cas++
		item = &fakeItem{cas: f.cas}
		f.items[fields[1]] = item
		item.recacheBusy = true
		f.mu.Unlock()
		suffix := recacheSuffix(opaque, returnKey, binaryKey, fields[1], true, false, false)
		if !quiet {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	f.mu.Unlock()
	suffix := ""
	if opaque != "" {
		suffix = " O" + opaque
	}
	if returnKey {
		suffix += " k" + fields[1]
	}
	if binaryKey {
		suffix += " b"
	}
	if ok && returnCAS {
		suffix += " c" + strconv.FormatUint(item.cas, 10)
	}
	if !ok {
		if !quiet {
			_, _ = w.WriteString("EN" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	won, stale, busy := false, item.stale, false
	if recache {
		f.mu.Lock()
		if item.recacheBusy {
			busy = true
		} else {
			won = true
			item.recacheBusy = true
		}
		f.mu.Unlock()
	}
	suffix += recacheExtra(won, stale, busy)
	if casSet && item.cas == cas {
		if !quiet {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if wantValue {
		_, _ = fmt.Fprintf(w, "VA %d f%d%s\r\n", len(item.value), item.flags, suffix)
		_, _ = w.Write(item.value)
		_, _ = w.WriteString("\r\n")
		_ = w.Flush()
		return
	}
	if !quiet {
		_, _ = w.WriteString("HD" + suffix + "\r\n")
		_ = w.Flush()
	}
}

func recacheSuffix(opaque string, returnKey, binaryKey bool, key string, won, stale, busy bool) string {
	s := ""
	if opaque != "" {
		s = " O" + opaque
	}
	if returnKey {
		s += " k" + key
	}
	if binaryKey {
		s += " b"
	}
	return s + recacheExtra(won, stale, busy)
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

func (f *fakeMemcached) metaDelete(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	quiet, _, _, _, _, _, _ := parseFakeMetaFlags(fields[2:])
	invalidate := false
	for _, tok := range fields[2:] {
		if tok == "I" {
			invalidate = true
		}
	}
	key := fields[1]
	f.mu.Lock()
	item, ok := f.items[key]
	if !ok {
		f.mu.Unlock()
		_, _ = w.WriteString("NF\r\n")
		_ = w.Flush()
		return
	}
	if invalidate {
		item.stale = true
		f.cas++
		item.cas = f.cas
	} else {
		delete(f.items, key)
	}
	f.mu.Unlock()
	if !quiet {
		_, _ = w.WriteString("HD\r\n")
		_ = w.Flush()
	}
}

func (f *fakeMemcached) metaArith(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	quiet, wantValue, _, cas, casSet, mode, _ := parseFakeMetaFlags(fields[2:])
	delta := uint64(1)
	initial := uint64(0)
	autoCreate := false
	for _, tok := range fields[2:] {
		switch {
		case len(tok) > 1 && tok[0] == 'D':
			delta, _ = strconv.ParseUint(tok[1:], 10, 64)
		case len(tok) > 1 && tok[0] == 'J':
			initial, _ = strconv.ParseUint(tok[1:], 10, 64)
		case len(tok) > 0 && tok[0] == 'N':
			autoCreate = true
		}
	}
	key := fields[1]
	f.mu.Lock()
	item, ok := f.items[key]
	if casSet && ok && item.cas != cas {
		f.mu.Unlock()
		_, _ = w.WriteString("EX\r\n")
		_ = w.Flush()
		return
	}
	if !ok {
		if !autoCreate {
			f.mu.Unlock()
			_, _ = w.WriteString("NF\r\n")
			_ = w.Flush()
			return
		}
		f.cas++
		item = &fakeItem{value: []byte(strconv.FormatUint(initial, 10)), cas: f.cas}
		f.items[key] = item
	}
	cur, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		f.mu.Unlock()
		_, _ = w.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
		_ = w.Flush()
		return
	}
	decr := mode == 'D' || mode == '-'
	if decr {
		if delta > cur {
			cur = 0
		} else {
			cur -= delta
		}
	} else {
		cur += delta
	}
	item.value = []byte(strconv.FormatUint(cur, 10))
	f.cas++
	item.cas = f.cas
	f.mu.Unlock()
	if quiet {
		return
	}
	if wantValue {
		_, _ = fmt.Fprintf(w, "VA %d\r\n%s\r\n", len(item.value), item.value)
	} else {
		_, _ = w.WriteString("HD\r\n")
	}
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
	f.mu.Lock()
	item, ok := f.items[lookup]
	f.mu.Unlock()
	if !ok {
		_, _ = w.WriteString("EN\r\n")
		_ = w.Flush()
		return
	}
	_, _ = fmt.Fprintf(w, "ME %s exp=-1 la=0 cas=%d fetch=no cls=1 size=%d extra=yes\r\n", key, item.cas, len(item.value))
	_ = w.Flush()
}
